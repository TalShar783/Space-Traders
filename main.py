import asyncio
import aiohttp
import json
import sqlite3
import traceback
from collections import deque
from datetime import datetime, timezone
from ship import Ship

#API documentation is available at https://spacetraders.io/openapi#tag/fleet/POST/my/ships

BASE_URL = "https://api.spacetraders.io/v2"
DEBUG = False
LOG_FILE = "game.log"

with open("agent.token") as f:
    AGENT_TOKEN = f.read().strip()

db = sqlite3.connect("known_locations.db")
db.execute("""
    CREATE TABLE IF NOT EXISTS waypoints (
        symbol TEXT PRIMARY KEY,
        system TEXT,
        data   TEXT
    )
""")
db.commit()

ships_db = sqlite3.connect("ships.db")
ships_db.execute("""
    CREATE TABLE IF NOT EXISTS ships (
        symbol   TEXT PRIMARY KEY,
        waypoint TEXT,
        system   TEXT,
        data     TEXT,
        role     TEXT
    )
""")
try:
    ships_db.execute("ALTER TABLE ships ADD COLUMN role TEXT")
except sqlite3.OperationalError:
    pass  # column already exists
ships_db.commit()

# aiohttp session — set inside main() once the event loop is running
session: aiohttp.ClientSession | None = None


# --- Logging ---

def log(data, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted = json.dumps(data, indent=2) if isinstance(data, (dict, list)) else str(data)
    output = f"[{timestamp}] [{level}] {formatted}"
    print(output)
    with open(LOG_FILE, "a") as f:
        f.write(output + "\n")

def jprint(data):
    log(data)


# --- API ---

class RateLimiter:
    def __init__(self, calls_per_second: int):
        self._limit = calls_per_second
        self._slots: deque = deque()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = asyncio.get_event_loop().time()
            while self._slots and now - self._slots[0] >= 1.0:
                self._slots.popleft()
            if len(self._slots) >= self._limit:
                wait = 1.0 - (now - self._slots[0])
                if wait > 0:
                    await asyncio.sleep(wait)
            self._slots.append(asyncio.get_event_loop().time())

rate_limiter = RateLimiter(calls_per_second=2)

async def call_api(endpoint, method="GET", data=None):
    assert session is not None, "call_api called before session was initialized"
    await rate_limiter.acquire()
    url = f"{BASE_URL}{endpoint}"
    if method == "GET":
        async with session.request(method, url) as response:
            result = await response.json(content_type=None)
    else:
        async with session.request(method, url, json=data if data is not None else {}) as response:
            result = await response.json(content_type=None)
    if "error" in result:
        log(result, level="ERROR")
    elif DEBUG:
        traceback.print_stack()
        log(result)
    return result


# --- Waypoint utilities ---

def upsert_waypoint(waypoint):
    symbol = waypoint["symbol"]
    system = "-".join(symbol.split("-")[:2])
    db.execute(
        "INSERT INTO waypoints (symbol, system, data) VALUES (?, ?, ?)"
        " ON CONFLICT(symbol) DO UPDATE SET data = excluded.data",
        (symbol, system, json.dumps(waypoint))
    )

async def ping_system(system):
    response = await call_api(f"/systems/{system}/waypoints")
    for waypoint in response["data"]:
        upsert_waypoint(waypoint)
        for orbital in waypoint.get("orbitals", []):
            orbital_data = (await call_api(f"/systems/{system}/waypoints/{orbital['symbol']}"))["data"]
            upsert_waypoint(orbital_data)
    db.commit()

def system_from_waypoint(waypoint):
    return "-".join(waypoint.split("-")[:2])

def lookup_waypoints_with_feature(system, feature_trait):
    rows = db.execute("SELECT data FROM waypoints WHERE system = ?", (system,)).fetchall()
    results = []
    for (data,) in rows:
        waypoint = json.loads(data)
        if any(t.get("symbol") == feature_trait for t in waypoint.get("traits", [])):
            results.append(waypoint)
    return results

def lookup_waypoints_by_type(system, waypoint_type):
    rows = db.execute("SELECT data FROM waypoints WHERE system = ?", (system,)).fetchall()
    results = []
    for (data,) in rows:
        waypoint = json.loads(data)
        if waypoint.get("type") == waypoint_type:
            results.append(waypoint)
    return results

async def find_waypoints_with_feature(system, feature_trait):
    return (await call_api(f"/systems/{system}/waypoints?traits={feature_trait}"))["data"]

async def find_waypoints_by_type(system, waypoint_type):
    return (await call_api(f"/systems/{system}/waypoints?type={waypoint_type}"))["data"]

async def get_available_ships_at_shipyard(waypoint):
    system = system_from_waypoint(waypoint)
    return await call_api(f"/systems/{system}/waypoints/{waypoint}/shipyard")

async def get_market_data(waypoint):
    system = system_from_waypoint(waypoint)
    return await call_api(f"/systems/{system}/waypoints/{waypoint}/market")

async def find_place_to_sell(cargo_symbol, system):
    for waypoint in lookup_waypoints_with_feature(system, "MARKETPLACE"):
        market_data = await get_market_data(waypoint["symbol"])
        market = market_data.get("data", {})
        accepted = market.get("imports", []) + market.get("exchange", [])
        if any(item["symbol"] == cargo_symbol for item in accepted):
            return waypoint["symbol"]
    return None

async def get_contracts():
    return (await call_api("/my/contracts"))["data"]

async def accept_contract(contract_id):
    return await call_api(f"/my/contracts/{contract_id}/accept", method="POST")

async def purchase_ship(ship_type, waypoint):
    response = await call_api("/my/ships", method="POST", data={"shipType": ship_type, "waypointSymbol": waypoint})
    if "data" in response:
        ship = Ship(response["data"]["ship"]["symbol"], ships_db, call_api, log)
        ship.data = response["data"]["ship"]
        ship.save()
        return ship
    return None


# --- Controller ---

def on_event(ship_symbol, event, data):
    log({"ship": ship_symbol, "event": event, "data": data})

async def update_ships():
    response = await call_api("/my/ships")
    for ship_data in response["data"]:
        ship = Ship(ship_data["symbol"], ships_db, call_api, log)
        ship.data = ship_data
        ship.save(preserve_role=True)


# --- Selling ---

async def sell_all_cargo(ship, return_waypoint):
    cargo = await ship.get_cargo()
    if not cargo:
        return

    system = system_from_waypoint(ship.data["nav"]["waypointSymbol"])

    # Build initial sell plan grouping items by marketplace
    sell_plan = {}
    for item in cargo:
        marketplace = await find_place_to_sell(item["symbol"], system)
        if marketplace:
            sell_plan.setdefault(marketplace, []).append(item)
        else:
            log(f"[{ship.symbol}] No marketplace found for {item['symbol']}, skipping")

    visited = set()
    while sell_plan:
        marketplace, items = next(iter(sell_plan.items()))
        del sell_plan[marketplace]
        visited.add(marketplace)

        log(f"[{ship.symbol}] Navigating to {marketplace} to sell")
        await ship.navigate(marketplace)
        await ship.wait_for_arrival()
        await ship.dock()

        for item in items:
            result = await ship.sell(item["symbol"], item["units"])
            if "data" in result:
                log(f"[{ship.symbol}] Sold {item['units']}x {item['symbol']} at {marketplace}")
            else:
                # Market didn't buy it — find a different one
                new_marketplace = await find_place_to_sell(item["symbol"], system)
                if new_marketplace and new_marketplace not in visited:
                    sell_plan.setdefault(new_marketplace, []).append(item)
                    log(f"[{ship.symbol}] {item['symbol']} not bought here, will try {new_marketplace}")
                else:
                    log(f"[{ship.symbol}] No other market found for {item['symbol']}, leaving in cargo")

        await ship.orbit()

    # Return to mining location if we left it
    if ship.data["nav"]["waypointSymbol"] != return_waypoint:
        log(f"[{ship.symbol}] Returning to {return_waypoint}")
        await ship.navigate(return_waypoint)
        await ship.wait_for_arrival()
        await ship.orbit()


# --- Mining utilities ---

VALID_MINING_TYPES = {"ASTEROID", "ASTEROID_FIELD", "ENGINEERED_ASTEROID"}

def find_best_mining_waypoint(system, current_waypoint_symbol, target_deposit=None):
    """Return the nearest valid mining waypoint, optionally filtered by deposit type."""
    ship_row = db.execute("SELECT data FROM waypoints WHERE symbol = ?", (current_waypoint_symbol,)).fetchone()
    ship_x, ship_y = 0, 0
    if ship_row:
        ship_data = json.loads(ship_row[0])
        ship_x, ship_y = ship_data.get("x", 0), ship_data.get("y", 0)

    rows = db.execute("SELECT data FROM waypoints WHERE system = ?", (system,)).fetchall()
    candidates = []
    for (data,) in rows:
        waypoint = json.loads(data)
        if waypoint.get("type") not in VALID_MINING_TYPES:
            continue
        if target_deposit:
            deposits = [d.get("symbol") for d in waypoint.get("deposits", [])]
            if target_deposit not in deposits:
                continue
        dx = waypoint.get("x", 0) - ship_x
        dy = waypoint.get("y", 0) - ship_y
        candidates.append(((dx**2 + dy**2) ** 0.5, waypoint))

    if not candidates and target_deposit:
        # No deposit data matched — fall back to nearest valid type
        return find_best_mining_waypoint(system, current_waypoint_symbol)

    return min(candidates, key=lambda c: c[0])[1] if candidates else None


# --- Loops ---

async def copper_loop():
    ships = [s for s in Ship.load_all(ships_db, call_api, log, on_event=on_event)
             if s.symbol in ("TALSHAR783-1", "TALSHAR783-3")]

    async def mine(ship):
        # Resume from mid-transit if restarted while navigating
        if ship.data.get("nav", {}).get("status") == "IN_TRANSIT":
            log(f"[{ship.symbol}] Resuming transit, waiting for arrival...")
            await ship.wait_for_arrival()
            await ship.orbit()

        # Sleep through any active cooldown remaining from before restart
        cooldown_expiry = ship.data.get("cooldown", {}).get("expiration")
        if cooldown_expiry:
            remaining = (datetime.fromisoformat(cooldown_expiry) - datetime.now(timezone.utc)).total_seconds()
            if remaining > 0:
                log(f"[{ship.symbol}] Resuming cooldown, {int(remaining)}s remaining...")
                await asyncio.sleep(remaining)

        system = system_from_waypoint(ship.data["nav"]["waypointSymbol"])
        best = find_best_mining_waypoint(system, ship.data["nav"]["waypointSymbol"], target_deposit="COPPER_ORE")
        if best is None:
            log(f"[{ship.symbol}] No valid mining waypoint found in {system}, aborting")
            return
        mining_waypoint = best["symbol"]
        if mining_waypoint != ship.data["nav"]["waypointSymbol"]:
            log(f"[{ship.symbol}] Navigating to mining target {mining_waypoint}")
            await ship.navigate(mining_waypoint)
            await ship.wait_for_arrival()
            await ship.orbit()

        while True:
            response = await ship.extract()
            if "error" in response:
                cooldown = response.get("error", {}).get("data", {}).get("cooldown", {}).get("remainingSeconds", 10)
            else:
                cargo_summary = response["data"]["cargo"]
                cooldown = response["data"]["cooldown"]["remainingSeconds"]
                inventory = cargo_summary.get("inventory", [])
                items_str = ", ".join(f"{item['symbol']} x{item['units']}" for item in inventory)
                log(f"[{ship.symbol}] Cargo: {cargo_summary['units']}/{cargo_summary['capacity']} | {items_str}")
                if cargo_summary["units"] >= cargo_summary["capacity"]:
                    log(f"[{ship.symbol}] Cargo full, heading to sell")
                    await sell_all_cargo(ship, mining_waypoint)
            await asyncio.sleep(cooldown)

    await asyncio.gather(*[mine(s) for s in ships])


# --- Main ---

async def main():
    global session
    async with aiohttp.ClientSession(headers={
        "Authorization": f"Bearer {AGENT_TOKEN}",
        "Content-Type": "application/json"
    }) as sess:
        session = sess
        await update_ships()
        await copper_loop()

try:
    asyncio.run(main())
except KeyboardInterrupt:
    print("\nStopped.")
