import asyncio
import json
from datetime import datetime, timezone


class Ship:
    def __init__(self, symbol, ships_db, call_api, log, on_event=None):
        self.symbol = symbol
        self.ships_db = ships_db
        self.call_api = call_api
        self.log = log
        self.on_event = on_event
        self.data = self._load()

    def _load(self):
        row = self.ships_db.execute("SELECT data, role FROM ships WHERE symbol = ?", (self.symbol,)).fetchone()
        if row:
            self.role = row[1]
            return json.loads(row[0])
        self.role = None
        return {}

    def save(self, preserve_role=False):
        if not self.data:
            return
        symbol = self.data["symbol"]
        waypoint = self.data["nav"]["waypointSymbol"]
        system = self.data["nav"]["systemSymbol"]
        if preserve_role:
            self.ships_db.execute(
                "INSERT INTO ships (symbol, waypoint, system, data) VALUES (?, ?, ?, ?)"
                " ON CONFLICT(symbol) DO UPDATE SET waypoint = excluded.waypoint, system = excluded.system, data = excluded.data",
                (symbol, waypoint, system, json.dumps(self.data))
            )
        else:
            self.ships_db.execute(
                "INSERT INTO ships (symbol, waypoint, system, data, role) VALUES (?, ?, ?, ?, ?)"
                " ON CONFLICT(symbol) DO UPDATE SET waypoint = excluded.waypoint, system = excluded.system, data = excluded.data, role = excluded.role",
                (symbol, waypoint, system, json.dumps(self.data), self.role)
            )
        self.ships_db.commit()

    @classmethod
    def load_all(cls, ships_db, call_api, log, on_event=None):
        rows = ships_db.execute("SELECT symbol FROM ships").fetchall()
        return [cls(row[0], ships_db, call_api, log, on_event=on_event) for row in rows]

    def _emit(self, event, data=None):
        if self.on_event:
            self.on_event(self.symbol, event, data)

    async def orbit(self):
        response = await self.call_api(f"/my/ships/{self.symbol}/orbit", method="POST")
        if "data" in response:
            self.data["nav"] = response["data"]["nav"]
            self.save()
        return response

    async def dock(self):
        response = await self.call_api(f"/my/ships/{self.symbol}/dock", method="POST")
        if "data" in response:
            self.data["nav"] = response["data"]["nav"]
            self.save()
        return response

    async def refuel(self):
        return await self.call_api(f"/my/ships/{self.symbol}/refuel", method="POST")

    async def navigate(self, waypoint_symbol):
        response = await self.call_api(f"/my/ships/{self.symbol}/navigate", method="POST", data={"waypointSymbol": waypoint_symbol})
        if "error" in response and response["error"].get("code") == 4203:
            self.log(f"[{self.symbol}] Insufficient fuel, refueling at current location...")
            await self.dock()
            refuel_response = await self.refuel()
            if "data" in refuel_response:
                await self.orbit()
                response = await self.call_api(f"/my/ships/{self.symbol}/navigate", method="POST", data={"waypointSymbol": waypoint_symbol})
            else:
                self._emit("error", {"message": f"Cannot refuel at {self.data['nav']['waypointSymbol']}, stuck"})
                return response
        if "data" in response:
            self.data["nav"] = response["data"]["nav"]
            self.save()
            arrival = response["data"]["nav"]["route"]["arrival"]
            seconds = int((datetime.fromisoformat(arrival) - datetime.now(timezone.utc)).total_seconds())
            print(f"[{self.symbol}] Arriving at {waypoint_symbol} in {seconds} seconds")
        return response

    async def wait_for_arrival(self):
        nav = self.data.get("nav", {})
        arrival = nav.get("route", {}).get("arrival")
        if arrival:
            remaining = (datetime.fromisoformat(arrival) - datetime.now(timezone.utc)).total_seconds()
            if remaining > 0:
                await asyncio.sleep(remaining)
        destination = nav.get("route", {}).get("destination", {}).get("symbol")
        self._emit("arrived", destination)

    async def extract(self):
        response = await self.call_api(f"/my/ships/{self.symbol}/extract", method="POST")
        if "error" in response:
            self._emit("error", response["error"])
        else:
            cargo = response["data"]["cargo"]
            self.data["cargo"] = cargo
            self.save()
            if cargo["units"] >= cargo["capacity"]:
                self._emit("cargo_full", cargo)
        return response

    async def get_cargo(self):
        response = await self.call_api(f"/my/ships/{self.symbol}/cargo")
        if "error" in response:
            self._emit("error", response["error"])
            return []
        return response["data"]["inventory"]

    async def sell(self, cargo_symbol, units):
        response = await self.call_api(f"/my/ships/{self.symbol}/sell", method="POST", data={"symbol": cargo_symbol, "units": units})
        if "error" in response:
            self._emit("error", response["error"])
        return response

    async def jettison(self, cargo_symbol, units):
        response = await self.call_api(f"/my/ships/{self.symbol}/jettison", method="POST", data={"symbol": cargo_symbol, "units": units})
        if "error" in response:
            self._emit("error", response["error"])
        return response
