import requests
import json


BASE_URL = "https://api.spacetraders.io/v2"

with open("agent.token") as f:
    AGENT_TOKEN = f.read().strip()

session = requests.Session()
session.headers["Authorization"] = f"Bearer {AGENT_TOKEN}"

def call_api(endpoint, method="GET", data=None):
    url = f"{BASE_URL}{endpoint}"
    response = session.request(method, url, json=data)
    return response.json()

def ping_system(system):
    try:
        with open("known_locations.json") as f:
            known = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        known = {}
    response = call_api(f"/systems/{system}/waypoints")
    for waypoint in response["data"]:
        symbol = waypoint["symbol"]
        if symbol in known:
            known[symbol].update(waypoint)
        else:
            known[symbol] = waypoint

        for orbital in waypoint.get("orbitals", []):
            orbital_symbol = orbital["symbol"]
            orbital_data = call_api(f"/systems/{system}/waypoints/{orbital_symbol}")["data"]
            if orbital_symbol in known:
                known[orbital_symbol].update(orbital_data)
            else:
                known[orbital_symbol] = orbital_data
    with open("known_locations.json", "w") as f:
        json.dump(known, f, indent=2)

    return 0

def get_contracts():
    contracts = []
    for contract in call_api("/my/contracts")["data"]:
        contracts.append(contract)
    return contracts

def accept_contract(contract_id):
    return call_api(f"/my/contracts/{contract_id}/accept", method="POST")

def system_from_waypoint(waypoint):
    return "-".join(waypoint.split("-")[:2])

def lookup_waypoints_with_feature(system, feature_trait):
    waypoints = []
    with open ("known_locations.json") as f:
        known = json.load(f)
    for key, waypoint in known.items():
        if not key.startswith(f"{system}-"):
            continue
        if any(t.get("symbol") == feature_trait for t in waypoint.get("traits", [])):
            waypoints.append(waypoint)
    return waypoints

def find_waypoints_with_feature(system, feature_trait):
    waypoints = []
    for waypoint in call_api(f"/systems/{system}/waypoints?traits={feature_trait}")["data"]:
        waypoints.append(waypoint)
    return waypoints

def get_available_ships_at_shipyard(waypoint):
    system = system_from_waypoint(waypoint)
    return call_api(f"/systems/{system}/waypoints/{waypoint}/shipyard")

print(json.dumps(lookup_waypoints_with_feature("X1-P4", "SHIPYARD"), indent=2))
# ping_system("X1-P4")