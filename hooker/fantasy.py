import requests
import json

BASE_URL = "https://fantasy.nrl.com/data"

s = requests.Session()

def pull_players():
    r = s.get(f"{BASE_URL}/nrl/players.json")
    r.raise_for_status()
    return r.json()

def pull_player_stats(external_player_id: int):
    r = s.get(f"{BASE_URL}/nrl/stats/players/{external_player_id}.json")
    r.raise_for_status()
    return r.json()

def get_all_player_data():
    players = pull_players()

    return [{**player, "match_stats": pull_player_stats(player["id"])} for player in players]

def get_all_teams():
    r = s.get(f"{BASE_URL}/nrl/squads.json")
    r.raise_for_status()
    return r.json()

def get_venues():
    r = s.get(f"{BASE_URL}/nrl/venues.json")
    r.raise_for_status()
    return r.json()

def get_rounds():
    r = s.get(f"{BASE_URL}/nrl/rounds.json")
    r.raise_for_status()
    return r.json()

def load_local_data(path: str) -> dict:
    with open(path, 'r') as fp:
        return json.load(fp)