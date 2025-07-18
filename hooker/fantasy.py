import requests
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


def upsert_player_info(external_player_info: dict):
    # First check if player with the external ID already exists

    # If it does, update the existing record

    # If it doesn't, insert a new record

    # nvm can just do INSERT OR REPLACE INTO
    pass


def upsert_player_stats(player_stats: dict):
    pass