import psycopg

from hooker.sql.helpers import get_external_to_internal_position_ids, get_external_to_internal_team_ids, get_external_to_internal_venue_ids, get_internal_round_id, get_internal_team_id, get_internal_venue_id
from ..fantasy import get_all_teams, get_rounds, get_venues, pull_players, load_local_data
from hooker import conn
from datetime import datetime

def insert_teams():
    with conn.cursor() as cur:
        teams = get_all_teams()
        for team in teams:
            cur.execute(
                """
                INSERT INTO teams (external_fantasy_id, full_name, short_name, abbreviation)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (external_fantasy_id) DO NOTHING;
                """,
                (team["id"], team["full_name"], team["name"], team["short_name"])
            )
    conn.commit()

def insert_players():
    with conn.cursor() as cur:
        players = pull_players()
        for player in players:
            cur.execute(
                """
                INSERT INTO players (external_fantasy_id, first_name, last_name, team_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (external_fantasy_id) DO UPDATE SET
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    team_id = EXCLUDED.team_id
                """,
                (player["id"], player["first_name"], player["last_name"], player["squad_id"])
            )
    conn.commit()

def insert_fantasy_positions():
    with conn.cursor() as cur:
        # Assuming fantasy positions are static and can be hardcoded.
        # Currently there is no endpoint to fetch these dynamically.
        positions = [
            (1,"HOK", "Hooker"),
            (2, "MID", "Middle Forward"),
            (3, "EDG", "Edge Forward"),
            (4, "HLF", "Halfback"),
            (5, "CTR", "Centre"),
            (6, "WFB", "Wing Fullback"),
        ]
        for pos_id, pos_code, pos_name in positions:
            cur.execute(
                """
                INSERT INTO fantasy_positions (external_fantasy_id, abbreviation, name)
                VALUES (%s, %s, %s)
                ON CONFLICT (abbreviation) DO NOTHING;
                """,
                (pos_id, pos_code, pos_name)
            )
    conn.commit()

def insert_positions():
    with conn.cursor() as cur:
        positions = [
            ("Hooker", "HOK"),
            ("Prop", "MID"),
            ("Second Row", "EDG"),
            ("Lock", "MID"),
            ("Halfback", "HLF"),
            ("Five-Eighth", "HLF"),
            ("Centre", "CTR"),
            ("Wing", "WFB"),
            ("Fullback", "WFB")
        ]
        for pos_name, fantasy_pos_abbr in positions:
            # Look up the fantasy position to link it
            cur.execute(
                """
                SELECT id FROM fantasy_positions WHERE abbreviation = %s;
                """,
                (fantasy_pos_abbr,)
            )
            fantasy_pos = cur.fetchone()
            if fantasy_pos:
                fantasy_pos_id = fantasy_pos[0]
                cur.execute(
                    """
                    INSERT INTO positions (name, fantasy_position_id)
                    VALUES (%s, %s)
                    ON CONFLICT (name) DO NOTHING;
                    """,
                    (pos_name, fantasy_pos_id)
                )
            else:
                raise ValueError(f"Fantasy position {fantasy_pos_abbr} not found in fantasy_positions table.")
    conn.commit()

def upsert_players():
    '''
    This function will upsert all players.
    Only use this when you know you need to update or create all player data.
    '''

    # TODO: Make this fetch dynamically.
    players = load_local_data("../data/fantasy/players.json")

    # Get the internal to external team id mapping.

    team_mapping = get_external_to_internal_team_ids()
    position_mapping = get_external_to_internal_position_ids()

    with conn.cursor() as cur:
        for player in players:
            cur.execute(
                    '''
                    INSERT INTO players (external_fantasy_id, first_name, last_name, team_id, primary_fantasy_position_id, secondary_fantasy_position_id)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (external_fantasy_id) DO UPDATE SET
                        first_name = EXCLUDED.first_name,
                        last_name = EXCLUDED.last_name,
                        team_id = EXCLUDED.team_id,
                        primary_fantasy_position_id = EXCLUDED.primary_fantasy_position_id,
                        secondary_fantasy_position_id = EXCLUDED.secondary_fantasy_position_id;
                    ''', (player["id"], player["first_name"], player["last_name"], team_mapping[player["squad_id"]], position_mapping[player["positions"][0]], position_mapping[player["positions"][1]] if len(player["positions"]) > 1 else None))
    conn.commit()

def upsert_rounds():
    # TODO: Make this fetch dynamically.
    rounds = load_local_data("../data/fantasy/rounds.json")

    with conn.cursor() as cur:
        for round in rounds:
            cur.execute(
                '''INSERT INTO rounds ("round", "year", "start", "end")
                VALUES (%s, %s, %s, %s)
                ON CONFLICT ("round", "year") DO UPDATE SET
                    "start" = EXCLUDED."start",
                    "end" = EXCLUDED."end";
                ''',
                (round["id"], datetime.fromisoformat(round["start"]).year, datetime.fromisoformat(round["start"]).date(), datetime.fromisoformat(round["end"]).date())
            )
    conn.commit()

def upsert_venues():
    venues = get_venues()
    with conn.cursor() as cur:
        for venue in venues:
            # Weird case where the venue name is "TBA".
            # We don't want that...
            if venue["name"] == "TBA":
                continue
            cur.execute(
                '''
                INSERT INTO venues (name, external_fantasy_id)
                VALUES (%s, %s)
                ON CONFLICT (external_fantasy_id) DO UPDATE SET
                    name = EXCLUDED.name;
                ''',
                (venue["name"], venue["id"])
            )
    conn.commit()

def upsert_matches():
    rounds = get_rounds()

    with conn.cursor() as cur:
        for round in rounds:
            for match in round["matches"]:
                # Look up internal venue id and internal round id
                venue_id = get_internal_venue_id(match["venue_id"])
                round_id = get_internal_round_id(round["id"], datetime.fromisoformat(round["start"]).year)
                # Look up internal team ids
                home_team_id = get_internal_team_id(match["home_squad_id"])
                away_team_id = get_internal_team_id(match["away_squad_id"])

                cur.execute(
                    '''
                    INSERT INTO matches (external_fantasy_id, round_id, home_team_id, away_team_id, venue_id, date)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (external_fantasy_id) DO UPDATE SET
                        round_id = EXCLUDED.round_id,
                        home_team_id = EXCLUDED.home_team_id,
                        away_team_id = EXCLUDED.away_team_id,
                        venue_id = EXCLUDED.venue_id,
                        date = EXCLUDED.date;
                    ''', (match["id"], round_id, home_team_id, away_team_id, venue_id, match["date"])
                )
    conn.commit()


if __name__ == "__main__":
    # insert_teams()
    # insert_fantasy_positions()
    # insert_positions()
    # upsert_players()
    # upsert_rounds()
    # upsert_venues()
    upsert_matches()