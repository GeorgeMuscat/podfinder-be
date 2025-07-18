from ..fantasy import get_all_teams
from hooker import conn

def insert_teams():
    with conn.cursor() as cur:
        teams = get_all_teams()
        for team in teams:
            cur.execute(
                """
                INSERT INTO fantasy_team (external_fantasy_id, full_name, short_name, abbreviation)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (external_fantasy_id) DO UPDATE SET
                    full_name = EXCLUDED.full_name,
                    short_name = EXCLUDED.short_name,
                    abbreviation = EXCLUDED.abbreviation
                """,
                (team["id"], team["full_name"], team["name"], team["short_name"]),
            )