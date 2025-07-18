import psycopg
from ..fantasy import get_all_teams
from hooker import conn

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



if __name__ == "__main__":
    insert_teams()
