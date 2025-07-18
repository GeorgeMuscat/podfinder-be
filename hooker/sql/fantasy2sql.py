import psycopg
from ..fantasy import get_all_teams
from hooker import conn

def insert_teams():
    try:
        with conn.cursor() as cur:
            teams = get_all_teams()
            for team in teams:
                cur.execute(
                    """
                    INSERT INTO teams (external_fantasy_id, full_name, short_name, abbreviation)
                    VALUES (%s, %s, %s, %s)

                    """,
                    (team["id"], team["full_name"], team["name"], team["short_name"]),
                )
        conn.commit()
    except (Exception, psycopg.DatabaseError) as error:
        print(error)


if __name__ == "__main__":
    insert_teams()
    print("Teams inserted successfully.")