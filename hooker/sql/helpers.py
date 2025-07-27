from hooker import conn

def get_external_to_internal_team_ids() -> dict[int, int]:
    with conn.cursor() as cur:
        cur.execute("SELECT id, external_fantasy_id FROM teams")
        data = cur.fetchall()
        return {v[1]: v[0] for v in data}

def get_internal_team_id(external_id: int) -> int:
    id = get_external_to_internal_team_ids().get(external_id)
    if id is None:
        raise ValueError(f"No internal team ID found for external ID {external_id}.")
    return id

def get_external_to_internal_position_ids() -> dict[int, int]:
    with conn.cursor() as cur:
        cur.execute("SELECT id, external_fantasy_id FROM fantasy_positions")
        data = cur.fetchall()
        return {v[1]: v[0] for v in data}

def get_external_to_internal_venue_ids() -> dict[int, int]:
    with conn.cursor() as cur:
        cur.execute("SELECT id, external_fantasy_id FROM venues")
        data = cur.fetchall()
        return {v[1]: v[0] for v in data}

def get_internal_venue_id(external_venue_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM venues WHERE external_fantasy_id = %s", (external_venue_id,))
        result = cur.fetchone()
        if result:
            return result[0]
        else:
            raise ValueError(f"No venue found for external ID {external_venue_id}.")

def get_internal_round_id(round_number: int, year: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM rounds WHERE round = %s AND year = %s",
            (round_number, year)
        )
        result = cur.fetchone()
        if result:
            return result[0]
        else:
            raise ValueError(f"No round found for round {round_number} in year {year}.")

def get_external_to_internal_match_ids() -> dict[int, int]:
    with conn.cursor() as cur:
        cur.execute("SELECT id, external_fantasy_id FROM matches")
        data = cur.fetchall()
        return {v[1]: v[0] for v in data}

def get_internal_match_id(external_match_id: int) -> int:
    match_id = get_external_to_internal_match_ids().get(external_match_id)
    if match_id is None:
        raise ValueError(f"No internal match ID found for external ID {external_match_id}.")
    return match_id

def get_external_to_internal_player_ids() -> dict[int, int]:
    with conn.cursor() as cur:
        cur.execute("SELECT id, external_fantasy_id FROM players")
        data = cur.fetchall()
        return {v[1]: v[0] for v in data}

def get_internal_player_id(external_player_id: int) -> int:
    player_id = get_external_to_internal_player_ids().get(external_player_id)
    if player_id is None:
        raise ValueError(f"No internal player ID found for external ID {external_player_id}.")
    return player_id