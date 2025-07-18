import json
import functools
from pprint import pprint
import duckdb as ddb

con = ddb.connect(":memory:")

def read_break_evens(round: int) -> list[dict]:
    '''
    Returns a list of objects contining the break even points for each player in the given round.
    '''
    with open(f"./data/break-evens/{round}.json") as fp:
        return json.load(fp)["break_evens"]

def get_current_players(round: int) -> list[int]:
    '''
    Returns a list of player ids for the given round.
    '''
    with open(f"./data/my-team/{round}.json") as fp:
        obj = json.load(fp)
        list_of_lists =  [v for v in obj["result"]["lineup"].values() if type(v) is list]
        return functools.reduce(lambda x, y: x + y, list_of_lists, [])

def get_breakeven_and_last_3_avg(break_evens: list):
    '''
    Returns a list of tuples containing the breakeven and last 3 average points for each player.
    Each tuple contains the player id, breakeven points, and last 3 average points.
    '''
    return [(be["id"], be["be"], be["last_3_avg"]) for be in break_evens]


def main():
    round_number = 19
    break_evens: list = read_break_evens(round_number)
    current_players = list(map(lambda x: str(x), get_current_players(round_number)))


    owned_be = [next((n for n in break_evens if n["id"] == player_id), None) for player_id in current_players]


    pprint(owned_be)

if __name__ == "__main__":
    # Example usage
    print(con.sql("SELECT * FROM )"))
    con.sql("SELECT id, first_name, last_name, be  ")