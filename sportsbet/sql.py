import sqlalchemy as sa
from sqlalchemy import delete, select
import pandas as pd
from datetime import datetime

engine = sa.create_engine('sqlite:///database.db')
connection = engine.connect()

metadata = sa.MetaData()

bet_table = sa.Table(
    'bet',
    metadata,
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column('team_1', sa.String),
    sa.Column('team_2', sa.String),
    sa.Column('sport', sa.String),
    sa.Column('bet_name', sa.String),
    sa.Column('sharp_odds', sa.Integer),
    sa.Column('sharp_odds_opp', sa.Integer),
    sa.Column('fair_odds', sa.Integer),
    sa.Column('rec_odds', sa.Integer),
    sa.Column('EV', sa.Float),
    sa.Column('scrape_time', sa.DateTime),
    sa.Column('game_time', sa.DateTime)
)

message_table = sa.Table(
    'message',
    metadata,
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column('team_1', sa.String),
    sa.Column('team_2', sa.String),
    sa.Column('sport', sa.String),
    sa.Column('bet_name', sa.String),
    sa.Column('sharp_odds', sa.Integer),
    sa.Column('sharp_odds_opp', sa.Integer),
    sa.Column('fair_odds', sa.Integer),
    sa.Column('rec_odds', sa.Integer),
    sa.Column('EV', sa.Float),
    sa.Column('reactions', sa.String),
    sa.Column('message_id', sa.String)
)

pm_table = sa.Table(
    'pm',
    metadata,
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column('team_1', sa.String),
    sa.Column('team_2', sa.String),
    sa.Column('sport', sa.String),
    sa.Column('bet_name', sa.String),
    sa.Column('sharp_odds', sa.Integer),
    sa.Column('sharp_odds_opp', sa.Integer),
    sa.Column('fair_odds', sa.Integer),
    sa.Column('rec_odds', sa.Integer),
    sa.Column('EV', sa.Float),
    sa.Column('user', sa.String),
    sa.Column('message_id', sa.String)
)
metadata.create_all(engine)

def view_bet_table():
    query = sa.select(bet_table)
    result = connection.execute(query)
    bets = result.fetchall()
    df = pd.DataFrame(bets, columns=result.keys())
    print(df)

def view_message_table():
    query = sa.select(message_table)
    result = connection.execute(query)
    bets = result.fetchall()
    df = pd.DataFrame(bets, columns=result.keys())
    print(df)


def view_pm_table():
    query = sa.select(pm_table)
    result = connection.execute(query)
    bets = result.fetchall()
    df = pd.DataFrame(bets, columns=result.keys())
    print(df)

def insert_bet(team_1: str, team_2: str, sport: str, bet_name: str, sharp_odds: int, sharp_odds_opp: int, fair_odds: int, rec_odds: int,
               EV: float, scrape_time: datetime, game_time: datetime) -> None:
    try:
        # Query to check for existing bets with the same team_1, team_2, sport, and bet_name
        query = select(
            bet_table.c.sharp_odds,
            bet_table.c.sharp_odds_opp,
            bet_table.c.fair_odds,
            bet_table.c.rec_odds,
            bet_table.c.EV
        ).where(
            (bet_table.c.team_1 == team_1) &
            (bet_table.c.team_2 == team_2) &
            (bet_table.c.sport == sport) &
            (bet_table.c.bet_name == bet_name)
        ).order_by(bet_table.c.id.desc())  # Get the most recent entry

        # Execute the query and fetch the result
        result = connection.execute(query)
        last_entry = result.fetchone()

        # Debugging prints
        print(f"Last entry fetched: {last_entry}")
        print(f"Comparing: Sharp Odds={sharp_odds}, Fair Odds={fair_odds}, Rec Odds={rec_odds}")

        # Check if the last entry exists and compare odds
        if last_entry:
            last_sharp_odds, last_sharp_odds_opp, last_fair_odds, last_rec_odds, last_EV = last_entry
            print(f"Last entry odds: Sharp Odds={last_sharp_odds}, Sharp Odds Opp={last_sharp_odds_opp}, Fair Odds={last_fair_odds}, Rec Odds={last_rec_odds}")
            if (int(sharp_odds) == int(last_sharp_odds) and
                    int(sharp_odds_opp) == int(last_sharp_odds_opp) and
                    int(fair_odds) == int(last_fair_odds) and
                    int(rec_odds) == int(last_rec_odds) and
                    float(EV) == float(last_EV)):
                print("No change in odds. No new entry inserted.")
                return  # No change in odds, so do not insert

        # Insert the new bet as it's either the first entry or odds have changed
        insert_query = bet_table.insert().values(
            team_1=team_1,
            team_2=team_2,
            sport=sport,
            bet_name=bet_name,
            sharp_odds=sharp_odds,
            sharp_odds_opp=sharp_odds_opp,
            fair_odds=fair_odds,
            rec_odds=rec_odds,
            EV=EV,
            scrape_time=scrape_time,
            game_time=game_time
        )

        # Debugging print for the SQL insert query
        print(
            f"Inserting new bet with values: {team_1}, {team_2}, {sport}, {bet_name}, {sharp_odds}, {sharp_odds_opp}, {fair_odds}, {rec_odds}, {EV}, {scrape_time}, {game_time}")

        connection.execute(insert_query)
        connection.commit()
        print("New bet inserted.")
    except Exception as e:
        print(f'Error inserting bet: {e}')

def clear_bets_table():
    # Define the delete query for the bet_table
    query = delete(bet_table)

    # Execute the delete query
    connection.execute(query)
    connection.commit()
    print("All records from the 'bet' table have been cleared.")

def clear_messages_table():
    # Define the delete query for the bet_table
    query = delete(message_table)

    # Execute the delete query
    connection.execute(query)
    connection.commit()
    print("All records from the 'message' table have been cleared.")


def clear_pms_table():
    # Define the delete query for the bet_table
    query = delete(pm_table)

    # Execute the delete query
    connection.execute(query)
    connection.commit()
    print("All records from the 'message' table have been cleared.")
