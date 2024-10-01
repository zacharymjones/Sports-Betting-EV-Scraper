from datetime import datetime
from sportsbet.scraper import *
from sportsbet.sql import *
import time
from sqlalchemy.orm import sessionmaker

def run_scrape():
    engine = sa.create_engine('sqlite:///database.db')
    connection = engine.connect()


    sports = [{'sport': 'Baseball',
               'rec_url': 'https://sports.nj.betmgm.com/en/sports/baseball-23/betting/usa-9/mlb-75',
               'pin_url': 'https://www.pinnacle.com/en/baseball/mlb/matchups/#period:0'},
              {'sport': 'NFL',
               'rec_url': 'https://sports.nj.betmgm.com/en/sports/football-11/betting/usa-9/nfl-35',
               'pin_url': 'https://www.pinnacle.com/en/football/nfl/matchups/#period:0'}]

    for sport in sports:
        games = scrape_mgm(sport)
        for i, game in enumerate(games):
            df = game['dataframe']
            if not df.empty and 'EV' in df.columns:
                # Iterate over each row of the filtered df and store in the database

                today_date = datetime.now().strftime('%Y-%m-%d')
                game_time_str = game['time']
                game_time_with_date = f"{today_date} {game_time_str}"
                for _, row in df.iterrows():
                    team_1 = game['teams'][0]
                    team_2 = game['teams'][1]
                    sport_name = sport['sport']
                    bet_type = row['name']  # Adjust this based on your DataFrame structure
                    sharp_odds = row['sharp price']
                    sharp_odds_opp = row['sharp price opp']
                    rec_odds = row['rec price']
                    fair_odds = row['fair price']
                    EV = row['EV']
                    scrape_time = datetime.now()
                    game_time = datetime.strptime(game_time_with_date, '%Y-%m-%d %I:%M %p')
                    insert_bet(team_1, team_2, sport_name, bet_type,
                               sharp_odds, sharp_odds_opp, fair_odds, rec_odds, EV, scrape_time, game_time)
    print('Finished scraping.')
try:
    run_scrape()
except Exception as e:
    print(f'Error running run_scrape: {e}')