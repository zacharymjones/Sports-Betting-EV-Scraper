from sportsbet.sql import *
import subprocess
from multiprocessing import Process
import sys
import time
# today_date = datetime.now().strftime('%Y-%m-%d')
# game_time_str = '9:10 PM'
# game_time_with_date = f"{today_date} {game_time_str}"
# scrape_time = datetime.now()
# game_time = datetime.strptime(game_time_with_date, '%Y-%m-%d %I:%M %p')

#insert_bet('Team 1', 'Team 2', 'Test Sport', 'Over 11.5', 151, -151, 150, 131, -3, scrape_time, game_time)

#clear_bets_table()
#clear_messages_table()
#clear_pms_table()
#view_bet_table()
# view_message_table()
#view_pm_table()


def run_discord_bot():
    # Run the Discord bot and wait for it to complete
    subprocess.run([sys.executable, 'discord_bot.py'], check=True)


def run_scraper():
    # Run the scraper and wait for it to complete
    subprocess.run([sys.executable, 'run_scrape.py'], check=True)


def main():
    while True:
        print("Running Discord bot...")
        run_discord_bot()  # Run the Discord bot first
        print("Running scraper...")
        run_scraper()  # Then run the scraper


# def main():
#     # Create two separate processes, passing the manager's lock
#     p1 = Process(target=run_discord_bot)
#     p2 = Process(target=run_scraper)
#
#     # Start both processes
#     p1.start()
#     p2.start()
#
#     # Wait for both processes to finish
#     p1.join()
#     p2.join()

if __name__ == "__main__":
    main()