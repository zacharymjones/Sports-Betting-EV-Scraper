# Sports Betting EV Scraper

![image](https://github.com/user-attachments/assets/5b63de7a-d3bd-4309-9f40-adeb2439e13d)

## Overview
This project is a sports betting edge-finding tool that scrapes betting odds from two sportsbooks: Pinnacle, a sharp sportsbook, and BetMGM, a recreational sportsbook. The scraper identifies +EV (positive expected value) bets by comparing BetMGM odds against devigged Pinnacle odds (fair odds). The results are then sent to a Discord server via a bot, and users can track their bets.

## Features
- **Scrape Betting Odds**: Mainline bets (moneylines, spreads, totals) are scraped from both Pinnacle and BetMGM.
- **Devigging and Fair Odds Calculation**: Pinnacle odds are devigged to calculate fair odds, which are then compared to BetMGM's lines to identify +EV opportunities.
- **+EV Bet Tracking**: Bets with +EV on BetMGM are identified and sent to a Discord server.
- **User Interaction via Discord**: Users can thumbs-up a message to lock in the odds and track their bets. SQLAlchemy is used to track user-specific odds and bet lines.
- **Notifications for Line Movements**: If a line moves against a user's bet and it becomes -EV, the user is notified.
- **Concise Bet Table**: A table showing all +EV bets at the moment is available for easy reference.
- **Line Movement Graphs**: Individual bets are plotted to visualize line movements over time.

## Technologies Used
- **Selenium**: To scrape the odds from Pinnacle and BetMGM.
- **Discord API**: To send messages and interact with users via a bot.
- **SQLAlchemy**: For managing bet data and user interactions in the database.
- **Python**: The core language for the project.




