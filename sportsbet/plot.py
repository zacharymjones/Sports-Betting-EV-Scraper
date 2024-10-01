import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from sportsbet.sql import *

# Function to create the plot
def create_plot(engine, team_1, team_2, bet_name, sport):
    # Read data from SQLAlchemy table into a pandas DataFrame
    query = select(bet_table).where(
        (bet_table.c.team_1 == team_1) &
        (bet_table.c.team_2 == team_2) &
        (bet_table.c.bet_name == bet_name) &
        (bet_table.c.sport == sport)
    )

    # Execute the query and fetch data into a DataFrame
    with engine.connect() as conn:
        data = pd.read_sql_query(query, conn)

    # Convert scrape_time to datetime
    data['scrape_time'] = pd.to_datetime(data['scrape_time'])

    if data.empty:
        print(f"No data available for {team_1} vs {team_2}, {sport}, {bet_name}.")
        return

    # Sort by scrape_time
    data = data.sort_values('scrape_time')

    fig = plt.figure(figsize=(6, 6))
    plt.plot(data['scrape_time'], data['sharp_odds'], label='Sharp Odds', marker='o')
    plt.plot(data['scrape_time'], data['rec_odds'], label='Rec Odds', marker='s')
    plt.plot(data['scrape_time'], data['fair_odds'], label='Fair Odds', marker='^')

    # Format x-axis as '%H:%M'
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())

    plt.title(f'{team_1} vs {team_2} {bet_name}')
    plt.xlabel('Time')
    plt.xticks(fontsize=8)
    plt.ylabel('Odds')
    plt.grid(True)

    # Move legend outside of plot
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)

    # Save the plot to a file
    image_path = f'../images/{team_1}_{team_2}_{bet_name}_{sport}.png'
    plt.savefig(image_path, bbox_inches='tight')
    plt.close()  # Close the figure to free up memory

    return image_path