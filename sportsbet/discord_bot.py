from sqlalchemy import insert, update, func, select, create_engine, Table, MetaData, delete, and_
import os
from discord import Intents
from discord.ext import commands, tasks
from dotenv import load_dotenv
import discord
from sportsbet.plot import *
from sportsbet.sql import *
from datetime import datetime
from sportsbet.book_calculations import *
from table2ascii import table2ascii, Alignment, PresetStyle
import asyncio
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text

engine = sa.create_engine('sqlite:///database.db')
connection = engine.connect()

metadata = sa.MetaData()


async def is_db_available():
    try:
        # PRAGMA command to check for locks
        result = connection.execute(text('PRAGMA busy_timeout = 5000'))  # Wait up to 5000 ms if the database is busy
        return True
    except sa.exc.OperationalError as e:
        print(f"Database is locked or unavailable: {e}")
        return False


# Initialize the Discord bot
intents = Intents.default()
intents.message_content = True
intents.guilds = True
intents.messages = True
intents.reactions = True
bot = commands.Bot(command_prefix='$', intents=intents)

# Load environment variables
load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')
CHANNEL = os.getenv('BETMGM_CHANNEL')
APPENDIX = os.getenv('APPENDIX_CHANNEL')
GUILD = os.getenv('GUILD_ID')

async def cleanup_bets():
    """
    Deletes Discord messages and DMs for bets that have a negative EV or are for past games,
    based on the most recent scrape. Sends a DM to the user informing them of the negative EV or that the game has started.
    If the DM is deleted, removes the user's ID from the reactions in the message_table.
    Also removes bets from the bet_table if the game has started.
    """

    channel = await bot.fetch_channel(CHANNEL)
    processed_dm_ids = set()  # Set to track processed DM IDs

    try:
        # Subquery to get the latest scrape_time for each game
        subquery = select(
            bet_table.c.team_1,
            bet_table.c.team_2,
            bet_table.c.sport,
            bet_table.c.bet_name,
            func.max(bet_table.c.scrape_time).label('latest_scrape_time')
        ).group_by(
            bet_table.c.team_1,
            bet_table.c.team_2,
            bet_table.c.sport,
            bet_table.c.bet_name
        ).alias('latest_bets')

        # Join the bet_table with the subquery on the latest scrape_time to get only the latest EV
        latest_bets_query = select(
            bet_table.c.EV,
            bet_table.c.game_time,
            message_table.c.message_id,
            message_table.c.team_1,
            message_table.c.team_2,
            message_table.c.sport,
            message_table.c.bet_name
        ).select_from(
            message_table.join(
                bet_table,
                and_(
                    message_table.c.team_1 == bet_table.c.team_1,
                    message_table.c.team_2 == bet_table.c.team_2,
                    message_table.c.sport == bet_table.c.sport,
                    message_table.c.bet_name == bet_table.c.bet_name
                )
            ).join(
                subquery,
                and_(
                    bet_table.c.team_1 == subquery.c.team_1,
                    bet_table.c.team_2 == subquery.c.team_2,
                    bet_table.c.sport == subquery.c.sport,
                    bet_table.c.bet_name == subquery.c.bet_name,
                    bet_table.c.scrape_time == subquery.c.latest_scrape_time
                )
            )
        )

        # Execute the query to fetch the latest EV, game_time, and message_id
        result = connection.execute(latest_bets_query)
        messages_to_clean = result.fetchall()

        # Loop through the messages and perform necessary deletions
        for message in messages_to_clean:
            message_id = message.message_id
            ev = message.EV
            game_time = message.game_time
            team_1 = message.team_1
            team_2 = message.team_2
            sport = message.sport
            bet_name = message.bet_name

            # If EV is negative or game_time is past, delete the public message
            if ev < 0 or game_time < datetime.now():
                if message_id:
                    try:
                        old_message = await channel.fetch_message(message_id)
                        await old_message.delete()

                        # Optionally: Remove the record from the message_table
                        delete_query = message_table.delete().where(message_table.c.message_id == message_id)
                        connection.execute(delete_query)

                    except discord.errors.NotFound:
                        print(f"Message with ID {message_id} not found. It might have been deleted already.")
                    except Exception as e:
                        print(f"Error deleting message with ID {message_id}: {e}")

        # Now handle the DM cleanup
        pm_query = select(
            pm_table.c.message_id,
            pm_table.c.user,
            pm_table.c.EV,
            bet_table.c.game_time,
            bet_table.c.team_1,
            bet_table.c.team_2,
            bet_table.c.sport,
            bet_table.c.bet_name
        ).select_from(
            pm_table.join(
                bet_table,
                and_(
                    pm_table.c.team_1 == bet_table.c.team_1,
                    pm_table.c.team_2 == bet_table.c.team_2,
                    pm_table.c.sport == bet_table.c.sport,
                    pm_table.c.bet_name == bet_table.c.bet_name
                )
            )
        )

        # Execute the query to fetch the necessary data
        pm_result = connection.execute(pm_query)
        pm_entries = pm_result.fetchall()

        # Loop through the PM entries and perform necessary deletions
        for entry in pm_entries:
            dm_message_id = entry.message_id
            user_id = entry.user[2:-1]  # Ensure this correctly extracts the user ID
            ev = entry.EV
            game_time = entry.game_time
            team_1 = entry.team_1
            team_2 = entry.team_2
            sport = entry.sport
            bet_name = entry.bet_name

            # If EV is negative or game_time is past, delete the DM
            if ev < 0 or game_time < datetime.now():
                if dm_message_id and dm_message_id not in processed_dm_ids:
                    processed_dm_ids.add(dm_message_id)  # Mark as processed
                    try:
                        # Fetch user and delete DM
                        user = await bot.fetch_user(int(user_id))  # Fetch user by ID
                        if user:
                            old_dm_message = await user.fetch_message(dm_message_id)
                            await old_dm_message.delete()

                            # Send different DMs based on the reason
                            if ev < 0:
                                notification_message = (
                                    f"Hi! The bet you placed has been updated and now has a negative expected value (EV). "
                                    f"Details are as follows:\n"
                                    f"**Teams**: {team_1} vs {team_2}\n"
                                    f"**Sport**: {sport}\n"
                                    f"**Bet**: {bet_name}\n"
                                    f"Please check the latest information to make informed decisions."
                                )
                            else:
                                notification_message = (
                                    f"Hi! The game for the following bet has already started:\n"
                                    f"**Teams**: {team_1} vs {team_2}\n"
                                    f"**Sport**: {sport}\n"
                                    f"**Bet**: {bet_name}\n"
                                )

                            try:
                                await user.send(notification_message)
                            except discord.errors.Forbidden:
                                print(
                                    f"Unable to send DM to user with ID {user_id}. They might have DMs disabled or the bot may not have permission.")

                            # Optionally: Remove the record from the pm_table
                            delete_query = pm_table.delete().where(pm_table.c.message_id == dm_message_id)
                            connection.execute(delete_query)

                            # Remove the user's ID from the reactions in the message_table
                            message_table_update_query = select(
                                message_table.c.message_id,
                                message_table.c.reactions
                            ).where(
                                and_(
                                    message_table.c.team_1 == team_1,
                                    message_table.c.team_2 == team_2,
                                    message_table.c.sport == sport,
                                    message_table.c.bet_name == bet_name
                                )
                            )
                            reaction_result = connection.execute(message_table_update_query)
                            reactions = reaction_result.fetchone()
                            if reactions:
                                old_reactions_str = reactions[1]
                                old_reactions = set(old_reactions_str.split()) if old_reactions_str else set()
                                updated_reactions = old_reactions - {f'<@{user_id}>'}
                                updated_reactions_str = ' '.join(updated_reactions)

                                update_query = message_table.update().where(
                                    and_(
                                        message_table.c.team_1 == team_1,
                                        message_table.c.team_2 == team_2,
                                        message_table.c.sport == sport,
                                        message_table.c.bet_name == bet_name
                                    )
                                ).values(
                                    reactions=updated_reactions_str
                                )
                                connection.execute(update_query)

                        else:
                            print(f"User with ID {user_id} not found.")

                    except discord.errors.NotFound:
                        print(f"DM with ID {dm_message_id} not found. It might have been deleted already.")
                    except Exception as e:
                        print(f"Error deleting DM with ID {dm_message_id}: {e}")

        # Clean up old bets from bet_table
        cleanup_query = bet_table.delete().where(bet_table.c.game_time < datetime.now())
        connection.execute(cleanup_query)

        connection.commit()

    except Exception as e:
        print(f"Error cleaning negative EV bets and DMs: {e}")

async def send_new_message(channel: discord.TextChannel, bet_name: str, message_content: str, image_path: str = None,
                           reaction_users_str=None, team_1=None, team_2=None, sport=None, sharp_odds=None,
                           sharp_odds_opp=None, fair_odds=None,
                           rec_odds=None, ev=None):
    reaction_users = set()
    # Query to check for existing message with the same team_1, team_2, sport, and bet_name
    try:
        query = select(
            message_table.c.message_id,
            message_table.c.sharp_odds,
            message_table.c.sharp_odds_opp,
            message_table.c.fair_odds,
            message_table.c.rec_odds,
            message_table.c.EV,
            message_table.c.reactions
        ).where(
            (message_table.c.team_1 == team_1) &
            (message_table.c.team_2 == team_2) &
            (message_table.c.sport == sport) &
            (message_table.c.bet_name == bet_name)
        ).order_by(message_table.c.id.desc()).limit(1)  # Get the most recent entry

        # Execute the query and fetch the result
        result = connection.execute(query)
        last_entry = result.fetchone()
    except Exception as e:
        print(f"Error checking existing messages: {e}")
        last_entry = None

    if last_entry:
        message_id, old_sharp_odds, old_sharp_odds_opp, old_fair_odds, old_rec_odds, old_ev, old_reactions = last_entry
        old_reactions = old_reactions.split() if old_reactions else []
        old_message = await channel.fetch_message(message_id)

        # If odds have changed, handle reactions
        for reaction in old_message.reactions:
            if str(reaction.emoji) == '👍':  # Only track thumbs up reactions
                users = [user async for user in reaction.users()]
                reaction_users.update(f'<@{user.id}>' for user in users if not user.bot)

        reaction_users.update(old_reactions)
        reaction_users_str = ' '.join(reaction_users)

        # Update existing entry
        update_query = update(message_table).where(
            (message_table.c.team_1 == team_1) &
            (message_table.c.team_2 == team_2) &
            (message_table.c.sport == sport) &
            (message_table.c.bet_name == bet_name)
        ).values(
            reactions=reaction_users_str
        )
        connection.execute(update_query)
        connection.commit()

        for user_id in reaction_users:
            try:
                # Fetch the user object using the user ID
                user = await channel.guild.fetch_member(int(user_id.strip('<@>')))

                # Check if an entry already exists for this user
                query = select(
                    pm_table.c.message_id
                ).where(
                    (pm_table.c.team_1 == team_1) &
                    (pm_table.c.team_2 == team_2) &
                    (pm_table.c.sport == sport) &
                    (pm_table.c.bet_name == bet_name) &
                    (pm_table.c.user == user_id)
                ).limit(1)

                result = connection.execute(query)
                existing_entry = result.fetchone()

                # If no existing entry is found, send a DM and insert a new one
                if not existing_entry:
                    # Insert a new entry in the pm_table with the DM's message ID
                    insert_query = insert(pm_table).values(
                        team_1=team_1,
                        team_2=team_2,
                        sport=sport,
                        bet_name=bet_name,
                        user=user_id,
                        sharp_odds=sharp_odds,
                        sharp_odds_opp=sharp_odds_opp,
                        fair_odds=fair_odds,
                        rec_odds=rec_odds,
                        EV=ev,
                        message_id=None
                    )
                    connection.execute(insert_query)
                    connection.commit()
            except Exception as e:
                print(f"Error inserting PM message for user {user_id}: {e}")
        # Check if the odds have changed
        if (sharp_odds == old_sharp_odds and
                sharp_odds_opp == old_sharp_odds_opp and
                fair_odds == old_fair_odds and
                rec_odds == old_rec_odds and
                ev == old_ev):
            # Odds have not changed; do not send a new message
            return

        if reaction_users_str != '':
            message_content += f"{reaction_users_str} gambled"
        await old_message.delete()

    # Send the new message
    if image_path:
        try:
            with open(image_path, 'rb') as file:
                new_message = await channel.send(content=message_content, file=discord.File(file, image_path))
        except Exception as e:
            print(f"Error sending image: {e}")
        finally:
            os.remove(image_path)
    else:
        new_message = await channel.send(message_content)

    if last_entry:
        try:
            # Update existing entry
            update_query = update(message_table).where(
                (message_table.c.team_1 == team_1) &
                (message_table.c.team_2 == team_2) &
                (message_table.c.sport == sport) &
                (message_table.c.bet_name == bet_name)
            ).values(
                message_id=new_message.id,
                sharp_odds=sharp_odds,
                sharp_odds_opp=sharp_odds_opp,
                fair_odds=fair_odds,
                rec_odds=rec_odds,
                EV=ev,
                reactions=reaction_users_str
            )
            connection.execute(update_query)
            connection.commit()
        except SQLAlchemyError as e:
            print(e)
    else:
        try:
            # Insert new entry
            insert_query = insert(message_table).values(
                team_1=team_1,
                team_2=team_2,
                sport=sport,
                bet_name=bet_name,
                sharp_odds=sharp_odds,
                sharp_odds_opp=sharp_odds_opp,
                fair_odds=fair_odds,
                rec_odds=rec_odds,
                EV=ev,
                message_id=new_message.id,
                reactions=reaction_users_str
            )
            connection.execute(insert_query)
            connection.commit()
        except SQLAlchemyError as e:
            print(e)
async def send_bets_to_discord():
    # Read the entire table into a pandas DataFrame
    df = pd.read_sql_table('bet', engine)

    # Ensure `scrape_time` is in datetime format
    df['scrape_time'] = pd.to_datetime(df['scrape_time'])

    # Find the latest scrape_time for each unique bet
    latest_bets = df.loc[df.groupby(['team_1', 'team_2', 'sport', 'bet_name'])['scrape_time'].idxmax()]

    # Filter where EV > 0
    positive_ev_bets = latest_bets[latest_bets['EV'] > 0]

    # Sort if needed (e.g., by EV in descending order)
    sorted_df = positive_ev_bets.sort_values(by='game_time', ascending=True)

    channel = await bot.fetch_channel(CHANNEL)

    # Loop through each unique bet and create a plot
    for _, bet in sorted_df.iterrows():
        implied_win = get_fair_prob(bet['sharp_odds'], bet['sharp_odds_opp'])[0]
        kelly = f'{round(kelly_criterion(implied_win, bet['rec_odds']) * 100, 2)} U'
        implied_win = f'{round(implied_win * 100, 2)}% win'
        EV_formatted = f'+{bet['EV']}% EV'

        message_content = (f'{table2ascii(header=['Team', 'Bet Info', 'Expected', 'Kelly'],
                                          body=[[bet['team_1'][:min(len(bet['team_1']), 8)], bet['bet_name'], EV_formatted, kelly],
                                                [bet['team_2'][:min(len(bet['team_2']), 8)], odds_format(bet['rec_odds']), implied_win, bet['sport']]],
                                          style=PresetStyle.thin_compact,
                                          cell_padding=2,
                                          alignments=Alignment.CENTER
                                          )}\n'
                           f'{table2ascii(header=['Scraped', 'Start', 'Sharp', 'Fair', 'Rec'],
                                          body=[[bet['scrape_time'].strftime('%I:%M %p'), bet['game_time'].strftime('%I:%M %p'), str(odds_format(bet['sharp_odds'])), str(odds_format(bet['fair_odds'])), str(odds_format(bet['rec_odds']))]],
                                          style=PresetStyle.thin_thick,
                                          cell_padding=2,
                                          alignments=Alignment.CENTER)}\n'
                           f'👍 if you place bet\n')
        # Create the plot and get the image path
        image_path = create_plot(engine, bet['team_1'], bet['team_2'], bet['bet_name'], bet['sport'])
        await send_new_message(channel, bet['bet_name'], message_content, image_path, team_1=bet['team_1'],
                               team_2=bet['team_2'], sport=bet['sport'],
                               sharp_odds=bet['sharp_odds'], sharp_odds_opp=bet['sharp_odds_opp'],
                               fair_odds=bet['fair_odds'], rec_odds=bet['rec_odds'], ev=bet['EV']
                               )


async def send_or_update_dms():
    channel = await bot.fetch_channel(CHANNEL)
    try:
        # Query all entries in pm_table
        query = select(
            pm_table.c.id,
            pm_table.c.team_1,
            pm_table.c.team_2,
            pm_table.c.sport,
            pm_table.c.bet_name,
            pm_table.c.user,
            pm_table.c.sharp_odds,
            pm_table.c.sharp_odds_opp,
            pm_table.c.fair_odds,
            pm_table.c.rec_odds,
            pm_table.c.EV,
            pm_table.c.message_id
        )
        result = connection.execute(query)
        pm_entries = result.fetchall()

        # Iterate through all pm_table entries
        for entry in pm_entries:
            user_id = entry.user[2:-1]
            team_1 = entry.team_1
            team_2 = entry.team_2
            sport = entry.sport
            bet_name = entry.bet_name
            message_id = entry.message_id

            # Query to get the latest odds from the relevant table
            odds_query = select(
                bet_table.c.sharp_odds,
                bet_table.c.sharp_odds_opp,
                bet_table.c.fair_odds,
                bet_table.c.scrape_time,
                bet_table.c.game_time
            ).where(
                (bet_table.c.team_1 == team_1) &
                (bet_table.c.team_2 == team_2) &
                (bet_table.c.sport == sport) &
                (bet_table.c.bet_name == bet_name)
            ).order_by(bet_table.c.id.desc()).limit(1)

            latest_odds_result = connection.execute(odds_query)
            latest_odds = latest_odds_result.fetchone()

            if not latest_odds:
                continue  # Skip if no odds are found

            new_sharp_odds, new_sharp_odds_opp, new_fair_odds, scrape_time, game_time = latest_odds
            new_EV = calculate_ev(entry.rec_odds, new_sharp_odds, new_sharp_odds_opp)

            implied_win = get_fair_prob(new_sharp_odds, new_sharp_odds_opp)[0]
            kelly = f'{round(kelly_criterion(implied_win, entry.rec_odds) * 100, 2)} U'
            implied_win = f'{round(implied_win * 100, 2)}% win'
            EV_formatted = f'+{new_EV}% EV'

            dm_content = (f'{table2ascii(header=['Team', 'Bet Info', 'Expected', 'Kelly'],
                                         body=[[entry.team_1[:min(len(entry.team_1), 8)], entry.bet_name, EV_formatted, kelly],
                                               [entry.team_2[:min(len(entry.team_2), 8)], odds_format(entry.rec_odds), implied_win, entry.sport]],
                                         style=PresetStyle.thin_compact,
                                         cell_padding=2,
                                         alignments=Alignment.CENTER
                                         )}\n'
                          f'{table2ascii(header=['Scraped', 'Start', 'Sharp', 'Fair', 'Rec'],
                                         body=[[scrape_time.strftime('%I:%M %p'), game_time.strftime('%I:%M %p'), str(odds_format(new_sharp_odds)), str(odds_format(new_fair_odds)), str(odds_format(entry.rec_odds))]],
                                         style=PresetStyle.thin_thick,
                                         cell_padding=2,
                                         alignments=Alignment.CENTER)}\n')

            # Fetch the user object
            user = await channel.guild.fetch_member(int(user_id))
            # If message_id is None, send a new DM
            if message_id is None:

                dm_message = await user.send(content=dm_content)

                # Update pm_table with the message_id and new odds
                update_query = update(pm_table).where(
                    pm_table.c.id == entry.id
                ).values(
                    message_id=dm_message.id,
                    sharp_odds=new_sharp_odds,
                    sharp_odds_opp=new_sharp_odds_opp,
                    fair_odds=new_fair_odds,
                    EV=new_EV
                )
                connection.execute(update_query)
                connection.commit()

            # If message_id exists, check for odds changes
            else:
                if (new_sharp_odds != entry.sharp_odds or
                        new_sharp_odds_opp != entry.sharp_odds_opp or
                        new_fair_odds != entry.fair_odds):
                    # Try to delete the old message
                    try:
                        old_message = await user.fetch_message(message_id)
                        await old_message.delete()
                    except discord.NotFound:
                        print(f"Old message with ID {message_id} not found.")
                    except discord.Forbidden:
                        print(f"Cannot delete message for user {user_id}.")
                    except Exception as e:
                        print(f"Error deleting old message for user {user_id}: {e}")

                    dm_message = await user.send(content=dm_content)

                    # Update the pm_table with the new message_id and new odds
                    update_query = update(pm_table).where(
                        pm_table.c.id == entry.id
                    ).values(
                        message_id=dm_message.id,
                        sharp_odds=new_sharp_odds,
                        sharp_odds_opp=new_sharp_odds_opp,
                        fair_odds=new_fair_odds,
                        EV=new_EV
                    )
                    connection.execute(update_query)
                    connection.commit()

    except Exception as e:
        print(f"Error processing DMs: {e}")

async def update_bets_appendix():
    channel = await bot.fetch_channel(APPENDIX)
    try:
        result = select(
            message_table.c.team_1,
            message_table.c.team_2,
            message_table.c.bet_name,
            message_table.c.sport,
            message_table.c.rec_odds,
            message_table.c.sharp_odds,
            message_table.c.sharp_odds_opp,
            message_table.c.message_id,
            message_table.c.EV,
            bet_table.c.game_time  # Including game_time from bet_table
        ).distinct().join(
            bet_table,  # Performing the join
            (message_table.c.team_1 == bet_table.c.team_1) &
            (message_table.c.team_2 == bet_table.c.team_2) &
            (message_table.c.sport == bet_table.c.sport) &
            (message_table.c.bet_name == bet_table.c.bet_name)
        ).order_by(bet_table.c.game_time.asc())

        execute_results = connection.execute(result)
        bets = execute_results.fetchall()
        current_time = datetime.now().strftime('%I:%M %p')
        if not bets:
            message_content = f'No bets as of {current_time}'

        body_content = []
        # Format the message
        for bet in bets:
            message_row = [bet.team_1[:min(len(bet.team_1), 6)], bet.team_2[:min(len(bet.team_2), 6)], bet.bet_name,
                           odds_format(bet.rec_odds), f'{bet.EV}%', bet.game_time.strftime('%I:%M')]
            body_content.append(message_row)

        message_content = (f'{current_time}\n'
                           f'{table2ascii(header=['Team 1', 'Team 2', 'Bet Name', 'Odds', 'EV', 'Start'],
                                          body=body_content,
                                          style=PresetStyle.ascii_borderless,
                                          cell_padding=0,
                                          alignments=Alignment.CENTER
                                          )}')
        # Fetch the last message in the channel
        async for message in channel.history(limit=1):
            # Update the existing message
            await message.edit(content=message_content)
            return  # Exit the function after editing

        # Create a new message if none exists
        await channel.send(message_content)
    except SQLAlchemyError as e:
        print(e)

async def send_bets_to_discord_loop():
    while True:
        try:
            if await is_db_available():
                await cleanup_bets()
                await asyncio.sleep(0.1)
                await send_bets_to_discord()  # Call your existing function to send bets to Discord
                await asyncio.sleep(0.1)
                await update_bets_appendix()
                await asyncio.sleep(0.1)
                await send_or_update_dms()
                await asyncio.sleep(0.1)
            else:
                print('Database is busy, retrying later...')
                await asyncio.sleep(5)
        except Exception as e:
            print(f'Error running send_Bets_to_discord_loop:{e}')
        await asyncio.sleep(5)
@bot.event
async def on_ready():
    print(f'We have logged in as {bot.user}')

    # Run the Discord sending loop asynchronously
    asyncio.create_task(send_bets_to_discord_loop())

# Run the bot
bot.run(TOKEN)
