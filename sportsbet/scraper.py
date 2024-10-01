from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import pandas as pd
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
import threading
from sportsbet.book_calculations import *


# Set up Chrome options
chrome_options = Options()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--window-size=500,500')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_experimental_option("useAutomationExtension", False)
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_argument("user-agent=Mozilla/5.0")


def scrape_pinnacle(driver, list_games, url):
    driver.set_window_size(1920, 1080)
    driver.get(url)


    try:
        dismiss = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div.container-ff6ef9a2fe27ca2870eb button'))
        )
        dismiss.click()
    except:
        pass

    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div.flex-row'))
        )
        pin_events = driver.find_elements(By.CSS_SELECTOR, 'div.flex-row')
    except:
        return

    for pin_event_index in range(len(pin_events)):
        odds_data = []
        try:
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'div.flex-row'))
            )
            pin_events = driver.find_elements(By.CSS_SELECTOR, 'div.flex-row')
            pin_event = pin_events[pin_event_index]
        except:
            continue
        names = pin_event.find_elements(By.CSS_SELECTOR, 'span.event-row-participant')
        team_1 = names[0].text
        team_2 = names[1].text

        if 'Home' in team_1 or 'Away' in team_1:
            continue
        try:
            if pin_event.find_element(By.CLASS_NAME, 'live-ac9705c62bc6980a86d9'):
                continue
        except:
            pass

        for game in list_games:
            game_teams = game['teams']
            if game_teams[0] in team_1 and game_teams[1] in team_2:
                pin_event.click()
                collapse_tabs = WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'button.showAllButton-dcec795edcb102c0ebd9'))
                )
                collapse_tabs.click()
                if collapse_tabs.text == 'Hide All':
                    collapse_tabs.click()

                # Money line scrape
                try:
                    moneyline_tab = WebDriverWait(driver, 30).until(
                        EC.presence_of_element_located((By.XPATH, "//span[contains(text(), 'Money Line')]"))
                    )
                    moneyline_tab.click()

                    WebDriverWait(driver, 30).until(
                        EC.presence_of_element_located((By.XPATH, "//button[contains(@class, 'market-btn')]"))
                    )
                    buttons = driver.find_elements(By.XPATH, "//button[contains(@class, 'market-btn')]")

                    for i, button in enumerate(buttons):
                        index = i % 2
                        name = game_teams[index]
                        price = button.find_element(By.CLASS_NAME, 'price-af9054d329c985ad490f').text
                        price = decimal_to_american(float(price))
                        odds_data.append({'name': name, 'sharp price': price})
                    moneyline_tab.click()

                except:
                    pass
                # Handicap scrape (Spread)
                try:
                    handicap_tab = WebDriverWait(driver, 30).until(
                        EC.presence_of_element_located((By.XPATH, "//span[contains(text(), 'Handicap')]"))
                    )
                    handicap_tab.click()

                    expand = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located(
                            (By.CSS_SELECTOR, 'button.toggleMarkets-e2ba630a691b2591fba9'))
                    )
                    expand.click()
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.XPATH, "//button[contains(@class, 'market-btn')]"))
                    )
                    buttons = driver.find_elements(By.XPATH, "//button[contains(@class, 'market-btn')]")
                    for i, button in enumerate(buttons):
                        team_index = i % 2

                        name = f'{button.find_element(By.CLASS_NAME, 'label-e0291710e17e8c18f43f').text} {game_teams[team_index]}'
                        price = button.find_element(By.CLASS_NAME, 'price-af9054d329c985ad490f').text
                        price = decimal_to_american(float(price))
                        odds_data.append({'name': name, 'sharp price': price})
                    handicap_tab.click()

                except:
                    pass

                # Total scrape
                try:
                    total_tab = WebDriverWait(driver, 30).until(
                        EC.presence_of_element_located((By.XPATH, "//span[contains(text(), 'Total')]"))
                    )
                    total_tab.click()

                    expand = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located(
                            (By.CSS_SELECTOR, 'button.toggleMarkets-e2ba630a691b2591fba9'))
                    )
                    expand.click()

                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.XPATH, "//button[contains(@class, 'market-btn')]"))
                    )
                    buttons = driver.find_elements(By.XPATH, "//button[contains(@class, 'market-btn')]")
                    for button in buttons:
                        name = button.find_element(By.CLASS_NAME, 'label-e0291710e17e8c18f43f').text
                        price = button.find_element(By.CLASS_NAME, 'price-af9054d329c985ad490f').text
                        price = decimal_to_american(float(price))
                        odds_data.append({'name': name, 'sharp price': price})
                    total_tab.click()

                except:
                    pass

                df = pd.DataFrame(odds_data)
                print(df)
                if len(df) >= 2:
                    # Initialize the 'fair price' column with None values
                    df['fair price'] = None
                    # Iterate over the DataFrame in pairs of rows
                    for i in range(0, len(df) - 1, 2):
                            if i + 1 < len(df):
                                odds_sharp_1 = float(df.iloc[i]['sharp price'])
                                odds_sharp_2 = float(df.iloc[i + 1]['sharp price'])
                                # Calculate fair odds
                                fair_price = get_fair_odds(odds_sharp_1, odds_sharp_2)

                                # Update the 'fair price' column for both rows in the pair
                                df.at[i, 'fair price'] = fair_price[0]
                                df.at[i + 1, 'fair price'] = fair_price[1]
                    combined_df = pd.merge(df, game['dataframe'], on='name', how='inner')
                    print(combined_df)
                try:
                    combined_df = combined_df.drop(columns=['sharp price_x'])
                    combined_df = combined_df.drop(columns=['fair price_x'])
                    combined_df = combined_df.rename(
                        columns={'sharp price_y': 'sharp price', 'fair price_y': 'fair price'})
                except:
                    pass

                combined_df['EV'] = None
                combined_df['sharp price opp'] = None
                # Calculate EV
                for ind in range(0, len(combined_df) - 1, 2):
                    try:
                        if ind + 1 < len(combined_df):
                            # Extract the book odds (from 'price_x' and 'price_y') and sharp odds (from 'fair price')
                            book_odds_1 = float(combined_df.iloc[ind]['rec price'])
                            book_odds_2 = float(combined_df.iloc[ind + 1]['rec price'])
                            sharp_odds_1 = float(combined_df.iloc[ind]['sharp price'])
                            sharp_odds_2 = float(combined_df.iloc[ind + 1]['sharp price'])

                            # Calculate EV for each row and update the DataFrame
                            ev_1 = calculate_ev(book_odds_1, sharp_odds_1, sharp_odds_2)
                            ev_2 = calculate_ev(book_odds_2, sharp_odds_2, sharp_odds_1)

                            combined_df.at[ind, 'EV'] = ev_1
                            combined_df.at[ind, 'sharp price opp'] = sharp_odds_2
                            combined_df.at[ind + 1, 'EV'] = ev_2
                            combined_df.at[ind + 1, 'sharp price opp'] = sharp_odds_1
                    except Exception as e:
                        print(f'Error processing EV for rows {ind} and {ind + 1}: {str(e)}')
                        continue
                # Remove rows where EV is None
                combined_df.dropna(subset=['EV'], inplace=True)
                game['dataframe'] = combined_df
                driver.back()

def process_events(driver, sport, start_index, end_index, thread_results):
    driver.set_window_size(1920, 1080)
    if driver.current_url != sport['rec_url']:
        driver.get(sport['rec_url'])
    action = ActionChains(driver)

    # Close out any pop-up promotion
    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'clock-time'))
        )
        close_promo = driver.find_element(By.CSS_SELECTOR, 'fast-svg')
        close_promo.click()
    except:
        pass

    # Proceed to scrape events within the given range
    for event_index in range(start_index, end_index):
        try:
            odds_data = []
            event = WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, f'.grid-event:nth-child({event_index + 1})'))
            )

            # Skip live events and events not today
            try:
                if event.find_element(By.CSS_SELECTOR, '.live-icon'):
                    continue
            except:
                pass

            try:
                match_time = event.find_element(By.CLASS_NAME, 'pre-match-time').text
                if 'Today' not in match_time and 'Starting in' not in match_time:
                    continue
            except:
                continue

            # Get team names
            teams = event.find_elements(By.CSS_SELECTOR, 'div.participant')
            teams = [team.text for team in teams]
            # Get money line
            try:
                main_bets = event.find_elements(By.CSS_SELECTOR, 'div.option-indicator')
                for i, bet in enumerate(main_bets):
                    price = bet.text.strip()
                    index = i % 2
                    if len(price.split()) == 1 and '.' not in price:
                        odds_data.append({'name': teams[index], 'rec price': price})
            except:
                pass

            # Click composable widget
            try:
                composable_widget = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CLASS_NAME, 'composable'))
                )
                action.move_to_element(composable_widget).perform()
                driver.execute_script("arguments[0].scrollIntoView();", event)
                action.move_to_element(event).click().perform()
            except:
                continue

            # Get time
            try:
                time_element = WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'span.time'))
                )
                match_time = time_element.text
            except:
                pass
            # Handicap scrape (Spread)
            try:
                spread_button = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'button[data-menu-item-id="Spread"]'))
                )
                spread_button.click()
            except:
                pass

            # Show more (if available)
            try:
                show_more = driver.find_element(By.CSS_SELECTOR, 'div.show-more-less-button')
                show_more.click()
            except:
                pass

            try:
                spread_group = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, '//ms-spread-option-group'))
                )
                spreads = spread_group.find_elements(By.CSS_SELECTOR, 'div.option-indicator')

                for i, spread in enumerate(spreads):
                    index = i % 2
                    spread_name = spread.find_element(By.CSS_SELECTOR, 'div.name')
                    name = f'{spread_name.text} {teams[index]}'

                    price = spread.find_element(By.CSS_SELECTOR, 'div.value span.custom-odds-value-style').text
                    odds_data.append({'name': name, 'rec price': price})
            except:
                pass

            # Totals scrape
            try:
                totals_button = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'button[data-menu-item-id="Totals"]'))
                )
                totals_button.click()
            except:
                pass

            # Collapse all tabs
            try:
                expanded_tabs = WebDriverWait(driver, 5).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div.option-group-name div.expanded'))
                )
                for tab in expanded_tabs:
                    if 'Totals' not in tab.text:
                        tab.click()
            except:
                pass

            # Show more (if available)
            try:
                show_more = driver.find_element(By.CSS_SELECTOR, 'div.show-more-less-button')
                show_more.click()
            except:
                pass

            try:
                container = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'div.option-group-container'))
                )
                totals = container.find_elements(By.CSS_SELECTOR, 'div.option-indicator')
                items = container.find_elements(By.CSS_SELECTOR, 'div.attribute-key')
                names = []

                for item in items:
                    if item.text == '7' or item.text == '38':
                        action.move_to_element(container).perform()
                        driver.execute_script("arguments[0].scrollIntoView();", item)
                    names.append(f'Over {item.text}')
                    names.append(f'Under {item.text}')

                for i, odd in enumerate(totals):
                    price = odd.text
                    name = names[i]
                    odds_data.append({'name': name, 'rec price': price})
            except:
                pass

            df = pd.DataFrame(odds_data)
            thread_results.append({
                'teams': teams,
                'dataframe': df,
                'time': match_time
            })
            driver.back()

        except Exception as e:
            print(f'Error processing event {event_index}: {str(e)}')
            continue  # Skip to the next event if there is an error

    if thread_results:
        scrape_pinnacle(driver, thread_results, sport['pin_url'])
    else:
        driver.quit()


def scrape_mgm(sport):
    games = []
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.set_window_size(1920, 1080)
    driver.get(sport['rec_url'])

    # Wait until the elements with the required class are present
    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '.grid-event'))
        )
        events = driver.find_elements(By.CSS_SELECTOR, '.grid-event')

        filtered_events = 0
        for i, event in enumerate(events):
            events = driver.find_elements(By.CSS_SELECTOR, '.grid-event')
            event = events[i]
            # Skip live events and events not today
            try:
                if event.find_element(By.CSS_SELECTOR, '.live-icon'):
                    continue
            except:
                pass
            try:
                match_time = event.find_element(By.CLASS_NAME, 'pre-match-time').text
                if 'Today' not in match_time and 'Starting in' not in match_time:
                    continue
            except:
                pass
            filtered_events += 1

    except:
        print(f'Timeout occurred while waiting for page to load.')
        driver.quit()
        return games  # Exit function if timeout occurs


    num_threads = min(4, filtered_events)
    try:
        chunk_size = len(events) // num_threads
    except:
        return games
    threads = []
    results = []

    thread_results = [[] for _ in range(num_threads)]

    driver_instances = [driver] + [webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options) for _ in range(num_threads - 1)]
    # Create and start threads for each chunk
    for i in range(num_threads):
        start_index = i * chunk_size
        end_index = (i + 1) * chunk_size if i != num_threads - 1 else len(events)
        thread = threading.Thread(target=process_events, args=(driver_instances[i], sport, start_index, end_index, thread_results[i]))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Combine all thread results
    for result in thread_results:
        results.extend(result)

    # Combine results from all threads
    games.extend(results)

    return games
