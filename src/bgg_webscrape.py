import time
import random
import os

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from bs4 import BeautifulSoup
import pandas as pd
from dotenv import load_dotenv

# load environment variables
load_dotenv()
bgg_username = os.getenv("BGG_USERNAME")
bgg_password =  os.getenv("BGG_PASSWORD")

static_vars = {
    "local_driver_path": r"chromedriver.exe",
    "url": r'https://boardgamegeek.com/browse/boardgame/page/1',
    "sign_in_btn":'//*[@id="global-header-outer"]/header/nav/div/div[2]/div/div[1]/ul/li[7]/button',
    "username_box":'//*[@id="inputUsername"]',
    "password_box": '//*[@id="inputPassword"]',
    "sign_in":'/html/body/div[1]/div/div/form/div[3]/button',
    "next_page_btn1":'//*[@id="maincontent"]/p/a[7]/b', 
    "next_page_btn2":'//*[@id="maincontent"]/p/a[5]/b',
}

def random_sleep(sec=0, min_sec=3, max_sec=5):
    """Assigns a random sleep time before proceeding to the next task.

    Parameters:
        sec {int} -- time of sleep in seconds
        min_sec {int} -- minimum possible time of sleep in seconds if the time of sleep \
            was not defined
        max_sec {int} -- maximum possible time of sleep in seconds if the time of sleep \
            was not defined

    Returns:
        {None} -- function returns no values. 
    """

    if sec > 0:
        time.sleep(sec)
    else:
        time.sleep(random.randint(min_sec, max_sec))

def account_login(driver, username, password):
    """Enters the username and password for the BoardGameGeek account.

    Parameters:
        driver {WebDriver} -- an instance of Selenium WebDriver with the loaded \
            BoardGameGeek Website.
        username {string} -- username for BoardGameGeek.
        password {string} -- password for BoardGameGeek.

    Returns:
        {WebDriver} -- instance of Selenium WebDriver with BoardGameGeek credentials \
            loaded into the website.
    """

    try:
        sign_in = driver.find_element(By.XPATH, static_vars["sign_in_btn"])
        sign_in.click()

        username_input = driver.find_element(By.XPATH, static_vars["username_box"])
        username_input.send_keys(username)

        password_input = driver.find_element(By.XPATH, static_vars["password_box"])
        password_input.send_keys(password)

        sign_in_btn = driver.find_element(By.XPATH, static_vars["sign_in"])
        sign_in_btn.click()

        random_sleep()

        print("BGG account successfully logged in.")

    except Exception as e:
        print("Error: Failed to log in BGG Account.")

    return driver

def scrape_page(driver):
    """Performs web scraping of the current web page of BoardGameGeek.

    Parameters:
        driver {WebDriver} -- an instance of Selenium WebDriver with the loaded \
            BoardGameGeek 'All Boardgames' Web page.

    Returns:
        {dataframe} -- contains the board game features such as rank, id, title, \
            and rating from BoardGameGeek 'All Boardgames' Web page.
    """

    board_games = []
    soup = BeautifulSoup(driver.page_source, 'lxml')
    table = soup.find('table', class_ = 'collection_table')

    for row in table.find_all('tr')[1:]:

        row_data = [data.text.strip() for data in row.find_all('td')]
        year_tag = row.find('span', class_='smallerfont dull')

        try:
            bgg_rank = row_data[0]
            bgg_id = row.find('a', class_ = 'primary').get('href').split('/')[2]
            title = row.find('a', class_ = 'primary').text
            year = year_tag.text[1:5] if year_tag is not None else "N/A"
            geek_rating = row_data[4]
            avg_rating = row_data[5]
            num_voters = row_data[6]

            board_games.append({
                "board_game_rank":bgg_rank,
                "bgg_id":bgg_id,
                "title":title,
                "year":year,
                "geek_rating":geek_rating,
                "avg_rating":avg_rating,
                "num_voters":num_voters,
            })

        except Exception as e:
            pass

    return pd.DataFrame(board_games)

def initial_load(driver):
    """Loads the first page of BoardGameGeek 'All Boardgames' and performs \
        account login.

    Parameters:
        driver {WebDriver} -- an instance of Selenium WebDriver with initialize Chrome \
            browser.add()
    Returns:
        driver {WebDriver} -- an instance of Selenium WebDriver with BoardGameGeek account \
            alread logged in and 'All Boardgames' Web page loaded.
    """

    driver.maximize_window()
    driver.get(static_vars["url"])

    account_login(driver, bgg_username, bgg_password)

    return driver

def main():
    "Main function python script."

    #selenium options to remove errors
    options = webdriver.ChromeOptions()
    options.add_experimental_option('excludeSwitches', ['enable-logging'])

    driver = webdriver.Chrome(service = Service(static_vars["local_driver_path"]), options=options)
    driver = initial_load(driver)

    df = pd.DataFrame()

    while True:

        df_tmp = scrape_page(driver)
        df = pd.concat([df, df_tmp], ignore_index=True)

        page_number = driver.current_url.split('/')[-1]
        print(f'Page {page_number} scraped. {df.shape[0]} board games available.')

        try:

            if page_number != '1':
                next_page_btn = static_vars["next_page_btn1"]
            else:
                next_page_btn = static_vars["next_page_btn2"]

            next_page = driver.find_element(By.XPATH, next_page_btn)
            next_page.click()

            random_sleep()

        except NoSuchElementException:
            print('Next page not found. Web scraping completed')
            break

    driver.close()

    df.to_csv('data/boardgamegeek.csv', index=False)

if __name__ == "__main__":
    main()
