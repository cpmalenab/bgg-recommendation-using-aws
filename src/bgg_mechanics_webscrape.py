import requests

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
import pandas as pd

from bgg_webscrape import random_sleep, initial_load

static_vars = {
    "local_driver_path": r"../chromedriver.exe",
    "url": r'https://boardgamegeek.com/browse/boardgamemechanic',
}

def main():
    "Main function python script."

    mechanic_dict = {}
    description_dict = {}

    driver = webdriver.Chrome(service = Service(static_vars["local_driver_path"]))
    driver = initial_load(driver)

    soup = BeautifulSoup(driver.page_source, 'lxml')
    table = soup.find('table', class_ = 'forum_table')

    for row in table.find_all('tr'):
        for column in row.find_all('a'):
            mechanic = column.text
            link = 'https://boardgamegeek.com' + column['href']

            mechanic_dict[mechanic] = link

    for mechanic, url in mechanic_dict.items():
        page = requests.get(url)
        soup = BeautifulSoup(page.text, 'lxml')
        description_dict[mechanic] = soup.find('meta', attrs={'name': 'description'})['content']
        random_sleep(1,2)

    mechanic_df = pd.DataFrame(data=[mechanic_dict]).melt(var_name='mechanic', value_name='link')
    description_df = pd.DataFrame(data=[description_dict])\
        .melt(var_name='mechanic', value_name='description')

    mechanics_description = pd.merge(mechanic_df, description_df, on='mechanic', how='left')
    mechanics_description = mechanics_description.drop('link', axis=1)

    mechanics_description.to_json('data/mechanics description.json', orient='records', lines=True)

if __name__ == "__main__":
    main()
