#!/usr/bin/env python
# coding: utf-8

import os
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import json


def create_df(game_list, region):
    for idx, game in enumerate(game_list):
        if idx == 0:
            main_df = get_data(game, region)
            continue
        # after index 0, append into temp_df
        temp_df = get_data(game, region)
        # append main_df with temp_df
        main_df = pd.concat([main_df, temp_df])

    # reset main_df index
    main_df = main_df.reset_index()

    return main_df


def get_data(game_id, region):
    api_key = os.getenv('PLAT_PRICE_KEY')
    response = requests.get(f"https://platprices.com/api.php?key={api_key}&ppid={game_id}&region={region}")
    json_data = json.loads(response.text)
    #     print(f"json text is type {type(json_data)}")
    #     print(json_data.keys())
    df = pd.DataFrame.from_dict(json_data, orient='index')
    transpose_df = df.transpose()
    return transpose_df


def extract_multi(url, region, last_page):
    page_list = list(range(1, last_page + 1))
    ids = []
    for page in page_list:
        print(f"scraping page {page}")
        response = requests.get(f"{url}?sort=alpha&page={page}&userregion={region}")
        # get soup
        soup = BeautifulSoup(response.content, 'html.parser')
        # Find the div tag with class "game-container-lo"
        game_container_lo = soup.find_all('div', {'class': 'game-container-lo'})
        # regex pattern starts with / followed by string of digits and ends with -
        pattern = r'/(\d+)-'
        # use list comprehension and pattern to extract the ids I want
        temp = [re.search(pattern, game.find('a')['href']).group(1) for game in game_container_lo]
        print(f"found {len(temp)} items")
        ids.append(temp)

    # flatten the lists of lists into a single list
    flatten_ids = [item for sublist in ids for item in sublist]

    print(f"total ids scraped are {len(flatten_ids)} items")

    return flatten_ids


def extract_single(soup):
    # Find the div tag with class "game-container-lo"
    game_container_lo = soup.find_all('div', {'class': 'game-container-lo'})

    # regex pattern starts with / followed by string of digits and ends with -
    pattern = r'/(\d+)-'

    # use list comprehension and pattern to extract the ids I want
    ids = [re.search(pattern, game.find('a')['href']).group(1) for game in game_container_lo]

    return ids


def get_last_page(resultset):
    page_tags = resultset.find_all('a')
    last_tag = page_tags[-1]
    last_page = last_tag.text
    return last_page


def get_ids(url, region):
    """ This function iterates through the plat_price pages for PS Essential/Extra and returns a list of gameids.
    This execution only scrapes Canadian games for now..."""
    response = requests.get(f"{url}?userregion={region}")

    # get soup
    soup = BeautifulSoup(response.content, 'html.parser')

    # checks if a div class='center-xs pagin' exists, if so return the largest page number"
    pagination_yes = soup.find('div', {'class': 'center-xs pagin'})

    # Check if the div tag exists
    if pagination_yes:

        # return the largest page number
        last_page = int(get_last_page(pagination_yes))
        ids = extract_multi(url, region, last_page)
        return ids

    else:
        # extract_single_page
        ids = extract_single(soup)
        return ids


if __name__ == '__main__':
    api_key = os.getenv('PLAT_PRICE_KEY')
    essential = get_ids("https://platprices.com/psplus/essential/", 'CA')
    essential_df = create_df(essential, 'CA')
    essential_df.to_csv('essential_df.csv')

    extra = get_ids("https://platprices.com/psplus/extra/", 'CA')
    extra_df = create_df(extra, 'CA')
    extra_df.to_csv('extra_df.csv')


