"""Functions to reproduce food security analysis"""

from scripts import utils, config
import pandas as pd
import numpy as np
import country_converter as coco
from bs4 import BeautifulSoup
from typing import Optional
import requests


# ======================================================================================================================
# Extraction tools
# ======================================================================================================================

def get_stunting_wb() -> pd.DataFrame:
    """
    Extract indicator Prevalence of stunting (SH.STA.STNT.ME.ZS) from World Bank
    https://data.worldbank.org/indicator/SH.STA.STNT.ME.ZS
    """
    return (utils.get_wb_indicator(code = 'SH.STA.STNT.ME.ZS', database=2)
            .dropna(subset='value')
            .reset_index(drop=True))


def __clean_dhs(df:pd.DataFrame) -> pd.DataFrame:
    """Cleans raw data from DHS"""

    columns = {'Country Name':'country',
               'Survey Year':'year',
               'Survey Name':'survey_name',
               'Indicator':'indicator',
               'Characteristic Category': 'category_name',
               'Characteristic Label': 'category_label',
               'Value':'value'}

    df = (df
          .rename(columns = columns)
          .dropna(subset = 'indicator')
          .filter(columns.values(), axis=1)
          .assign(iso_code = lambda d: coco.convert(d.country))
          .reset_index(drop=True)
          )

    return df

def get_dhs_indicator(indicator:str, category_name:str) -> pd.DataFrame:
    """ """

    df = pd.read_excel(f'{config.paths.raw_data}/DHS_data.xlsx')
    df = __clean_dhs(df)

    return df.loc[(df['indicator'] == indicator)&(df['category_name'] == category_name)]


def __read_fao_food_price_index(parser:Optional[str] = 'html.parser',
                              headers:Optional[dict] = {'User-Agent': 'Mozilla/5.0'}) -> pd.DataFrame:
    """Retrieve food price index from FAO"""

    full_url = 'https://www.fao.org/worldfoodsituation/foodpricesindex/en/'
    base_url = 'https://www.fao.org/'

    # scrape download link
    content = requests.get(full_url).content
    soup = BeautifulSoup(content, parser)
    href = soup.find_all(text = 'CSV')[0].parent.get('href')
    download_link = base_url + href

    # read csv
    df = pd.read_csv(download_link, skiprows=2, storage_options=headers, parse_dates=['Date'])

    return df

def __clean_fao_food_price_index(df = pd.DataFrame) -> pd.DataFrame:
    """Clean food price dataframe"""

    df = (df.rename(columns = {'Date': 'date'})
          .pipe(utils.remove_unnamed_cols)
          .dropna(subset = 'date')
          .reset_index(drop=True)
          )

    return df

def get_food_price_index(*,
                         parser:Optional[str] = 'html.parser',
                         headers:Optional[dict] = {'User-Agent': 'Mozilla/5.0'}) -> pd.DataFrame:
    """
    extract food price index from FAO
        parser: type of parser to use, default = html.parser
        headers: set headers to read csv, default = 'User-Agent': 'Mozilla/5.0'
    """

    df = __read_fao_food_price_index(parser = parser, headers = headers)
    df = __clean_fao_food_price_index(df)

    return df


def get_ipc():
    """
    return clean dataset from IPC: https://www.ipcinfo.org/ipcinfo-website/ipc-dashboard/en/
    data needs to be manually downloaded and put into raw_data folder as 'IPC_data.csv'
    """
    df = pd.read_csv(f'{config.paths.raw_data}/IPC_data.csv', skiprows=1)
    columns = {'Country':'country', 'Phase 2':'phase_2', 'Phase 3':'phase_3',
               'Phase 4':'phase_4', 'Phase 5':'phase_5', 'Phase 3+':'phase_3plus',
               'Period from':'period_start', 'Period to':'period_end', 'IPC/CH':'source'}
    df = df.rename(columns=columns).dropna(subset='source')

    return df




if __name__ == '__main__':
    pass


