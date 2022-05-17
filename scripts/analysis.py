"""Functions to reproduce food security analysis"""

from scripts import utils, config
import pandas as pd
import country_converter as coco
from bs4 import BeautifulSoup
from typing import Optional
import requests


def get_stunting_wb() -> pd.DataFrame:
    """
    Extract indicator Prevalence of stunting (SH.STA.STNT.ME.ZS) from World Bank
    https://data.worldbank.org/indicator/SH.STA.STNT.ME.ZS
    """
    return utils.get_wb_indicator(code = 'SH.STA.STNT.ME.ZS', database=2)


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













if __name__ == '__main__':
    stunting = get_dhs_indicator('Children stunted', 'Wealth quintile')

