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

def __clean_ipc(df:pd.DataFrame) -> pd.DataFrame:
    """Cleans data from IPC"""

    columns = {'Country':'country',
               'Area':'area',
               'Analysis Name':'analysis_name',
               'Date of Analysis':'date',
               '% of total county Pop':'pct_of_population_analyzed',
               'Area Phase':'area_phase',
               'Analysis Period':'analysis_period',

               #numbers
               '#':'area_population_current',
               '#.1':'number_phase1_current',
               '#.2':'number_phase2_current',
               '#.3':'number_phase3_current',
               '#.4':'number_phase4_current',
               '#.5':'number_phase5_current',
               '#.6':'number_phase3plus_current',
               '#.7':'area_population_proj',
               '#.8':'number_phase1_proj',
               '#.9':'number_phase2_proj',
               '#.10':'number_phase3_proj',
               '#.11':'number_phase4_proj',
               '#.12':'number_phase5_proj',
               '#.13':'number_phase3plus_proj',

               #percentages
               '%': 'pct_phase1_current',
               '%.1':'pct_phase2_current',
               '%.2':'pct_phase3_current',
               '%.3':'pct_phase4_current',
               '%.4':'pct_phase5_current',
               '%.5':'pct_phase3plus_current',
               '%.6':'pct_phase1_proj',
               '%.7':'pct_phase2_proj',
               '%.8':'pct_phase3_proj',
               '%.9':'pct_phase4_proj',
               '%.10':'pct_phase5_proj',
               '%.11':'pct_phase3plus_proj'
               }

    df = (df.rename(columns = columns)
          .filter(columns.values(), axis=1)
          .dropna(subset='date')
          .assign(date = lambda d: pd.to_datetime(d.date)))

    df = (df.loc[df.country.str.contains('Afar/Amh/Tigray') == False, :] #remove subset of Ethiopia for specific regions
          .assign(country = lambda d: d.country.str.split(':', expand=True)[0]
                  .fillna(np.nan))
          .reset_index(drop=True))

    return df

def get_ipc_latest_country() -> pd.DataFrame:
    """Extract the latest data from IPC at country level"""

    try:
        url = 'https://map.ipcinfo.org/api/public/population-tracking-tool/data/2017,2022/?export=true&condition=A'
        df = pd.read_excel(url, skiprows=11, usecols=[i for i in range(39)])
    except ConnectionError:
        raise ConnectionError('Could not read data from IPC')

    df = __clean_ipc(df)
    df = (df[df.area.isna()]
          .assign(iso_code = lambda d: coco.convert(d.country)))
    df = (df.loc[(df.iso_code != 'not found')&(~df.country.isin(['Kinshasa', 'Djibouti Ville', 'Somali', 'Gaza']))])
    df = df[df.groupby(['country'])['date'].transform(max) == df['date']] # get latest value
    df = (df.drop_duplicates(subset = ['country', 'date', 'number_phase1_current', 'number_phase2_current',
                                     'number_phase3_current', 'number_phase4_current', 'number_phase5_current',
                                     'number_phase3plus_current'])
          .reset_index(drop=True))

    return df


if __name__ == '__main__':
    #stunting_wealth = get_dhs_indicator('Children stunted', 'Wealth quintile')
    #ipc = get_ipc_latest_country()

    stunting = get_stunting_wb()
    stunting = utils.get_latest_values(stunting, 'iso_code', 'year')
    stunting = utils.add_flourish_geometries(stunting)

