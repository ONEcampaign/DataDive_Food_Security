"""Functions to reproduce food security analysis"""

from scripts import utils, config
import pandas as pd
import country_converter as coco


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
          )

    return df

def get_dhs_indicator(indicator:str, category_name:str) -> pd.DataFrame:
    """ """

    df = pd.read_excel(f'{config.paths.raw_data}/DHS_data.xlsx')
    df = __clean_dhs(df)

    return df.loc[(df['indicator'] == indicator)&(df['category_name'] == category_name)]







if __name__ == '__main__':
    stunting = get_dhs_indicator('Children stunted', 'Wealth quintile')

