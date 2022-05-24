""" """

import pandas as pd
import numpy as np
from scripts import utils, config
from scripts.analysis import get_stunting_wb, get_fao_undernourishment, get_usda_food_exp
import country_converter as coco


# ================================================================================
# undernourishment
# ================================================================================

def _undernourishment_world(df:pd.DataFrame) -> None:
    """ """


    pct_df = df.loc[df.item == 'Prevalence of undernourishment (percent) (annual value)', ['area', 'year', 'value', 'value_text']]
    mil_df = df.loc[df.item == 'Number of people undernourished (million) (annual value)', ['area', 'year', 'value', 'value_text']]

    final =  pd.merge(pct_df, mil_df, on=['area', 'year'], how='inner', suffixes=('_pct', '_mil'))
    final[final.area == 'World'].to_csv(f'{config.paths.output}/undernourishment_world.csv', index=False)


def _undernourishment_region(df:pd.DataFrame) -> None:
    """ """

    regions = ['Africa', 'Northern America', 'Europe',  'Central America', 'Caribbean', 'South America', 'Asia', 'Oceania']

    pct_df = df.loc[df.item == 'Prevalence of undernourishment (percent) (annual value)', ['area', 'year', 'value', 'value_text']]
    mil_df = df.loc[df.item == 'Number of people undernourished (million) (annual value)', ['area', 'year', 'value', 'value_text']]

    final =  pd.merge(pct_df, mil_df, on=['area', 'year'], how='inner', suffixes=('_pct', '_mil'))
    final = final[final.area.isin(regions)]

    return final




   # final.to_csv(f'{config.paths.output}/undernourishment_region.csv', index=False)


def undernourishment() -> None:
    """ """

    fao_df = get_fao_undernourishment()

    _undernourishment_world(fao_df)

# ================================================================================
# stunting
# ================================================================================

def _stunting_map(df:pd.DataFrame) -> None:
    """create stunting map - by country for latest available data point"""

    (utils.get_latest_values(df, 'iso_code', 'year')
     .pipe(utils.add_flourish_geometries)
     .to_csv(f'{config.paths.output}/stunting_map.csv', index=False))

def _stunting_top_countries_bar(df:pd.DataFrame) -> None:
    """ """

    ssf = (df.loc[df.iso_code == 'SSF']
           .pipe(utils.get_latest_values, 'iso_code', 'year')) #get latest value for Sub-Saharan Africa

    df = (utils
     .get_latest_values(df, 'iso_code', 'year')
     .pipe(utils.filter_countries, 'continent', ['Africa'])
     .sort_values(by='value', ascending=False)
     .pipe(utils.keep_countries)
     .reset_index(drop=True)
     .loc[0:20])

    df = df.append(ssf)
    df.to_csv(f'{config.paths.output}/stunting_top_countries_bar.csv', index=False)


def _stunting_vs_gdppc(df:pd.DataFrame) -> None:
    """ """

    df =   (utils.keep_countries(df)
            .pipe(utils.get_latest_values, 'iso_code', 'year')
            .pipe(utils.add_gdp_latest, per_capita=True)
            .dropna(subset='gdp_per_capita').assign(continent = lambda d: coco.convert(d.iso_code, to='continent')))
    df.loc[df.continent != 'Africa', 'continent'] = np.nan
    df.to_csv(f'{config.paths.output}/stunting_vs_gdppc.csv', index=False)


def stunting_charts() -> None:
    """Update stunting charts"""

    stunting = get_stunting_wb() # get stunting data

    _stunting_map(stunting) #update stunting map
    _stunting_top_countries_bar(stunting)
    _stunting_vs_gdppc(stunting)


# ========================================================
# food expenditure share
# ========================================================

def food_exp_share_chart() -> None:
    """ """

    df = get_usda_food_exp()
    df = utils.add_gdp_latest(df, iso_col='iso_code', per_capita = True)

    return df



