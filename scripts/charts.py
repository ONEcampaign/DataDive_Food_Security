""" """

import pandas as pd
import numpy as np
from scripts import utils, config
from scripts.analysis import get_stunting_wb, get_fao_undernourishment, get_usda_food_exp, get_ipc, get_food_price_index
import country_converter as coco


def fao_fpi_main(start_date:str = '2000-01-01') -> None:
    """Creates csv for FAO Food Price Index Chart starting in 2000-01-01"""

    df = get_food_price_index()
    df.assign(date_popup = lambda d: d.date).loc[df.date>=start_date].to_csv(f'{config.paths.output}/fao_fpi_main.csv', index=False)





# ================================================================================
# undernourishment
# ================================================================================

def _undernourishment_world(df:pd.DataFrame) -> None:
    """Create undernourishment chart from FAO food security data"""


    pct_df = df.loc[df.item == 'Prevalence of undernourishment (percent) (annual value)', ['area', 'year', 'value', 'value_text']]
    mil_df = df.loc[df.item == 'Number of people undernourished (million) (annual value)', ['area', 'year', 'value', 'value_text']]

    final =  pd.merge(pct_df, mil_df, on=['area', 'year'], how='inner', suffixes=('_pct', '_mil'))
    final[final.area == 'World'].to_csv(f'{config.paths.output}/undernourishment_world.csv', index=False)

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
     #.pipe(utils.filter_countries, 'continent', ['Africa'])
     .sort_values(by='value', ascending=False)
     .pipe(utils.keep_countries)
     .reset_index(drop=True)
     .loc[0:30])

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
    df = (utils.add_gdp_latest(df, iso_col='iso_code', per_capita = True).pipe(utils.add_income_levels))

    df.loc[df.income_level.isin(['Low income', 'Lower middle income']), 'income_level'] = 'Low/lower middle income'
    df.loc[df.income_level.isin(['High income', 'Upper middle income']), 'income_level'] = 'High/higher middle income'

    df.to_csv(f'{config.paths.output}/food_share_chart.csv', index=False)



# ===========================================================
# IPC charts
# ===============================================================


def ipc_charts() -> None:
    """ """

    phases = {'Phase 2':'phase_2', 'Phase 3':'phase_3',
              'Phase 4':'phase_4', 'Phase 5':'phase_5', 'Phase 3+':'phase_3plus'}
    df = get_ipc()

    for phase in phases.values():
        df_phase = df.copy(deep=True)
        (df_phase
        .sort_values(by = phase, ascending=False)
        .reset_index(drop=True)
        .loc[0:15, ['country', phase, 'period_start', 'period_end', 'source']]
        .dropna(subset = phase)
        .to_csv(f'{config.paths.output}/ipc_{phase}.csv', index=False))








