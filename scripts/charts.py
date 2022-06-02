"""Function to create flourish charts"""

import pandas as pd
from scripts import utils, config
from scripts.analysis import get_stunting_wb, get_fao_undernourishment, get_usda_food_exp, get_ipc, get_food_price_index, get_commodity_prices, get_indices, get_fao_fertilizer
from typing import Optional
import country_converter as coco


def fao_fpi_main(start_date:str = '2000-01-01') -> None:
    """Creates csv for FAO Food Price Index Chart starting in 2000-01-01"""

    df = get_food_price_index()
    df.assign(date_popup = lambda d: d.date).loc[df.date>=start_date].to_csv(f'{config.paths.output}/fao_fpi_main.csv', index=False)


def undernourishment_world() -> None:
    """
    Create undernourishment chart for world, from FAO food security data
    (Chart not used in page)
    """

    df = get_fao_undernourishment()

    pct_df = df.loc[df.item == 'Prevalence of undernourishment (percent) (annual value)', ['area', 'year', 'value', 'value_text']]
    mil_df = df.loc[df.item == 'Number of people undernourished (million) (annual value)', ['area', 'year', 'value', 'value_text']]
    final =  pd.merge(pct_df, mil_df, on=['area', 'year'], how='inner', suffixes=('_pct', '_mil'))
    final[final.area == 'World'].to_csv(f'{config.paths.output}/undernourishment_world.csv', index=False)


def stunting_map() -> None:
    """
    creates stunting map - by country for latest available data point
    (not used in main page)
    """

    df = get_stunting_wb()

    (utils.get_latest_values(df, 'iso_code', 'year')
     .pipe(utils.add_flourish_geometries)
     .to_csv(f'{config.paths.output}/stunting_map.csv', index=False))


def stunting_top_countries_bar() -> None:
    """Creates a chart for 30 countries with highest stunting values + SSA"""

    df = get_stunting_wb()

    ssf = (df.loc[df.iso_code == 'SSF']
           .pipe(utils.get_latest_values, 'iso_code', 'year')) #get latest value for Sub-Saharan Africa

    df = (utils
     .get_latest_values(df, 'iso_code', 'year')
     .sort_values(by='value', ascending=False)
     .pipe(utils.keep_countries)
     .reset_index(drop=True)
     .loc[0:30])

    df = df.append(ssf)
    df.to_csv(f'{config.paths.output}/stunting_top_countries_bar.csv', index=False)


def ipc_charts() -> None:
    """Create charts for all IPC phases"""

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


def food_exp_share_chart() -> None:
    """Creates scatter plot of share of food expenditure vs gdp per capita"""

    df = get_usda_food_exp()
    df = (utils.add_gdp_latest(df, iso_col='iso_code', per_capita = True).pipe(utils.add_income_levels).assign(income_level_agg = lambda d: d.income_level))

    df.loc[df.income_level_agg.isin(['Low income', 'Lower middle income']), 'income_level_agg'] = 'Low/lower middle income'
    df.loc[df.income_level_agg.isin(['High income', 'Upper middle income']), 'income_level_agg'] = 'High/higher middle income'

    df.to_csv(f'{config.paths.output}/food_share_chart.csv', index=False)



def fao_fpi_scrolly(start_date:str = '2010-01-01') -> None:
    """Creates csv for FAO Food Price Index Chart starting in 2014-01-01 to embed in the scolly story"""

    df = get_food_price_index()
    df.assign(date_popup = lambda d: d.date).loc[df.date>=start_date].to_csv(f'{config.paths.output}/fao_fpi_scrolly.csv', index=False)


def commodity_chart(commodities: Optional[list] = ['Palm oil', 'Sunflower oil', 'Maize', 'Wheat']) -> None:
    """Creates chart for WB commodity prices"""

    df = get_commodity_prices(commodities)
    (df.assign(date_popup = lambda d: d.period)
     .loc[df.period>='2010-01-01']
     .to_csv(f'{config.paths.output}/food_commodity_chart.csv', index=False))

def index_chart(indexes:Optional[list] = ['Agriculture', 'Food', 'Oils & Meals', 'Grains', 'Other Food', 'Fertilizers']) -> None:
    """
    Creates chart for WB index
    (Not Used in main page)
    """

    df = get_indices(indexes)
    df.loc[df.period>='2010-01-01'].to_csv(f'{config.paths.output}/index_chart.csv', index=False)


def ifpri_restriction_chart() -> None:
    """Create trade restriction chart from IFPRI"""

    df = pd.read_csv(f'{config.paths.raw_data}/restrictions_data.csv')
    (df.rename(columns={"Ukraine Crisis [2022]": "Russia's war in Ukraine [2022]" })
     .to_csv(f'{config.paths.output}/ifpri_restriction.csv', index=False))



def potash_dependence_chart() -> None:
    """Create potash dependence map"""

    df = get_fao_fertilizer()

    (df.pipe(utils.add_flourish_geometries)
     .loc[:, ['flourish_geom', 'iso_code', 'country', 'dependence']]
    .assign(country = lambda d: coco.convert(d.iso_code, to='name_short'))
    .to_csv(f'{config.paths.output}/potash_map.csv', index=False))


def update_charts() -> None:
    """pipileine to update charts for the page"""

    fao_fpi_main()
    ipc_charts()
    stunting_top_countries_bar()
    food_exp_share_chart()
    fao_fpi_scrolly()
    commodity_chart()
    ifpri_restriction_chart()
    potash_dependence_chart()


if __name__ == '__main__':
    update_charts()
    print('Successfully updated charts')

