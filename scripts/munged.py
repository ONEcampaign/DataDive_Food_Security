import numpy as np
import pandas as pd
import country_converter as coco


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
