"""Utility functions"""

from scripts import config
import wbgapi as wb
import pandas as pd


def add_flourish_geometries(df: pd.DataFrame, key_column_name: str = 'iso_code') -> pd.DataFrame:
    """
    Adds a geometry column to a dataframe based on iso3 code
        df: DataFrame to add a column
        key_column_name: name of column with iso3 codes to merge on, default = 'iso_code'
    """

    g = pd.read_json(f"{config.paths.glossaries}/flourish_geometries_world.json")
    g = (
        g.rename(columns={g.columns[0]: "flourish_geom", g.columns[1]: key_column_name})
            .iloc[1:]
            .drop_duplicates(subset=key_column_name, keep="first")
            .reset_index(drop=True)
    )

    return pd.merge(g, df, on=key_column_name, how="left")


def remove_unnamed_cols(df:pd.DataFrame) -> pd.DataFrame:
    """removes all columns with 'Unnamed' """

    return df.loc[:, ~df.columns.str.contains('Unnamed')]

def get_latest_values(df:pd.DataFrame, grouping_col:str, date_col:str) -> pd.DataFrame:
    """ """

    return (df.loc[df.groupby(grouping_col)[date_col].transform(max) == df[date_col]]
            .reset_index(drop=True))


# ===================================================
# World Bank API
# ===================================================

def _download_wb_data(code: str, database: int = 2) -> pd.DataFrame:
    """
    Queries indicator from World Bank API
        default database = 2 (World Development Indicators)
    """

    try:
        df = wb.data.DataFrame(
            series=code, db=database, numericTimeKeys=True, labels=True
        )
        return df

    except:
        raise Exception(f"Could not retieve {code} indicator from World Bank")

def _melt_wb_data(df: pd.DataFrame) -> pd.DataFrame:
    """Melts dataframe extracted from World Bank from wide to "long" format"""

    df = df.reset_index()
    df = df.melt(id_vars=df.columns[0:2])
    df.columns = ["iso_code", "country_name", "year", "value"]

    return df

def get_wb_indicator(code: str, database: int = 2) -> pd.DataFrame:
    """
    Steps to extract and clean an indicator from World Bank
        code: indicator code
        database: database number, default = 2 (World Development Indicators)
    """

    df = (_download_wb_data(code, database).pipe(_melt_wb_data))
    print(f"Successfully extracted {code} from World Bank")

    return df

