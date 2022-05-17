"""Utility functions"""

from scripts import config
import wbgapi as wb
import pandas as pd


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

