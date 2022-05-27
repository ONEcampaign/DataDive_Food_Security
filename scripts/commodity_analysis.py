""" """

from scripts import config, utils
import pandas as pd
import country_converter as coco
from typing import Optional
import numpy as np


OILS = ['Palm oil', 'Soybean oil', 'Rapeseed oil', 'Sunflower oil', 'Coconut oil']
GRAINS = ['Barley', 'Maize', 'Sorghum', 'Rice, Thai 5% ', 'Wheat']

COMMODITY_URL = (
    "https://thedocs.worldbank.org/en/doc/5d903e848db1d1b83e0ec8f744e55570-"
    "0350012021/related/CMO-Historical-Data-Monthly.xlsx"
)

COMMODITY_DATA = pd.read_excel(COMMODITY_URL, sheet_name="Monthly Prices")
INDEX_DATA = pd.read_excel(COMMODITY_URL, sheet_name="Monthly Indices")

def get_commodity_prices(commodities: list) -> pd.DataFrame:
    """
    Gets the commodity data from the World Bank and returns a clean DataFrame
    """
    # read excel
    df = COMMODITY_DATA.copy()

    # cleaning
    df.columns = df.iloc[3]
    df = (
        df.rename(columns={np.nan: "period"})
            .iloc[6:]
            .reset_index(drop=True)
            .rename(columns={"Rice, Thai 5%": "Rice "})
            .rename(columns={"Wheat, US HRW": "Wheat"})
            .filter(["period"] + commodities)
            .replace("..", np.nan)
    )

    # change date format
    df["period"] = pd.to_datetime(df.period, format="%YM%m")

    return df

def get_indices(indices: Optional[list] = None) -> pd.DataFrame:
    """gets index data from World Bank and returns a clean dataframe"""

    df = INDEX_DATA.copy()

    df = df.iloc[9:].reset_index(drop=True).replace("..", np.nan)
    df.columns = [
        "period",
        "Energy",
        "Non-energy",
        "Agriculture",
        "Beverages",
        "Food",
        "Oils & Meals",
        "Grains",
        "Other Food",
        "Raw Materials",
        "Timber",
        "Other Raw Mat.",
        "Fertilizers",
        "Metals & Minerals",
        "Base Metals (ex. iron ore)",
        "Precious Metals",
    ]

    # filter indices
    if indices is not None:
        indices.insert(0, "period")  # add column name for period
        df = df.loc[:, indices]

    # change date format
    df["period"] = pd.to_datetime(df.period, format="%YM%m")

    return df



