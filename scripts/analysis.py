"""Functions to reproduce food security analysis"""

from scripts import utils, config
import pandas as pd
import numpy as np
import country_converter as coco
from bs4 import BeautifulSoup
from typing import Optional
import requests

from scripts.ipc_data import IPC


def get_stunting_wb() -> pd.DataFrame:
    """
    Extract indicator Prevalence of stunting (SH.STA.STNT.ME.ZS) from World Bank
    https://data.worldbank.org/indicator/SH.STA.STNT.ME.ZS
    """
    return (
        utils.get_wb_indicator(code="SH.STA.STNT.ME.ZS", database=2)
        .dropna(subset="value")
        .reset_index(drop=True)
    )


# FAO Food index
def __read_fao_food_price_index(
    parser: Optional[str] = "html.parser",
    headers: Optional[dict] = {"User-Agent": "Mozilla/5.0"},
) -> pd.DataFrame:
    """Retrieve food price index from FAO"""

    full_url = "https://www.fao.org/worldfoodsituation/foodpricesindex/en/"
    base_url = "https://www.fao.org/"

    # scrape download link
    content = requests.get(full_url).content
    soup = BeautifulSoup(content, parser)
    href = soup.find_all(text="CSV")[0].parent.get("href")
    download_link = base_url + href

    # read csv
    df = pd.read_csv(
        download_link, skiprows=2, storage_options=headers, parse_dates=["Date"]
    )

    return df


def __clean_fao_food_price_index(df=pd.DataFrame) -> pd.DataFrame:
    """Clean food price dataframe"""

    df = (
        df.rename(columns={"Date": "date"})
        .pipe(utils.remove_unnamed_cols)
        .dropna(subset="date")
        .reset_index(drop=True)
        .rename(columns={"Oils": "Vegetable Oil"})
    )

    return df


def get_food_price_index(
    *,
    parser: Optional[str] = "html.parser",
    headers: Optional[dict] = {"User-Agent": "Mozilla/5.0"},
) -> pd.DataFrame:
    """
    extract food price index from FAO
        parser: type of parser to use, default = html.parser
        headers: set headers to read csv, default = 'User-Agent': 'Mozilla/5.0'
    """

    df = __read_fao_food_price_index(parser=parser, headers=headers)
    df = __clean_fao_food_price_index(df)

    return df


def get_ipc_table():
    ipc = IPC()

    df = ipc.get_ipc_ch_data(latest=True, only_valid=True)
    df.to_csv(f"{config.paths.raw_data}/IPC_table.csv", index=False)
    print("Successfully downloaded IPC table")


# IPC tools
def get_ipc():
    """
    return clean dataset from IPC: https://www.ipcinfo.org/ipcinfo-website/ipc-dashboard/en/
    data needs to be manually downloaded and put into raw_data folder as 'IPC_data.csv'
    """
    df = pd.read_csv(f"{config.paths.raw_data}/IPC_data.csv", skiprows=1)
    columns = {
        "Country": "country",
        "Phase 2": "phase_2",
        "Phase 3": "phase_3",
        "Phase 4": "phase_4",
        "Phase 5": "phase_5",
        "Phase 3+": "phase_3plus",
        "Period from": "period_start",
        "Period to": "period_end",
        "IPC/CH": "source",
    }
    df = (
        df.rename(columns=columns)
        .dropna(subset="source")
        .replace(
            {
                "United Republic of Tanzania": "Tanzania",
                "Tri-national Central America": "Central America",
                "Democratic Republic of the Congo": "DRC",
                "Central African Republic": "CAR",
            }
        )
    )
    for col in ["phase_2", "phase_3", "phase_4", "phase_5", "phase_3plus"]:
        df[col] = utils.clean_numeric_column(df[col])

    return df


# FAO undernourishment
def __clean_fao_undernourishment(df: pd.DataFrame) -> pd.DataFrame:
    """cleans undernourishment dataframe"""

    df.columns = df.columns.str.lower()
    df = (
        df.loc[:, ["area", "item", "year", "value"]]
        .replace({"China, Taiwan Province of": "Taiwan", "China, mainland": "China"})
        .assign(value_text=lambda d: d.value)
        .assign(
            value=lambda d: utils.clean_numeric_column(d.value.str.replace("<", ""))
        )
        .reset_index(drop=True)
    )

    return df


def get_fao_undernourishment() -> pd.DataFrame:
    """
    read FAO undernourishment data from raw_data folder 'fao_undernourishment_data
    Data needs to be manually downloaded from FAOStat food security and Nutrition
    """

    df = pd.read_csv(f"{config.paths.raw_data}/FAO_undernourishment_data.csv")
    df = __clean_fao_undernourishment(df)

    return df


# USDA tools
def __clean_usda_data(df: pd.DataFrame, year: str) -> pd.DataFrame:
    """Cleans USDA dataframe"""

    df = (
        df.rename(
            columns={
                "Unnamed: 0": "country",
                "Consumer expenditures3": "total_cons_exp",
                "Expenditure on food2": "food_exp",
            }
        )
        .loc[:, ["country", "total_cons_exp", "food_exp"]]
        .dropna(subset="country")
        .dropna(subset=["total_cons_exp", "food_exp"])
        .reset_index(drop=True)
        .assign(iso_code=lambda d: coco.convert(d.country))
        .assign(continent=lambda d: coco.convert(d.iso_code, to="continent"))
    )

    return df


def _calc_avg_food_exp(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates average food expenditure over 2018, 2019, and 2019 from USDA dataframe"""

    df = df.groupby(["country", "iso_code", "continent"], as_index=False).agg(
        {"food_exp": np.sum, "total_cons_exp": np.sum}
    )
    df["avg_share"] = (df.food_exp / df.total_cons_exp) * 100

    return df


def get_usda_food_exp() -> pd.DataFrame:
    """Pipeline to extract USDA data"""
    url = "https://www.ers.usda.gov/media/e2pbwgyg/2015-2020-food-spending_update-july-2021.xlsx"
    df = pd.DataFrame()

    years = [2020, 2019, 2018]
    for year in years:
        df_year = pd.read_excel(url, sheet_name=str(year), skiprows=2)
        df_year = __clean_usda_data(df_year, str(year))
        df = pd.concat([df, df_year], ignore_index=True)

    df = _calc_avg_food_exp(df)

    return df


# World Bank Commodity prices and Index
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


# Potash


def clean_fao_fertilizer(df: pd.DataFrame) -> pd.DataFrame:
    """Clean FAO fertilizer dataset"""

    df = (
        df.loc[
            df["Year"].isin([2019, 2018, 2017]),
            ["Area", "Element", "Item", "Year", "Value"],
        ]
        .groupby(["Area", "Element", "Item"])
        .agg("mean")["Value"]
        .reset_index()
        .pivot(index=["Area", "Item"], columns="Element", values="Value")
        .reset_index()
        .rename(
            columns={
                "Area": "country",
                "Item": "fertiliser",
                "Agricultural Use": "ag_use",
                "Export Quantity": "export_quantity",
                "Import Quantity": "import_quantity",
                "Export Value": "export_value",
                "Import Value": "import_value",
                "Production": "production",
            }
        )
        .replace({"China, Taiwan Province of": "Taiwan", "China, mainland": "China"})
    )

    # clean countries
    df["iso_code"] = coco.convert(df.country)
    df["continent"] = coco.convert(df.iso_code, to="continent")
    df.country = coco.convert(df.country, to="name_short")

    return df


def _calculations(df: pd.DataFrame) -> pd.DataFrame:
    """
    calculations for net imports and dependence of net imports in domestic agricultural use
    where there is no domestic use, dependence is set to 0
    """

    df["net_import_quantity"] = df.import_quantity - df.export_quantity

    # adjust net import  - change net export quantity to 0
    df["net_import_quantity_adj"] = df.net_import_quantity
    df.loc[df.net_import_quantity_adj < 0, "net_import_quantity_adj"] = 0

    # net import dependence in agriculture
    df["dependence"] = (df.net_import_quantity_adj / df.ag_use) * 100
    df.replace([np.inf, np.nan], 0, inplace=True)
    df.loc[
        df.dependence > 100, "dependence"
    ] = 100  # replace values with over 100% dependence with 100

    return df


def get_fao_fertilizer(
    fertilizer_list: Optional[list] = ["Nutrient potash K2O (total)"],
) -> pd.DataFrame:
    """Pipeline to read and clean FAO fertilizer data"""
    df = pd.read_csv(f"{config.paths.raw_data}/FAO_fertilizer.csv")

    df = clean_fao_fertilizer(df).pipe(_calculations)

    return df[df.fertiliser.isin(fertilizer_list)]
