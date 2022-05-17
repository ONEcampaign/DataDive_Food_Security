"""Functions to reproduce food security analysis"""

from scripts import utils, config
import pandas as pd


def get_stunting_wb() -> pd.DataFrame:
    """
    Extract indicator Prevalence of stunting (SH.STA.STNT.ME.ZS) from World Bank
    https://data.worldbank.org/indicator/SH.STA.STNT.ME.ZS
    """
    return utils.get_wb_indicator(code = 'SH.STA.STNT.ME.ZS', database=2)

