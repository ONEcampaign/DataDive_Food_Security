"""Utility functions"""

from scripts import config
import wbgapi as wb
import pandas as pd
import weo
import country_converter as coco


def add_flourish_geometries(
    df: pd.DataFrame, key_column_name: str = "iso_code"
) -> pd.DataFrame:
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


def remove_unnamed_cols(df: pd.DataFrame) -> pd.DataFrame:
    """removes all columns with 'Unnamed'"""

    return df.loc[:, ~df.columns.str.contains("Unnamed")]


def clean_numeric_column(column: pd.Series) -> pd.Series:
    """removes commas and transforms pandas series to numeric"""

    column = column.str.replace(",", "")
    column = pd.to_numeric(column)

    return column


def get_latest_values(
    df: pd.DataFrame, grouping_col: str, date_col: str
) -> pd.DataFrame:
    """returns a dataframe with only latest values per group"""

    return df.loc[
        df.groupby(grouping_col)[date_col].transform(max) == df[date_col]
    ].reset_index(drop=True)


def keep_countries(df: pd.DataFrame, iso_col: str = "iso_code") -> pd.DataFrame:
    """returns a dataframe with only countries"""

    cc = coco.CountryConverter()
    return df[df[iso_col].isin(cc.data["ISO3"])].reset_index(drop=True)


def filter_countries(
    df: pd.DataFrame, by: str, values: list = ["Africa"], iso_col: str = "iso_code"
) -> pd.DataFrame:
    """
    returns a filtered dataframe
        by: filtering category -'continent', UNregion etc.
        values: list of values to keep
    """

    cc = coco.CountryConverter()
    if by not in cc.data.columns:
        raise ValueError(f"{by} is not valid")

    df[by] = coco.convert(df[iso_col], to=by)
    return df[df[by].isin(values)].drop(columns=by).reset_index(drop=True)


# ============================================================================
# Income levels
# ============================================================================


def get_income_levels() -> pd.DataFrame:
    """Downloads fresh version of income levels from WB"""
    url = "https://databank.worldbank.org/data/download/site-content/CLASS.xlsx"

    df = pd.read_excel(
        url,
        sheet_name="List of economies",
        usecols=["Code", "Income group"],
        na_values=None,
    )

    df = df.dropna(subset=["Income group"])

    return df


def add_income_levels(df: pd.DataFrame, iso_col: str = "iso_code") -> pd.DataFrame:
    """Add income levels to a dataframe"""

    income_levels = (
        get_income_levels().set_index("Code").loc[:, "Income group"].to_dict()
    )
    return df.assign(income_level=lambda d: d[iso_col].map(income_levels))


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

    df = _download_wb_data(code, database).pipe(_melt_wb_data)
    print(f"Successfully extracted {code} from World Bank")

    return df


# ==========================================
# IMF
# ==============================================

WEO_YEAR = 2022
WEO_RELEASE = 1


def _download_weo(year: int = WEO_YEAR, release: int = WEO_RELEASE) -> None:
    """Downloads WEO as a csv to raw data folder as "weo_month_year.csv"""

    try:
        weo.download(
            year=year,
            release=release,
            directory=config.paths.raw_data,
            filename=f"weo_{year}_{release}.csv",
        )
    except ConnectionError:
        raise ConnectionError("Could not download weo data")


def _clean_weo(df: pd.DataFrame) -> pd.DataFrame:
    """cleans and formats weo dataframe"""

    columns = {
        "ISO": "iso_code",
        "WEO Subject Code": "indicator",
        "Subject Descriptor": "indicator_name",
        "Units": "units",
        "Scale": "scale",
    }
    cols_to_drop = [
        "WEO Country Code",
        "Country",
        "Subject Notes",
        "Country/Series-specific Notes",
        "Estimates Start After",
    ]
    return (
        df.drop(cols_to_drop, axis=1)
        .rename(columns=columns)
        .melt(id_vars=columns.values(), var_name="year", value_name="value")
        .assign(
            value=lambda d: d.value.map(
                lambda x: str(x).replace(",", "").replace("-", "")
            )
        )
        .astype({"year": "int32"})
        .assign(value=lambda d: pd.to_numeric(d.value, errors="coerce"))
    )


def get_weo_indicator_latest(
    indicator: str, target_year: int = 2022, *, min_year: int = 2018
) -> pd.DataFrame:
    """
    Retrieves values for an indicator for a target year
    """

    df = weo.WEO(f"{config.paths.raw_data}/weo_{WEO_YEAR}_{WEO_RELEASE}.csv").df

    df = (
        df.pipe(_clean_weo)
        .dropna(subset=["value"])
        .loc[
            lambda d: (d.indicator == indicator)
            & (d.year >= min_year)
            & (d.year <= target_year),
            ["iso_code", "year", "value"],
        ]
        .reset_index(drop=True)
    )
    return df.loc[
        df.groupby(["iso_code"])["year"].transform(max) == df["year"],
        ["iso_code", "value"],
    ]


def get_gdp_latest(per_capita: bool = False, year: int = 2022) -> pd.DataFrame:
    """
    return latest gdp values
        set per_capita = True to return gdp per capita values
    """
    if per_capita:
        return get_weo_indicator_latest(target_year=year, indicator="NGDPDPC")
    else:
        return get_weo_indicator_latest(target_year=year, indicator="NGDPD").assign(
            value=lambda d: d.value * 1e9
        )


def add_gdp_latest(
    df: pd.DataFrame, iso_col: str = "iso_code", per_capita=False, year: int = 2022
) -> pd.DataFrame:
    """adds a column with latest gdp values to a dataframe"""

    if per_capita:
        new_col_name = "gdp_per_capita"
    else:
        new_col_name = "gdp"

    gdp_df = get_gdp_latest(year=year, per_capita=per_capita)
    gdp_dict = gdp_df.set_index("iso_code")["value"].to_dict()

    df[new_col_name] = df[iso_col].map(gdp_dict)

    return df


IPC_COUNTRIES: dict = {
    "LAC": "Tri-national Central America",
    "AF": "Afghanistan",
    "AL": "Albania",
    "DZ": "Algeria",
    "AS": "American Samoa",
    "AD": "Andorra",
    "AO": "Angola",
    "AI": "Anguilla",
    "AQ": "Antarctica",
    "AG": "Antigua and Barbuda",
    "AR": "Argentina",
    "AM": "Armenia",
    "AW": "Aruba",
    "AU": "Australia",
    "AT": "Austria",
    "AZ": "Azerbaijan",
    "BS": "Bahamas",
    "BH": "Bahrain",
    "BD": "Bangladesh",
    "BB": "Barbados",
    "BY": "Belarus",
    "BE": "Belgium",
    "BZ": "Belize",
    "BJ": "Benin",
    "BM": "Bermuda",
    "BT": "Bhutan",
    "BO": "Bolivia (Plurinational State of)",
    "BQ": "Bonaire, Sint Eustatius and Saba",
    "BA": "Bosnia and Herzegovina",
    "BW": "Botswana",
    "BV": "Bouvet Island",
    "BR": "Brazil",
    "IO": "British Indian Ocean Territory",
    "VG": "British Virgin Islands",
    "BN": "Brunei Darussalam",
    "BG": "Bulgaria",
    "BF": "Burkina Faso",
    "BI": "Burundi",
    "CV": "Cabo Verde",
    "KH": "Cambodia",
    "CM": "Cameroon",
    "CA": "Canada",
    "KY": "Cayman Islands",
    "CF": "Central African Republic",
    "TD": "Chad",
    "CL": "Chile",
    "CN": "China",
    "HK": "China, Hong Kong SAR",
    "MO": "China, Macao SAR",
    "CX": "Christmas Island",
    "CC": "Cocos (Keeling) Islands",
    "CO": "Colombia",
    "KM": "Comoros",
    "CG": "Congo",
    "CK": "Cook Islands",
    "CR": "Costa Rica",
    "HR": "Croatia",
    "CU": "Cuba",
    "CW": "Curaçao",
    "CY": "Cyprus",
    "CZ": "Czechia",
    "CI": "Côte d'Ivoire",
    "KP": "Democratic People's Republic of Korea",
    "CD": "Democratic Republic of the Congo",
    "DK": "Denmark",
    "DJ": "Djibouti",
    "DM": "Dominica",
    "DO": "Dominican Republic",
    "EC": "Ecuador",
    "EG": "Egypt",
    "SV": "El Salvador",
    "GQ": "Equatorial Guinea",
    "ER": "Eritrea",
    "EE": "Estonia",
    "SZ": "Eswatini",
    "ET": "Ethiopia",
    "N/": "European Union",
    "FK": "Falkland Islands (Malvinas)",
    "FO": "Faroe Islands ",
    "FJ": "Fiji",
    "FI": "Finland",
    "FR": "France",
    "GF": "French Guyana",
    "PF": "French Polynesia",
    "TF": "French Southern and Antarctic Territories",
    "GA": "Gabon",
    "GM": "Gambia",
    "GE": "Georgia",
    "DE": "Germany",
    "GH": "Ghana",
    "GI": "Gibraltar",
    "GR": "Greece",
    "GL": "Greenland",
    "GD": "Grenada",
    "GP": "Guadeloupe",
    "GU": "Guam",
    "GT": "Guatemala",
    "GG": "Guernsey",
    "GN": "Guinea",
    "GW": "Guinea-Bissau",
    "GY": "Guyana",
    "HT": "Haiti",
    "HM": "Heard and McDonald Islands",
    "VA": "Holy See",
    "HN": "Honduras",
    "HU": "Hungary",
    "IS": "Iceland",
    "IN": "India",
    "ID": "Indonesia",
    "IR": "Iran (Islamic Republic of)",
    "IQ": "Iraq",
    "IE": "Ireland",
    "IM": "Isle of Man",
    "IL": "Israel",
    "IT": "Italy",
    "JM": "Jamaica",
    "JP": "Japan",
    "JE": "Jersey",
    "JO": "Jordan",
    "KZ": "Kazakhstan",
    "KE": "Kenya",
    "KI": "Kiribati",
    "KW": "Kuwait",
    "KG": "Kyrgyzstan",
    "LA": "Lao People's Democratic Republic",
    "LV": "Latvia",
    "LB": "Lebanon",
    "LS": "Lesotho",
    "LR": "Liberia",
    "LY": "Libya",
    "LI": "Liechtenstein",
    "LT": "Lithuania",
    "LU": "Luxembourg",
    "MG": "Madagascar",
    "MW": "Malawi",
    "MY": "Malaysia",
    "MV": "Maldives",
    "ML": "Mali",
    "MT": "Malta",
    "MH": "Marshall Islands",
    "MQ": "Martinique",
    "MR": "Mauritania",
    "MU": "Mauritius",
    "YT": "Mayotte",
    "MX": "Mexico",
    "FM": "Micronesia (Federated States of)",
    "MC": "Monaco",
    "MN": "Mongolia",
    "ME": "Montenegro",
    "MS": "Montserrat",
    "MA": "Morocco",
    "MZ": "Mozambique",
    "MM": "Myanmar",
    "NA": "Namibia",
    "NR": "Nauru",
    "NP": "Nepal",
    "NL": "Netherlands",
    "NC": "New Caledonia",
    "NZ": "New Zealand",
    "NI": "Nicaragua",
    "NE": "Niger",
    "NG": "Nigeria",
    "NU": "Niue",
    "NF": "Norfolk Island",
    "MP": "Northern Mariana Islands",
    "NO": "Norway",
    "OM": "Oman",
    "PK": "Pakistan",
    "PW": "Palau",
    "PS": "Palestine",
    "PA": "Panama",
    "PG": "Papua New Guinea",
    "PY": "Paraguay",
    "PE": "Peru",
    "PH": "Philippines",
    "PN": "Pitcairn Islands",
    "PL": "Poland",
    "PT": "Portugal",
    "PR": "Puerto Rico",
    "QA": "Qatar",
    "KR": "Republic of Korea",
    "MD": "Republic of Moldova",
    "RO": "Romania",
    "RU": "Russian Federation",
    "RW": "Rwanda",
    "RE": "Réunion",
    "BL": "Saint Barthélemy",
    "SH": "Saint Helena, Ascension and Tristan da Cunha",
    "KN": "Saint Kitts and Nevis",
    "LC": "Saint Lucia",
    "PM": "Saint Pierre and Miquelon",
    "VC": "Saint Vincent and the Grenadines",
    "MF": "Saint-Martin (French part)",
    "WS": "Samoa",
    "SM": "San Marino",
    "ST": "Sao Tome and Principe",
    "SA": "Saudi Arabia",
    "SN": "Senegal",
    "RS": "Serbia",
    "SC": "Seychelles",
    "SL": "Sierra Leone",
    "SG": "Singapore",
    "SK": "Slovakia",
    "SI": "Slovenia",
    "SB": "Solomon Islands",
    "SO": "Somalia",
    "ZA": "South Africa",
    "GS": "South Georgia and the South Sandwich Islands",
    "SS": "South Sudan",
    "ES": "Spain",
    "LK": "Sri Lanka",
    "SD": "Sudan",
    "SR": "Suriname",
    "SJ": "Svalbard and Jan Mayen Islands",
    "SE": "Sweden",
    "CH": "Switzerland",
    "SY": "Syrian Arab Republic",
    "TW": "Taiwan Province of China",
    "TJ": "Tajikistan",
    "TH": "Thailand",
    "MK": "The former Yugoslav Republic of Macedonia",
    "TL": "Timor-Leste",
    "TG": "Togo",
    "TK": "Tokelau ",
    "TO": "Tonga",
    "TT": "Trinidad and Tobago",
    "TN": "Tunisia",
    "TR": "Turkey",
    "TM": "Turkmenistan",
    "TC": "Turks and Caicos Islands",
    "TV": "Tuvalu",
    "UG": "Uganda",
    "UA": "Ukraine",
    "AE": "United Arab Emirates",
    "GB": "United Kingdom",
    "TZ": "United Republic of Tanzania",
    "UM": "United States Minor Outlying Islands",
    "VI": "United States Virgin Islands",
    "US": "United States of America",
    "UY": "Uruguay",
    "UZ": "Uzbekistan",
    "VU": "Vanuatu",
    "VE": "Venezuela (Bolivarian Republic of)",
    "VN": "Viet Nam",
    "WF": "Wallis and Futuna Islands",
    "EH": "Western Sahara",
    "YE": "Yemen",
    "ZM": "Zambia",
    "ZW": "Zimbabwe",
    "AX": "Åland Islands",
}
