# DataDive_Food_Security
This repository contains the scripts and data necessary to reproduce the 
<a href = "https://www.one.org/africa/explore-food-security/">Food Security Data Dive page</a>.

Maintainers:
- Luca Picci [https://github.com/lpicci96/lpicci96]
- Jorge Rivera [https://github.com/jm-rivera]

### Sources
- World Bank - World Development Indicators
- Integrated Food Security Phase Classification (IPC)
- Food and Agriculture Organization (FAO)
- United States Department of Agriculture (USDA) Economic Research Service.
- International Food Policy Research Institute (IFPRI)
- World Food Programme

### Repository Structure and Information

In order to reproduce this analysis, Python (>= 3.10) is needed. Other packages are listed in `requirements.txt`.
The repository includes the following sub-folders:

`output`: contains clean and formatted csv filed that are used to create the visualizations.
`raw_data`: contains raw data used for the analysis. Manually downloaded files are added to this folder.
`glossaries`: contains metadata and other useful lookup files.
`scripts`: scripts for creating the analysis. `analysis.py` contains functions to extract and clean data from various
sources. `charts.py` contains functions to produce the visualizations that appear on the page. `utils.py` contains 
utility functions and `config.py` manages file paths to different folders.

#### Manually downloaded data

Some data needs to be manually downloaded and moved into the `raw_data` folder:

1. Download IPC country level data from the [IPC dashboard](https://www.ipcinfo.org/ipcinfo-website/ipc-dashboard/en/), 
and place the file in `raw_data` as `IPC_data.csv`. 
2. Download FOA undernourishment fata from [FAOStat](https://www.fao.org/faostat/en/#data) - food security and
nutrition indicators. Select `prevalence of undernourishment (%)` for all years and countries. Place the csv file 
in `raw data` as `FAO_undernourishment_data.csv`.
3. Download IFPRI data from the [IFPRI blogpost](https://www.ifpri.org/blog/bad-worse-how-export-restrictions-exacerbate-global-food-security)
under `Figure 1` and place the file in `raw_data` as `restrictions_data.csv`.
4. Download FAO fertilizer data from [FAOStat](https://www.fao.org/faostat/en/#data) -
Fertilizer by nutrient dataset. Select `Nutrient potash k20 (total)` for all elements, all countries and years. Place the
file in `raw_data` as `FAO_fertilizer.csv`.


