# pmp-mandatory-use

custom mandatory use compliance module for prescription monitoring programs

## setup

this project uses [uv](https://github.com/astral-sh/uv?tab=readme-ov-file)  
after installing `uv` on your system using the link above, use `uv sync` to install all dependencies

## data

see the [data readme](data/README.md) for more information on the required input data

<details>
    <summary>Tableau API</summary>

to make use of `tableauserverclient` to have the script pull the data instead of downloading manually:

1. you will need a `secrets.toml` file in the following format in the root folder of this repo:
   ```
   [tableau]
   server = 'tableau.server.address.com'
   site = 'insert-sitename'
   token_name = 'INSERT-YOUR-TOKEN-NAME-HERE'
   token_value = 'INSERT-YOUR-TABLEAU-API-KEY-HERE'
   ```
   you can find your server address and site name from the url you use to access tableau, for example:
   `https://server.name.here.com/#/site/site_name` the server name would be `https://server.name.here.com` and the site name would be `site_name`
2. update the workbook in tableau with these 4 parameters (you can set the default values all to the same date for speed in the tableau client, since the `mu` notebook sets these values itself for querying tableau anyway):
   | parameter | description |
   |------------------|-------------|
   | first_of_month | short date |
   | first_for_search | short date |
   | last_of_month | short date |
   | last_for_search | short date |
3. add the following calculated fields and set them as filters as described:
   | calculated field | data source | code | description |
   |--------------------|-----------------|----------------------------------------------------------------------------------------------|--------------------------------------------------------|
   | between_active | dispensations | [rx_end] >= [first_of_month] and [Filled At] <= [last_of_month] | replace other time based filters in `active_rx` with this set to True |
   | between_f_l_month | dispensations | [Written At] <= [last_of_month] and [Written At] >= [first_of_month] | replace other time based filters in `dispensations` with this set to True |
   | between_naive | dispensations | [naive_end] >= [first_of_month] and [Filled At] <= [last_of_month] | replace other time based filters in `naive_rx` with this set to True |
   | between_for_search | search requests | [Search Creation Date] <= [last_for_search] and [Search Creation Date] >= [first_for_search] | replace other time based filters in `searches` with this set To true |
4. next, uncomment the imports under `# for using tableau API:`
5. set `TABLEAU_API` to `True` in the constants section and set `WORKBOOK_NAME` to the name of the workbook you use for mandatory use (view names must match those in the [data readme](data/README.md) without the `_data.csv`).
6. choose to either use `AUTO_DATE` which will use the full previous month (if today is `May 24, 2024`, written start date will be set to `April 1, 2024` and written end date will be set to `April 30, 2024`) or to set `FIRST_WRITTEN_DATE` and `LAST_WRITTEN_DATE` manually, note that setting these dates too far apart will significantly slow performance and may cause the `tableauserverclient` to time out

</details>

## settings

the following settings are available in the notebook for easy adjustment

```python
RATIO = .7              # patient name similarity between rx and search to give search credit
PARTIAL_RATIO = .5      # patient name similarity between rx and search to give search credit for partial searches
DAYS_BEFORE = 7         # max number of days before an rx was written where searching should receive credit
FILTER_VETS = True      # remove veterinarians from the data

TESTING = False         # save progress and detail files such as search_results, dispensations_results, overlaps_active
# ------------------------------------------------------------------------------------------------------------------------------------
SUPPLEMENT = True       # add additional information to the results: overlapping dispensations, opioids to opioid naive patients, etc

OVERLAP_RATIO = .9      # patient name similarity between opioid and benzo prescriptions to confirm overlap
OVERLAP_TYPE = 'last'   # last, part, both; how overlaps are counted, see readme
NAIVE_RATIO = .7        # patient name similarity between 2 opioid rx to confirm patient is NOT opioid naive
MME_THRESHOLD = 90      # mme threshold for single rx
```

it is recommended to be more generous (use a lower ratio) when setting a ratio that will go in a prescriber's favor such as `RATIO`, `PARTIAL_RATIO`, and `NAIVE_RATIO` and to be more strict with ratios that go 'against' a prescriber like `OVERLAP_RATIO`

this way, potential false positives are more likely to favor the prescriber

### `OVERLAP_TYPE`

this setting controls how overlapping rx are measured

`part`: any time an rx written date for an opoid falls between the filled date and the end date for a benzodiazepine or vice versa, any prescribers involved will be counted as _participating_ in the overlap and `overlapping_rx_part` will increase

`last`: this setting is meant to only add to the count in `overlapping_rx_last` if the prescriber could have seen the first part of an overlap by performing a search, and still wrote the second part of the overlap, this means that **_the first rx of an overlap is not counted_**

as noted in the code, this also means that overlaps are only counted if the second rx was written a day after the first prescription was reported, this can be adjusted in the code according to state reporting frequency:

```python
.filter(
    # using create_date + 1 day for start date, adjust for reporting frequency
    ((pl.col('written_date_opi').is_between((pl.col('create_date') + pl.duration(days=1)), pl.col('rx_end'))) |
    (pl.col('written_date').is_between((pl.col('create_date_opi') + pl.duration(days=1)), pl.col('rx_end_opi'))))
)
```

this has the consequence of not counting any overlaps prescribed at the same time by the same prescriber; as stated above, the goal of this style of measurement is to only count overlaps that could have been prevented by the second prescriber performing a search

`both`: includes `overlapping_rx_part` and `overlapping_rx_last` in the results  
this comes at a performance cost as essentially, the overlap calculations must be run twice

---

> for pushing ipynb files without their output:  
> https://gist.github.com/33eyes/431e3d432f73371509d176d0dfb95b6e
