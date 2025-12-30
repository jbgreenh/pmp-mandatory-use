# pmp-mandatory-use

custom mandatory use compliance module for prescription monitoring programs

## setup

this project uses [uv](https://github.com/astral-sh/uv?tab=readme-ov-file)  
after installing `uv` on your system using the link above and adding the input data ([data](#data)), you can run the script using `uv run mu.py`

## data

see the [data readme](data/README.md) for more information on the required input data

<details>
    <summary>tableau api</summary>

to make use of `tableauserverclient` to have the script pull the data instead of downloading manually:

1. you will need a `.env` file in the following format in the root folder of this repo:

   ```text
   TABLEAU_SERVER='tableau.server.address.com'
   TABLEAU_SITE='insert-sitename'
   TABLEAU_TOKEN_NAME='INSERT-YOUR-TOKEN-NAME-HERE'
   TABLEAU_TOKEN_VALUE='INSERT-YOUR-TABLEAU-API-KEY-HERE'
   ```

   you can find your server address and site name from the url you use to access tableau, for example:
   `https://server.name.here.com/#/site/site_name` the server name would be `https://server.name.here.com` and the site name would be `site_name`
2. update the workbooks in tableau with these 4 parameters (you can set the default values all to the same date for speed in the tableau client, since `mu.py` sets these values itself for querying tableau anyway):

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

4. use `--tableau-api` or `-ta` when running `mu.py`, if you use a different name than `mu` for the workbook you use for mandatory use in tableau, set the workbook name using `--workbook-name name` `-w name` (view names must match those in the [data readme](data/README.md) without the `_data.csv`).
5. the script automatically chooses dates, using the full previous month (if today is `May 24, 2024`, written start date will be set to `April 1, 2024` and written end date will be set to `April 30, 2024`)
6. to set custom start and end dates, use `--no-auto-date` or `-na` to turn off auto-dates, `--first-written-date` or `-f` to set the first written date and `--last-written-date` or `-l` to set the last written date
7. for example, to run the script for the month of January 2021, use `uv run mu.py -ta -na -f 2021-01-01 -l 2021-01-31`; note that setting longer date ranges will drastically effect performance, as well as risk timing out the `tableauserverclient`
8. for more details on the available arguments when running `mu.py` see [settings](#settings)

</details>

## settings

a variety of settings are available for customizing how the data is processed  
use `uv run mu.py -h` to see the available settings and their defaults:

<details>
    <summary>help output</summary>

```text
usage: mu.py [-h] [-r RATIO] [-p PARTIAL_RATIO] [-d DAYS_BEFORE] [-nf] [-t] [-ns] [-o OVERLAP_RATIO]
             [-ot {last,part,both}] [-n NAIVE_RATIO] [-m MME_THRESHOLD] [-ta] [-w WORKBOOK_NAME] [-na]
             [-f FIRST_WRITTEN_DATE] [-l LAST_WRITTEN_DATE]

configure constants

options:
  -h, --help            show this help message and exit
  -r, --ratio RATIO     patient name similarity ratio for full search (default: 0.7)
  -p, --partial-ratio PARTIAL_RATIO
                        patient name similarity ratio for partial search (default: 0.5)
  -d, --days-before DAYS_BEFORE
                        max number of days before an rx was written to give credit for a search (default: 7)
  -nf, --no-filter-vets
                        do not remove veterinarians from data
  -t, --testing         save progress and detail files
  -ns, --no-supplement  do not add additional information to the results
  -o, --overlap-ratio OVERLAP_RATIO
                        patient name similarity for confirming overlap (default: 0.9) only used if using
                        --supplement
  -ot, --overlap-type {last,part,both}
                        type of overlap (default: last) only used if using --supplement
  -n, --naive-ratio NAIVE_RATIO
                        ratio for opioid naive confirmation (default: 0.7) only used if using --supplement
  -m, --mme-threshold MME_THRESHOLD
                        mme threshold for single rx (default: 90)
  -ta, --tableau-api    pull tableau files using the api
  -w, --workbook-name WORKBOOK_NAME
                        workbook name in tableau (default: mu) only used if using --tableau-api
  -na, --no-auto-date   pull data based on last month only used if using --tableau-api
  -f, --first-written-date FIRST_WRITTEN_DATE
                        first written date in tableau in YYYY-MM-DD format (default: 2024-04-01) only used if
                        --tableau-api --no-auto-date
  -l, --last-written-date LAST_WRITTEN_DATE
                        last written date in tableau in YYYY-MM-DD format (default: 2024-04-30) only used if
                        --tableau-api --no-auto-date
```

</details>

for example, to disable the filter for removing veterinarian prescriptions from the data, lower the ratio for matching in the case of partial searches from the default of `0.5` to `0.4` and use no supplement data:

```text
uv run mu.py --no-filter-vets --partial-ratio 0.4 --no-supplement
```

or:

```text
uv run mu.py -nf -p 0.4 -ns
```

it is recommended to be more generous (use a lower ratio) when setting a ratio that will go in a prescriber's favor such as `--ratio`, `--partial-ratio`, and `--naive-ratio` and to be more strict with ratios that go 'against' a prescriber like `--overlap-ratio`, this way, potential false positives are more likely to favor the prescriber

### `--overlap-type`

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

`overlap-type` is set to `last` by default

### notebook version

to use the old ipynb version (no longer supported), use the `notebook` branch: `git checkout notebook`
