# pmp-mandatory-use
custom mandatory use compliance module for prescription monitoring programs  
## setup  
this notebook has been developed and tested in python3.11
```
pip install -r requirements.txt
```
## data
see the [data readme](data/README.md) for more information on the required input data  
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
it is recommended to be more generous (use a lower ratio) when setting a ratio that will go in a prescriber's favor  
such as `RATIO`, `PARTIAL_RATIO`, and `NAIVE_RATIO` and to be more strict with ratios that go 'against' a prescriber like `OVERLAP_RATIO`  
this way, potential false positives are more likely to favor the prescriber
### ``OVERLAP_TYPE``
this setting controls how overlapping rx are measured  

`part`: any time an rx written date for an opoid falls between the filled date and the end date for a benzodiazepine or vice versa, any prescribers involved will be counted as participating in the overlap and `overlapping_rx_part` will increase  

`last`: this setting is meant to only add to the count in `overlapping_rx_last` if the prescriber could have seen the first part of an overlap by performing a search, and still wrote the second part of the overlap, this means that ***the first rx of an overlap is not counted***  

as noted in the code, this also means that overlaps are only counted if the second rx was written a day after the first prescription was reported, this can be adjusted in the code according to state reporting frequency:
```python
.filter(
    # using create_date + 1 day for start date, adjust for reporting frequency
    ((pl.col('written_date_opi').is_between((pl.col('create_date') + pl.duration(days=1)), pl.col('rx_end'))) |
    (pl.col('written_date').is_between((pl.col('create_date_opi') + pl.duration(days=1)), pl.col('rx_end_opi'))))
)
```
this has the consequence of not counting any overlaps prescribed at the same time by the same prescriber, as stated above, the goal of this style of measurement is to only count overlaps that could have been prevented by the second prescriber performing a search

`both`: includes `overlapping_rx_part` and `overlapping_rx_last` in the results  
this comes at a performance cost as essentially, the overlap calculations must be run twice
___
> for pushing ipynb files without their output:  
> https://gist.github.com/33eyes/431e3d432f73371509d176d0dfb95b6e  

