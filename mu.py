import argparse
import calendar
import os
import time
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import polars as pl
import polars_distance as pld
from az_pmp_utils import tableau


def add_days(n: int, d: date | None = None) -> date:
    """
    add `n` days to date `d`

    args:
        n: the number of days to add
        d: the date to add days to

    returns:
        the date after adding the specified days
    """
    d = d or datetime.now(tz=ZoneInfo(os.environ.get('TZ', 'UTC'))).date()
    return d + timedelta(n)


def filter_vets(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    filter out veteranarians from the provided lazyframe

    args:
        lf: a lazyframe with an `animal_name` column

    returns:
        the lazyframe with veterinarian prescriptions filtered out
    """
    if args.no_filter_vets:
        lf = lf.drop('animal_name')
    else:
        lf = (
            lf
            .filter(
                (pl.col('animal_name') == 'Unspecified') |
                    (pl.col('animal_name') == '~')
            )
            .drop('animal_name')
        )

    return lf


def csv_from_view_id(file_name: str, luid: str, filters: dict | None = None) -> None:
    """
    writes a csv from a tableau view at the provided luid

    args:
        file_name: the filename, without an extension, to write
        luid: the luid of the view
        filters: filters to apply to the tableau view
    """
    lf = tableau.lazyframe_from_view_id(luid, filters)
    lf.collect().write_csv(f'data/{file_name}.csv')


def pull_files() -> None:
    """pull the necessary files mu files from tableau and write them to the data folder"""
    t_start_pull_files = time.perf_counter()
    if args.no_auto_date:
        first_of_month, last_of_month = args.first_written_date, args.last_written_date
    else:
        today = datetime.now(tz=ZoneInfo(os.environ.get('TZ', 'UTC')))
        last_of_month = add_days(-1, today.replace(day=1))
        first_of_month = last_of_month.replace(day=1)

    print(f'pulling files using written dates from {first_of_month!s} to {last_of_month!s}...')

    print('finding luids...')
    t_start = time.perf_counter()
    disp_luid = tableau.find_view_luid('dispensations', args.workbook_name)
    searches_luid = tableau.find_view_luid('searches', args.workbook_name)
    id_luid = tableau.find_view_luid('ID', args.workbook_name)
    if not args.no_supplement:
        active_rx_luid = tableau.find_view_luid('active_rx', args.workbook_name)
        naive_rx_luid = tableau.find_view_luid('naive_rx', args.workbook_name)
    t_elapsed = time.perf_counter() - t_start
    print(f'luids pulled: {t_elapsed:.2f}s')

    filters = {
        'first_of_month': first_of_month, 'last_of_month': last_of_month,
        'first_for_search': add_days(-args.days_before, first_of_month), 'last_for_search': add_days(1, last_of_month)
    }

    print('pulling dispensations_data...')
    t_start = time.perf_counter()
    csv_from_view_id('dispensations_data', disp_luid, filters)
    t_elapsed = time.perf_counter() - t_start
    print(f'pulled and wrote data/dispensations_data.csv: {t_elapsed:.2f}s')
    print('pulling searches_data...')
    t_start = time.perf_counter()
    csv_from_view_id('searches_data', searches_luid, filters)
    t_elapsed = time.perf_counter() - t_start
    print(f'pulled and wrote data/searches_data.csv: {t_elapsed:.2f}s')
    print('pulling ID_data...')
    t_start = time.perf_counter()
    csv_from_view_id('ID_data', id_luid, filters)
    t_elapsed = time.perf_counter() - t_start
    print(f'pulled and wrote data/ID_data.csv: {t_elapsed:.2f}s')
    if not args.no_supplement:
        print('pulling active_rx...')
        t_start = time.perf_counter()
        csv_from_view_id('active_rx_data', active_rx_luid, filters)  # type:ignore[reportPossiblyUnboundVariable] | not unbound
        t_elapsed = time.perf_counter() - t_start
        print(f'pulled and wrote data/active_rx_data.csv: {t_elapsed:.2f}s')
        print('pulling naive_rx...')
        t_start = time.perf_counter()
        csv_from_view_id('naive_rx_data', naive_rx_luid, filters)  # type:ignore[reportPossiblyUnboundVariable] | not unbound
        t_elapsed = time.perf_counter() - t_start
        print(f'pulled and wrote data/naive_rx_data.csv: {t_elapsed:.2f}s')
    t_end_pull_files = time.perf_counter()
    t_elapsed_pull_files = t_end_pull_files - t_start_pull_files
    print(f'files pulled: {t_elapsed_pull_files:.2f}s')


def supplement(final_dispensations: pl.LazyFrame, first_of_month: date, last_of_month: date, results: pl.DataFrame, users_explode: pl.LazyFrame) -> pl.DataFrame:
    print('adding supplemental information...')
    t_start_sup = time.perf_counter()

    active = (
        pl.scan_csv('data/active_rx_data.csv', infer_schema_length=10000)
        .rename({
            'Month, Day, Year of Patient Birthdate': 'dob', 'Month, Day, Year of Filled At': 'filled_date',
            'Month, Day, Year of Dispensations Created At': 'create_date', 'Month, Day, Year of Written At': 'written_date',
            'Orig Patient First Name': 'patient_first_name', 'Orig Patient Last Name': 'patient_last_name', 'Prescriber DEA': 'dea',
            'AHFS Description': 'ahfs', 'Month, Day, Year of rx_end': 'rx_end', 'Animal Name': 'animal_name'
        })
        .join(users_explode, how='left', left_on='dea', right_on='dea_number', coalesce=True)
        .with_columns(
            pl.col(['filled_date', 'create_date', 'written_date', 'rx_end', 'dob']).str.to_date('%B %d, %Y'),
            pl.col('true_id').fill_null(pl.col('dea')).alias('final_id'),
            (pl.col('patient_first_name') + ' ' + pl.col('patient_last_name')).str.to_uppercase().alias('patient_name'),
        )
        .drop('true_id', 'patient_first_name', 'patient_last_name')
    )

    active = filter_vets(active)

    benzo_active = (
        active
        .filter(
            pl.col('ahfs').str.contains('BENZO')
        )
    )

    opi_active = (
        active
        .filter(
            pl.col('ahfs').str.contains('OPIOID')
        )
    )
    t_elapsed = time.perf_counter() - t_start_sup
    print(f'supplemental files prep complete: {t_elapsed:.2f}s')

    t_start = time.perf_counter()
    if args.overlap_type in {'part', 'both'}:
        print('processing --overlap-type part...')
        overlap_active = (
            benzo_active
            .join(opi_active, how='left', on='dob', suffix='_opi', coalesce=True)
            .filter(
                (pl.col('written_date_opi').is_between(pl.col('filled_date'), pl.col('rx_end'))) |
                (pl.col('written_date').is_between(pl.col('filled_date_opi'), pl.col('rx_end_opi')))
            )
            .with_columns(
                (1 - pld.col('patient_name_opi').dist_str.jaro_winkler('patient_name')).alias('ratio')
            )
            .filter(
                pl.col('ratio') >= args.overlap_ratio
            )
            .collect(engine='streaming')
        )

        if args.testing:
            overlap_active.write_csv('overlaps_part.csv')

        benzo_dispensations_overlap = (
            overlap_active
            .filter(
                pl.col('written_date').is_between(first_of_month, last_of_month)
            )
            .select('final_id')
            .group_by('final_id')
            .len()
        )

        opi_dispensations_overlap = (
            overlap_active
            .filter(
                pl.col('written_date_opi').is_between(first_of_month, last_of_month)
            )
            .select('final_id_opi')
            .rename({'final_id_opi': 'final_id'})
            .group_by('final_id')
            .len()
        )

        all_overlaps = (
            pl.concat([benzo_dispensations_overlap, opi_dispensations_overlap])
            .group_by('final_id')
            .sum()
            .rename({'len': 'overlapping_rx_part'})
        )

        # add count of overlapping rx to the results
        results = (
            results
            .join(all_overlaps, how='left', on='final_id', coalesce=True)
            .with_columns(
                pl.col('overlapping_rx_part').fill_null(0)
            )
        )
        print('--overlap-type part complete')

    if args.overlap_type in {'last', 'both'}:
        print('processing --overlap-type last...')
        overlap_active = (
            benzo_active
            .join(opi_active, how='left', on='dob', suffix='_opi', coalesce=True)
            .filter(
                # using create_date + 1 day for start date, adjust for reporting frequency
                (pl.col('written_date_opi').is_between((pl.col('create_date') + pl.duration(days=1)), pl.col('rx_end'))) |
                (pl.col('written_date').is_between((pl.col('create_date_opi') + pl.duration(days=1)), pl.col('rx_end_opi')))
            )
            .with_columns(
                (1 - pld.col('patient_name_opi').dist_str.jaro_winkler('patient_name')).alias('ratio')
            )
            .filter(
                pl.col('ratio') >= args.overlap_ratio
            )
            .collect(engine='streaming')
        )

        if args.testing:
            overlap_active.write_csv('overlaps_last.csv')

        benzo_dispensations_overlap = (
            overlap_active
            .filter(
                (pl.col('written_date').is_between(first_of_month, last_of_month)) &
                (pl.col('written_date') > pl.col('written_date_opi'))
            )
            .select('final_id')
            .group_by('final_id')
            .len()
        )

        opi_dispensations_overlap = (
            overlap_active
            .filter(
                (pl.col('written_date_opi').is_between(first_of_month, last_of_month)) &
                (pl.col('written_date') < pl.col('written_date_opi'))
            )
            .select('final_id_opi')
            .rename({'final_id_opi': 'final_id'})
            .group_by('final_id')
            .len()
        )

        all_overlaps = (
            pl.concat([benzo_dispensations_overlap, opi_dispensations_overlap])
            .group_by('final_id')
            .sum()
            .rename({'len': 'overlapping_rx_last'})
        )

        # add count of overlapping rx to the results
        results = (
            results
            .join(all_overlaps, how='left', on='final_id', coalesce=True)
            .with_columns(
                pl.col('overlapping_rx_last').fill_null(0)
            )
        )
        print('--overlap-type last complete')

    t_elapsed = time.perf_counter() - t_start
    print(f'overlaps processed: {t_elapsed:.2f}s')

    print('processing opioid naive...')
    t_start = time.perf_counter()
    naive = (
        pl.scan_csv('data/naive_rx_data.csv', infer_schema_length=10000)
        .rename({
            'Orig Patient First Name': 'patient_first_name', 'Orig Patient Last Name': 'patient_last_name', 'Max. naive_end': 'naive_end',
            'Month, Day, Year of Patient Birthdate': 'dob', 'Month, Day, Year of Filled At': 'naive_filled_date', 'Animal Name': 'animal_name'
        })
        .with_columns(
            pl.col(['dob', 'naive_filled_date']).str.to_date('%B %-d, %Y'),
            pl.col('naive_end').str.to_date('%-m/%-d/%Y'),
            (pl.col('patient_first_name') + ' ' + pl.col('patient_last_name')).str.to_uppercase().alias('naive_patient_name')
        )
        .drop('patient_first_name', 'patient_last_name')
    )

    naive = filter_vets(naive)

    naive_disps = (
        final_dispensations
        .lazy()
        .filter(pl.col('ahfs').str.contains('OPIOID'))
        .join(naive, how='left', left_on='disp_dob', right_on='dob', coalesce=True)
        .filter(
            pl.col('written_date').is_between(pl.col('naive_filled_date'), pl.col('naive_end'))
        )
        .with_columns(
            (1 - pld.col('naive_patient_name').dist_str.jaro_winkler('patient_name')).alias('ratio')
        )
        .filter(
            pl.col('ratio') >= args.naive_ratio
        )
        .with_columns(
            pl.lit(False).alias('opi_naive')  # noqa: FBT003 | setting col values to False
        )
        .unique(subset=['final_id', 'rx_number'])
    )

    naive_disps = (
        final_dispensations
        .lazy()
        .join(naive_disps, how='left', on=['final_id', 'rx_number'], coalesce=True)
        .select('final_id', 'ahfs', 'opi_naive')
        .with_columns(
            pl.col('opi_naive').fill_null(True),  # noqa: FBT003 | setting col values to True
            ((pl.col('ahfs').str.contains('OPIOID')) & pl.col('opi_naive')).fill_null(True).alias('opi_to_opi_naive')  # noqa: FBT003 | setting col values to True
        )
        .select('final_id', 'opi_to_opi_naive')
        .filter(
            pl.col('opi_to_opi_naive')
        )
        .group_by('final_id').len()
        .rename({'len': 'opi_to_opi_naive'})
        .collect()
    )

    # add number of opioid dispensations to opioid naive patients to results
    results = (
        results
        .join(naive_disps, how='left', on='final_id', coalesce=True)
        .with_columns(
            pl.col('opi_to_opi_naive').fill_null(0)
        )
    )
    t_elapsed = time.perf_counter() - t_start
    print(f'naive processed: {t_elapsed:.2f}s')
    t_elapsed_sup = time.perf_counter() - t_start_sup
    print(f'supplemental information complete: {t_elapsed_sup:.2f}s')
    return results


def mu():
    print('preparing files...')
    t_start_mu = time.perf_counter()
    users = (
        pl.scan_csv('data/ID_data.csv', infer_schema_length=10000)
        .rename({
            'Associated DEA Number(s)': 'dea_number(s)', 'User ID': 'true_id', 'User Full Name': 'user_full_name', 'State Professional License': 'license_number',
            'Specialty Level 1': 'specialty_1', 'Specialty Level 2': 'specialty_2', 'Specialty Level 3': 'specialty_3'
        })
    )

    # each user dea gets its own row so a prescriber gets credit for searches on prescriptions with any of their registered deas
    users_explode = (
        users
        .with_columns(
            pl.col('dea_number(s)').str.to_uppercase().str.strip_chars().str.split(',').alias('dea_number')
        )
        .explode('dea_number')
        .select('true_id', 'dea_number')
        .with_columns(
            pl.col('dea_number').str.strip_chars()
        )
    )

    pattern = r'^[A-Za-z]{2}\d{7}$'  # 2 letters followed by 7 digits
    dispensations = (
        pl.scan_csv('data/dispensations_data.csv', infer_schema_length=10000)
        .rename({'Month, Day, Year of Patient Birthdate': 'disp_dob', 'Month, Day, Year of Written At': 'written_date',
                 'Month, Day, Year of Filled At': 'filled_date', 'Month, Day, Year of Dispensations Created At': 'disp_created_date',
                 'Prescriber First Name': 'prescriber_first_name', 'Prescriber Last Name': 'prescriber_last_name',
                 'Orig Patient First Name': 'patient_first_name', 'Orig Patient Last Name': 'patient_last_name',
                 'Prescriber DEA': 'prescriber_dea', 'Generic Name': 'generic_name', 'Prescription Number': 'rx_number',
                 'AHFS Description': 'ahfs', 'Daily MME': 'mme', 'Days Supply': 'days_supply', 'Animal Name': 'animal_name'})
        .with_columns(
            pl.col(['disp_dob', 'written_date', 'filled_date', 'disp_created_date']).str.to_date('%B %d, %Y'),
            pl.col('prescriber_dea').str.to_uppercase().str.strip_chars(),
            (pl.col('patient_first_name') + ' ' + pl.col('patient_last_name')).str.to_uppercase().alias('patient_name'),
            (pl.col('prescriber_first_name') + ' ' + pl.col('prescriber_last_name')).str.to_uppercase().alias('prescriber_name')
        )
        .filter(
            pl.col('prescriber_dea').str.contains(pattern)
        )
        .join(users_explode, how='left', left_on='prescriber_dea', right_on='dea_number', coalesce=True)
        .with_columns(
            (pl.col('written_date').dt.offset_by(f'-{args.days_before}d')).alias('start_date'),
            (pl.col('written_date').dt.offset_by('1d')).alias('end_date')   # to account for bamboo's issues handling UTC
        )
        .drop('patient_first_name', 'patient_last_name', 'prescriber_first_name', 'prescriber_last_name')
    )

    dispensations = filter_vets(dispensations)

    # for filtering searches to only the days we could potentially need
    if args.no_auto_date:
        first_of_month, last_of_month = args.first_written_date, args.last_written_date
    else:
        today = datetime.now(tz=ZoneInfo(os.environ.get('TZ', 'UTC')))
        last_of_month = add_days(-1, today.replace(day=1))
        first_of_month = last_of_month.replace(day=1)

    min_date = add_days(-args.days_before, first_of_month)
    max_date = add_days(1, last_of_month)

    searches = (
        pl.scan_csv('data/searches_data.csv', infer_schema_length=10000)
        .rename({'Month, Day, Year of Search Creation Date': 'created_date', 'Month, Day, Year of Searched DOB':
                'search_dob', 'Searched First Name': 'first_name', 'Searched Last Name': 'last_name',
                'Partial First Name?': 'partial_first', 'Partial Last Name?': 'partial_last', 'True ID': 'true_id'})
        .join(dispensations, on='true_id', how='semi')
        .with_columns(
            pl.col(['search_dob', 'created_date']).str.to_date('%B %d, %Y'),
            (pl.col('first_name') + ' ' + pl.col('last_name')).alias('full_name').str.to_uppercase(),
            (pl.col('partial_first') | pl.col('partial_last')).alias('partial')
        )
        .filter(
            pl.col('created_date').is_between(min_date, max_date)
        )
        .collect()
        .with_columns(
            (pl.col('partial').map_elements(lambda x: args.partial_ratio if x else args.ratio, return_dtype=pl.Float32)).alias('ratio_check')
        )
        .drop('first_name', 'last_name', 'partial_first', 'partial_last')
        .lazy()
    )
    t_elapsed = time.perf_counter() - t_start_mu
    print(f'users, dispensations, searches prepared: {t_elapsed:.2f}s')

    print('checking dispensations for searches...')
    t_start = time.perf_counter()
    dispensations_with_searches = (
        dispensations
        .join(searches, how='left', on='true_id', coalesce=True)
        .filter(
            (pl.col('created_date').is_between(pl.col('start_date'), pl.col('end_date'))) &
            (pl.col('disp_dob') == pl.col('search_dob'))
        )
        .with_columns(
            (1 - pld.col('full_name').dist_str.jaro_winkler('patient_name')).alias('ratio')
        )
        .filter(
            pl.col('ratio') >= pl.col('ratio_check')
        )
        # .collect(engine='streaming')
        .unique(subset=['rx_number', 'prescriber_dea', 'written_date'])
        .select('rx_number', 'prescriber_dea', 'written_date')
        .with_columns(
            pl.lit(True).alias('search')  # noqa: FBT003 | setting col values to True
        )
    )

    final_dispensations = (
        dispensations
        .join(dispensations_with_searches, how='left', on=['rx_number', 'prescriber_dea', 'written_date'], coalesce=True)
        .fill_null(False)  # noqa: FBT003 | setting col values to False
        .unique(subset=['rx_number', 'prescriber_dea', 'written_date'])
        .with_columns(
            pl.col('true_id').fill_null(pl.col('prescriber_dea')).alias('final_id')
        )
    )

    pattern_cap = r'^([A-Za-z]{2}\d{7})$'  # 2 letters followed by 7 digits
    deas = dispensations.select('prescriber_dea', 'prescriber_name').lazy()
    results = (
        final_dispensations
        .group_by(['final_id'])
        .agg([pl.len(), pl.col('search').sum()])
        .with_columns(
            ((pl.col('search') / pl.col('len')) * 100).alias('rate'),
            (pl.col('final_id').str.to_integer(base=10, strict=False).cast(pl.Int64)).alias('true_id'),
            (pl.col('final_id').str.extract(pattern_cap)).alias('unreg_dea')
        )
        .rename({'len': 'dispensations', 'search': 'searches'})
        .lazy()
        .join(users, how='left', on='true_id', coalesce=True)
        .join(deas, how='left', left_on='unreg_dea', right_on='prescriber_dea', coalesce=True)
        .unique('final_id')
        .with_columns(
            pl.col('user_full_name').fill_null(pl.col('prescriber_name')),
            pl.col('dea_number(s)').fill_null(pl.col('unreg_dea')),
            pl.col('true_id').is_not_null().alias('registered')
        )
        .drop('prescriber_name')
        .rename({'user_full_name': 'prescriber_name'})
        .select(
            'final_id', 'prescriber_name', 'dea_number(s)', 'license_number', 'specialty_1', 'specialty_2', 'specialty_3',
            'dispensations', 'searches', 'rate', 'registered'
        )
    )
    t_elapsed = time.perf_counter() - t_start
    print(f'dispensations checked for searches: {t_elapsed:.2f}s')

    if args.testing:
        results.collect().write_csv('search_results.csv')
        final_dispensations.collect(engine='streaming').write_csv('dispensations_results.csv')

    print('adding opioid and benzo counts...')
    t_start = time.perf_counter()
    opi_count = (
        final_dispensations
        .filter(pl.col('ahfs').str.contains('OPIOID'))
        .with_columns(
        (pl.col('filled_date') + pl.duration(days='days_supply')).alias('opi_end_date'),
        (pl.col('disp_created_date') + pl.duration(days=1)).alias('opi_start_date')
        )
        .rename({
            'written_date': 'opi_written_date', 'filled_date': 'opi_filled_date',
            'patient_name': 'opi_patient_name', 'disp_created_date': 'opi_disp_created_date'
        })
        .group_by('final_id')
        .len()
        .rename({'len': 'opi_rx'})
    )

    benzo_count = (
        final_dispensations
        .filter(pl.col('ahfs').str.contains('BENZO'))
        .with_columns(
            (pl.col('filled_date') + pl.duration(days='days_supply')).alias('benzo_end_date'),
            (pl.col('disp_created_date') + pl.duration(days=1)).alias('benzo_start_date')
        )
        .rename({
            'written_date': 'benzo_written_date', 'filled_date': 'benzo_filled_date',
            'patient_name': 'benzo_patient_name', 'disp_created_date': 'benzo_disp_created_date'
        })
        .group_by('final_id')
        .len()
        .rename({'len': 'benzo_rx'})
    )

    # add counts of opi and benzo disps
    results = (
        results
        .join(opi_count, how='left', on='final_id', coalesce=True)
        .with_columns(
            pl.col('opi_rx').fill_null(0)
        )
        .join(benzo_count, how='left', on='final_id', coalesce=True)
        .with_columns(
            pl.col('benzo_rx').fill_null(0)
        )
    )
    t_elapsed = time.perf_counter() - t_start
    print(f'opioid and benzo counts added: {t_elapsed:.2f}s')

    print('adding mmes over threshold...')
    t_start = time.perf_counter()
    over_mme = (
        final_dispensations
        .select('final_id', 'mme')
        .filter(
            pl.col('mme') >= args.mme_threshold
        )
        .group_by('final_id')
        .len()
        .rename({'len': 'rx_over_mme_threshold'})
    )

    # add count of rx over the mme threshold to the results
    results = (
        results
        .join(over_mme, how='left', on='final_id', coalesce=True)
        .with_columns(
            pl.col('rx_over_mme_threshold').fill_null(0)
        )
        .collect(engine='streaming')
    )
    t_elapsed = time.perf_counter() - t_start
    print(f'mmes added: {t_elapsed:.2f}s')

    if not args.no_supplement:
        results = supplement(final_dispensations, first_of_month, last_of_month, results, users_explode)

    print('processing results and writing files...')
    t_start = time.perf_counter()
    results = (
        results
        .sort(['searches', 'dispensations'], descending=[False, True])
    )

    start_month = calendar.month_name[first_of_month.month].lower()
    start_year = first_of_month.year
    end_month = calendar.month_name[last_of_month.month].lower()
    end_year = last_of_month.year
    result_file_name = 'results_full.csv'

    tail = 'full' if not args.no_supplement else 'base'

    if start_month == end_month:
        result_file_name = f'{start_month}{start_year}_mandatory_use_{tail}.csv'
    else:
        result_file_name = f'{start_month}{start_year}-{end_month}{end_year}_mandatory_use_{tail}.csv'

    results.write_csv(result_file_name)
    print(f'{result_file_name} saved')

    stats = (
        results
        .drop('rate')
        .sum()
        .with_columns(
            ((pl.col('searches') / pl.col('dispensations')) * 100).round(2).alias('rate')
        )
        .select('dispensations', 'searches', 'rate')
    )

    t_elapsed = time.perf_counter() - t_start
    print(f'results complete: {t_elapsed:.2f}s')
    t_elapsed_mu = time.perf_counter() - t_start_mu
    print(f'mu complete!: {t_elapsed_mu:.2f}s')
    print('stats below:')
    print(stats)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='configure constants')

    parser.add_argument('-r', '--ratio', type=float, default=0.7, help='patient name similarity ratio for full search (default: %(default)s)')
    parser.add_argument('-p', '--partial-ratio', type=float, default=0.5, help='patient name similarity ratio for partial search (default: %(default)s)')
    parser.add_argument('-d', '--days-before', type=int, default=7, help='max number of days before an rx was written to give credit for a search (default: %(default)s)')
    parser.add_argument('-nf', '--no-filter-vets', action='store_true', help='do not remove veterinarians from data')
    parser.add_argument('-t', '--testing', action='store_true', help='save progress and detail files')
    parser.add_argument('-ns', '--no-supplement', action='store_true', help='do not add additional information to the results')
    parser.add_argument('-o', '--overlap-ratio', type=float, default=0.9, help='patient name similarity for confirming overlap (default: %(default)s) only used if using --no-supplement')
    parser.add_argument('-ot', '--overlap-type', type=str, default='last', choices=['last', 'part', 'both'], help='type of overlap (default: %(default)s) only used if using --no-supplement')
    parser.add_argument('-n', '--naive-ratio', type=float, default=0.7, help='ratio for opioid naive confirmation (default: %(default)s) only used if using --no-supplement')
    parser.add_argument('-m', '--mme-threshold', type=int, default=90, help='mme threshold for single rx (default: %(default)s)')
    parser.add_argument('-ta', '--tableau-api', action='store_true', help='pull tableau files using the api')
    parser.add_argument('-w', '--workbook-name', type=str, default='mu', help='workbook name in tableau (default: %(default)s) only used if using --tableau-api')
    parser.add_argument('-na', '--no-auto-date', action='store_true', help='pull data based on last month only used if using --tableau-api')
    parser.add_argument('-f', '--first-written-date', type=date.fromisoformat, default=date(2024, 4, 1), help='first written date in tableau in YYYY-MM-DD format (default: %(default)s) only used if --tableau-api --no-auto-date')
    parser.add_argument('-l', '--last-written-date', type=date.fromisoformat, default=date(2024, 4, 30), help='last written date in tableau in YYYY-MM-DD format (default: %(default)s) only used if --tableau-api --no-auto-date')

    args = parser.parse_args()

    if args.tableau_api:
        pull_files()

    mu()
