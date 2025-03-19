import argparse
import calendar
from datetime import date, timedelta

import polars as pl
from Levenshtein import ratio

# for using tableau API:
import tableauserverclient as TSC
import toml

def add_days(n:int, d:date = date.today()):
  return d + timedelta(n)

def filter_vets(df):
    if FILTER_VETS:
        df = (
            df
            .filter(
                (pl.col('animal_name') == 'Unspecified') |
                (pl.col('animal_name') == '~')
            )
            .drop('animal_name')
        )
    else:
        df = df.drop('animal_name')

    return df

def pull_files():

    if AUTO_DATE:
        today = date.today()
        last_of_month = add_days(-1, today.replace(day=1))
        first_of_month = last_of_month.replace(day=1)
    else:
        last_of_month = LAST_WRITTEN_DATE
        first_of_month = FIRST_WRITTEN_DATE

    with open('secrets.toml', 'r') as f:
            secrets = toml.load(f)

    server = secrets['tableau']['server']
    site = secrets['tableau']['site']
    token_name = secrets['tableau']['token_name']
    token_value = secrets['tableau']['token_value']

    tableau_auth = TSC.PersonalAccessTokenAuth(token_name, token_value, site)
    tableau_server = TSC.Server(server, use_server_version=True, http_options={'verify':False})

    def csv_from_view_id(file_name:str, view_id:str, filters:dict|None=None) -> None:
        if filters:
            options = TSC.CSVRequestOptions()
            for k,v in filters.items():
                options.vf(k,v)
        else:
            options = None
        view = tableau_server.views.get_by_id(view_id)
        tableau_server.views.populate_csv(view, options)
        with open(f'data/{file_name}.csv', 'wb') as f:
            f.write(b''.join(view.csv))

    def find_view_luid(view_name:str, workbook_name:str) -> str:
        all_workbooks = list(TSC.Pager(tableau_server.workbooks))
        searched_workbook = [workbook for workbook in all_workbooks if workbook.name==workbook_name][0]
        tableau_server.workbooks.populate_views(searched_workbook)
        views = searched_workbook.views
        searched_view = [view for view in views if view.name==view_name][0]
        if searched_view.id is None:
            raise Exception(f'luid for {view_name} in {workbook_name} not found')
        return searched_view.id

    with tableau_server.auth.sign_in(tableau_auth):
        print('finding dispensations luid...')
        disp_luid = find_view_luid('dispensations', WORKBOOK_NAME)
        print(disp_luid)
        print('finding searches luid...')
        searches_luid = find_view_luid('searches', WORKBOOK_NAME)
        print(searches_luid)
        print('finding id luid...')
        id_luid = find_view_luid('ID', WORKBOOK_NAME)
        print(id_luid)
        print('finding active_rx luid...')
        active_rx_luid = find_view_luid('active_rx', WORKBOOK_NAME)
        print(active_rx_luid)
        print('finding naive_rx luid...')
        naive_rx_luid = find_view_luid('naive_rx', WORKBOOK_NAME)
        print(naive_rx_luid)

        last_for_search = add_days(1, last_of_month)
        first_for_search = add_days(-DAYS_BEFORE, first_of_month)

        filters = {
            'first_of_month':first_of_month, 'last_of_month':last_of_month,
            'first_for_search':first_for_search, 'last_for_search':last_for_search
        }

        print('pulling dispensations_data...')
        csv_from_view_id('dispensations_data', disp_luid, filters)
        print('wrote data/dispensations_data.csv')
        print('pulling searches_data...')
        csv_from_view_id('searches_data', searches_luid, filters)
        print('wrote data/searches_data.csv')
        print('pulling ID_data...')
        csv_from_view_id('ID_data', id_luid, filters)
        print('wrote data/ID_data.csv')
        print('pulling active_rx...')
        csv_from_view_id('active_rx_data', active_rx_luid, filters)
        print('wrote data/active_rx.csv')
        print('pulling naive_rx...')
        csv_from_view_id('naive_rx_data', naive_rx_luid, filters)
        print('wrote data/naive_rx.csv')

def mu():
    print('preparing files...')
    users = (
        pl.scan_csv('data/ID_data.csv', infer_schema_length=10000)
        .rename({
            'Associated DEA Number(s)':'dea_number(s)', 'User ID':'true_id', 'User Full Name':'user_full_name', 'State Professional License':'license_number',
            'Specialty Level 1':'specialty_1', 'Specialty Level 2':'specialty_2', 'Specialty Level 3':'specialty_3'
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
    )

    pattern = r'^[A-Za-z]{2}\d{7}$' # 2 letters followed by 7 digits
    dispensations = (
        pl.scan_csv('data/dispensations_data.csv', infer_schema_length=10000)
        .rename({'Month, Day, Year of Patient Birthdate': 'disp_dob', 'Month, Day, Year of Written At': 'written_date',
                 'Month, Day, Year of Filled At': 'filled_date', 'Month, Day, Year of Dispensations Created At': 'disp_created_date',
                 'Prescriber First Name': 'prescriber_first_name', 'Prescriber Last Name': 'prescriber_last_name',
                 'Orig Patient First Name': 'patient_first_name', 'Orig Patient Last Name': 'patient_last_name',
                 'Prescriber DEA': 'prescriber_dea', 'Generic Name':'generic_name', 'Prescription Number':'rx_number',
                 'AHFS Description':'ahfs', 'Daily MME':'mme', 'Days Supply':'days_supply', 'Animal Name':'animal_name'})
        .with_columns(
            pl.col(['disp_dob', 'written_date', 'filled_date', 'disp_created_date']).str.to_date('%B %d, %Y'),
            pl.col('prescriber_dea').str.to_uppercase().str.strip_chars(),
            (pl.col('patient_first_name') + ' ' + pl.col('patient_last_name')).str.to_uppercase().alias('patient_name'),
            (pl.col('prescriber_first_name') + ' ' + pl.col('prescriber_last_name')).str.to_uppercase().alias('prescriber_name')
        )
        .filter(
            (pl.col('prescriber_dea').str.contains(pattern))
        )
        .join(users_explode, how='left', left_on='prescriber_dea', right_on='dea_number', coalesce=True)
        .collect()
        .with_columns(
            (pl.col('written_date').dt.offset_by(f'-{DAYS_BEFORE}d')).alias('start_date'),
            (pl.col('written_date').dt.offset_by('1d')).alias('end_date')   # to account for bamboo's issues handling of UTC
        )
        .drop('patient_first_name', 'patient_last_name', 'prescriber_first_name', 'prescriber_last_name')
    )

    dispensations = filter_vets(dispensations)

    #for filtering searches to only the days we could potentially need
    first_of_month = dispensations['written_date'].min()
    last_of_month = dispensations['written_date'].max()
    assert first_of_month is date, 'minimum of written_date should be a date'
    assert last_of_month is date, 'maximum of written_date should be a date'
    min_date = add_days(-DAYS_BEFORE, first_of_month)
    max_date = add_days(1, last_of_month)

    searches = (
        pl.scan_csv('data/searches_data.csv', infer_schema_length=10000)
        .rename({'Month, Day, Year of Search Creation Date': 'created_date', 'Month, Day, Year of Searched DOB':
                'search_dob', 'Searched First Name': 'first_name', 'Searched Last Name': 'last_name',
                'Partial First Name?': 'partial_first', 'Partial Last Name?': 'partial_last', 'True ID': 'true_id'})
        .with_columns(
            pl.col(['search_dob', 'created_date']).str.to_date('%B %d, %Y'),
            (pl.col('first_name') + ' ' + pl.col('last_name')).alias('full_name').str.to_uppercase(),
            (pl.col('partial_first') | pl.col('partial_last')).alias('partial')
        )
        .filter(
            pl.col('created_date').is_between(min_date, max_date) &
            pl.col('true_id').is_in(dispensations['true_id'])
        )
        .collect()
        .with_columns(
            (pl.col('partial').map_elements(lambda x: PARTIAL_RATIO if x else RATIO, return_dtype=pl.Float32)).alias('ratio_check')
        )
        .drop('first_name', 'last_name', 'partial_first', 'partial_last')
        .lazy()
    )
    print('users, dispensations, searches prepared')

    print('checking dispensations for searches...')
    dispensations_with_searches = (
        dispensations
        .lazy()
        .join(searches, how='left', on='true_id', coalesce=True)
        .filter(
            (pl.col('created_date').is_between(pl.col('start_date'), pl.col('end_date'))) &
            (pl.col('disp_dob') == pl.col('search_dob'))
        )
        .with_columns(
            pl.struct(['full_name', 'patient_name'])
            .map_elements(lambda x: ratio(x['full_name'], x['patient_name']), return_dtype=pl.Float32).alias('ratio')
        )
        .collect(streaming=True)
        .filter(
            pl.col('ratio') >= pl.col('ratio_check')
        )
        .unique(subset=['rx_number','prescriber_dea','written_date'])
        .select('rx_number','prescriber_dea','written_date')
        .with_columns(
            pl.lit(True).alias('search')
        )
    )

    final_dispensations = (
        dispensations
        .join(dispensations_with_searches, how='left', on=['rx_number','prescriber_dea','written_date'], coalesce=True)
        .fill_null(False)
        .unique(subset=['rx_number','prescriber_dea','written_date'])
        .with_columns(
            pl.col('true_id').fill_null(pl.col('prescriber_dea')).alias('final_id')
        )
    )

    pattern_cap = r'^([A-Za-z]{2}\d{7})$' # 2 letters followed by 7 digits
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
        .rename({'len':'dispensations', 'search':'searches'})
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
        .rename({'user_full_name':'prescriber_name'})
        .select(
            'final_id', 'prescriber_name', 'dea_number(s)', 'license_number', 'specialty_1', 'specialty_2', 'specialty_3',
            'dispensations', 'searches', 'rate', 'registered'
        )
        .collect()
    )
    print('dispensations checked for searches')

    if TESTING:
        results.write_csv('search_results.csv')
        final_dispensations.write_csv('dispensations_results.csv')

    print('adding opioid and benzo counts...')
    opi_count = (
        final_dispensations
        .lazy()
        .filter(pl.col('ahfs').str.contains('OPIOID'))
        .with_columns(
        (pl.col('filled_date') + pl.duration(days='days_supply')).alias('opi_end_date'),
        (pl.col('disp_created_date') + pl.duration(days=1)).alias('opi_start_date')
        )
        .rename({
            'written_date':'opi_written_date', 'filled_date':'opi_filled_date',
            'patient_name':'opi_patient_name', 'disp_created_date':'opi_disp_created_date'
        })
        .collect()
        .group_by('final_id')
        .len()
        .rename({'len':'opi_rx'})
    )

    benzo_count = (
        final_dispensations
        .lazy()
        .filter(pl.col('ahfs').str.contains('BENZO'))
        .with_columns(
            (pl.col('filled_date') + pl.duration(days='days_supply')).alias('benzo_end_date'),
            (pl.col('disp_created_date') + pl.duration(days=1)).alias('benzo_start_date')
        )
        .rename({
            'written_date':'benzo_written_date', 'filled_date':'benzo_filled_date',
            'patient_name':'benzo_patient_name', 'disp_created_date':'benzo_disp_created_date'
        })
        .collect()
        .group_by('final_id')
        .len()
        .rename({'len':'benzo_rx'})
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
    print('opioid and benzo counts added')

    print('adding mmes over threshold...')
    over_mme = (
        final_dispensations
        .select('final_id', 'mme')
        .filter(
            pl.col('mme') >= MME_THRESHOLD
        )
        .group_by('final_id')
        .len()
        .rename({'len':'rx_over_mme_threshold'})
    )

    # add count of rx over the mme threshold to the results
    results = (
        results
        .join(over_mme, how='left', on='final_id', coalesce=True)
        .with_columns(
            pl.col('rx_over_mme_threshold').fill_null(0)
        )
    )
    print('mmes added')

    if SUPPLEMENT:
        print('adding supplemental information...')

        active = (
            pl.scan_csv('data/active_rx_data.csv', infer_schema_length=10000)
            .rename({
                'Month, Day, Year of Patient Birthdate':'dob', 'Month, Day, Year of Filled At':'filled_date',
                'Month, Day, Year of Dispensations Created At':'create_date', 'Month, Day, Year of Written At':'written_date',
                'Orig Patient First Name':'patient_first_name', 'Orig Patient Last Name':'patient_last_name', 'Prescriber DEA':'dea',
                'AHFS Description':'ahfs', 'Month, Day, Year of rx_end':'rx_end', 'Animal Name':'animal_name'
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

        if OVERLAP_TYPE in ['part', 'both']:
            print('processing overlap="part"...')
            overlap_active = (
                benzo_active
                .join(opi_active, how='left', on='dob', suffix='_opi', coalesce=True)
                .filter(
                    ((pl.col('written_date_opi').is_between(pl.col('filled_date'), pl.col('rx_end'))) |
                    (pl.col('written_date').is_between(pl.col('filled_date_opi'), pl.col('rx_end_opi'))))
                )
                .with_columns(
                    pl.struct(['patient_name_opi', 'patient_name'])
                    .map_elements(lambda x: ratio(x['patient_name_opi'], x['patient_name']), return_dtype=pl.Float32).alias('ratio')
                )
                .filter(
                    pl.col('ratio') >= OVERLAP_RATIO
                )
                .collect(streaming=True)
            )

            if TESTING:
                overlap_active.write_csv('overlaps_part.csv')

            benzo_dispensations_overlap = (
                overlap_active
                .filter(
                    (pl.col('written_date').is_between(first_of_month, last_of_month))
                )
                .select('final_id')
                .group_by('final_id')
                .len()
            )

            opi_dispensations_overlap = (
                overlap_active
                .filter(
                    (pl.col('written_date_opi').is_between(first_of_month, last_of_month))
                )
                .select('final_id_opi')
                .rename({'final_id_opi':'final_id'})
                .group_by('final_id')
                .len()
            )

            all_overlaps = (
                pl.concat([benzo_dispensations_overlap, opi_dispensations_overlap])
                .group_by('final_id')
                .sum()
                .rename({'len':'overlapping_rx_part'})
            )

            # add count of overlapping rx to the results
            results = (
                results
                .join(all_overlaps, how='left', on='final_id', coalesce=True)
                .with_columns(
                    pl.col('overlapping_rx_part').fill_null(0)
                )
            )
            print('overlap="part" complete')

        if OVERLAP_TYPE in ['last', 'both']:
            print('processing overlap="both"...')
            overlap_active = (
                benzo_active
                .join(opi_active, how='left', on='dob', suffix='_opi', coalesce=True)
                .filter(
                    # using create_date + 1 day for start date, adjust for reporting frequency
                    ((pl.col('written_date_opi').is_between((pl.col('create_date') + pl.duration(days=1)), pl.col('rx_end'))) |
                    (pl.col('written_date').is_between((pl.col('create_date_opi') + pl.duration(days=1)), pl.col('rx_end_opi'))))
                )
                .with_columns(
                    pl.struct(['patient_name_opi', 'patient_name'])
                    .map_elements(lambda x: ratio(x['patient_name_opi'], x['patient_name']), return_dtype=pl.Float32).alias('ratio')
                )
                .filter(
                    pl.col('ratio') >= OVERLAP_RATIO
                )
                .collect(streaming=True)
            )

            if TESTING:
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
                .rename({'final_id_opi':'final_id'})
                .group_by('final_id')
                .len()
            )

            all_overlaps = (
                pl.concat([benzo_dispensations_overlap, opi_dispensations_overlap])
                .group_by('final_id')
                .sum()
                .rename({'len':'overlapping_rx_last'})
            )

            # add count of overlapping rx to the results
            results = (
                results
                .join(all_overlaps, how='left', on='final_id', coalesce=True)
                .with_columns(
                    pl.col('overlapping_rx_last').fill_null(0)
                )
            )
            print('overlap="last" processed')

        print('processing opioid naive...')
        naive = (
            pl.scan_csv('data/naive_rx_data.csv', infer_schema_length=10000)
            .rename({
                'Orig Patient First Name':'patient_first_name', 'Orig Patient Last Name':'patient_last_name', 'Max. naive_end':'naive_end',
                'Month, Day, Year of Patient Birthdate':'dob', 'Month, Day, Year of Filled At':'naive_filled_date', 'Animal Name':'animal_name'
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
                pl.struct(['patient_name', 'naive_patient_name'])
                .map_elements(lambda x: ratio(x['patient_name'], x['naive_patient_name']), return_dtype=pl.Float32).alias('ratio')
            )
            .filter(
                pl.col('ratio') >= NAIVE_RATIO
            )
            .with_columns(
                pl.lit(False).alias('opi_naive')
            )
            .unique(subset=['final_id', 'rx_number'])
        )

        naive_disps = (
            final_dispensations
            .lazy()
            .join(naive_disps, how='left', on=['final_id', 'rx_number'], coalesce=True)
            .select('final_id', 'ahfs', 'opi_naive')
            .with_columns(
                pl.col('opi_naive').fill_null(True),
                ((pl.col('ahfs').str.contains('OPIOID')) & pl.col('opi_naive')).fill_null(True).alias('opi_to_opi_naive')
            )
            .select('final_id', 'opi_to_opi_naive')
            .filter(
                pl.col('opi_to_opi_naive')
            )
            .group_by('final_id').len()
            .rename({'len':'opi_to_opi_naive'})
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
        print('naive added')
        print('supplemental information complete')

    print('processing results and writing files...')
    results = (
        results
        .sort(['searches', 'dispensations'], descending=[False, True])
    )

    start_month = calendar.month_name[first_of_month.month].lower()
    start_year = first_of_month.year
    end_month = calendar.month_name[last_of_month.month].lower()
    end_year = last_of_month.year
    result_file_name = 'results_full.csv'

    tail = 'full' if SUPPLEMENT else 'base'

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

    print('complete! stats below:')
    print(stats)

def parse_arguments():
    parser = argparse.ArgumentParser(description='configure constants')

    parser.add_argument('--r', '--ratio', type=float, default=0.7, help='patient name similarity ratio for full search (default: %(default)s)')
    parser.add_argument('--p', '--partial-ratio', type=float, default=0.5, help='patient name similarity ratio for partial search (default: %(default)s)')
    parser.add_argument('--d', '--days-before', type=int, default=7, help='max number of days before an rx was written to give credit for a search (default: %(default)s)')
    parser.add_argument('--nf', '--no-filter-vets', action='store_false', help='do not remove veterinarians from data')
    parser.add_argument('--t', '--testing', action='store_true', help='save progress and detail files')
    parser.add_argument('--ns', '--no-supplement', action='store_false', help='do not add additional information to the results')
    parser.add_argument('--o', '--overlap-ratio', type=float, default=0.9, help='patient name similarity for confirming overlap (default: %(default)s) only used if using --supplement')
    parser.add_argument('--ot', '--overlap-type', type=str, default='last', choices=['last', 'part', 'both'], help='type of overlap (default: %(default)s) only used if using --supplement')
    parser.add_argument('--n', '--naive-ratio', type=float, default=0.7, help='ratio for opioid naive confirmation (default: %(default)s) only used if using --supplement')
    parser.add_argument('--m', '--mme-threshold', type=int, default=90, help='mme threshold for single rx (default: %(default)s)')
    parser.add_argument('--ta', '--tableau-api', action='store_true', help='pull tableau files using the api')
    parser.add_argument('--w', '--workbook-name', type=str, default='mu', help='workbook name in tableau (default: %(default)s) only used if using --tableau-api')
    parser.add_argument('--a', '--auto-date', type=bool, default=True, help='pull data based on last month (default: %(default)s) only used if using --tableau-api')
    parser.add_argument('--f', '--first-written-date', type=str, default=str(date(2024, 4, 1)), help='first written date in tableau in YYYY-MM-DD format (default: %(default)s) only used if --tableau-api --auto-date False')
    parser.add_argument('--l', '--last-written-date', type=str, default=str(date(2024, 4, 30)), help='last written date in tableau in YYYY-MM-DD format (default: %(default)s) only used if --tableau-api --auto-date False')

    return parser.parse_args()

def main():
    args = parse_arguments()

    global RATIO, PARTIAL_RATIO, DAYS_BEFORE, FILTER_VETS, TESTING, SUPPLEMENT, OVERLAP_RATIO, OVERLAP_TYPE, NAIVE_RATIO
    global MME_THRESHOLD, TABLEAU_API, WORKBOOK_NAME, AUTO_DATE, FIRST_WRITTEN_DATE, LAST_WRITTEN_DATE

    RATIO = args.r
    PARTIAL_RATIO = args.p
    DAYS_BEFORE = args.d
    FILTER_VETS = args.nf
    TESTING = args.t
    SUPPLEMENT = args.ns
    OVERLAP_RATIO = args.o
    OVERLAP_TYPE = args.ot
    NAIVE_RATIO = args.n
    MME_THRESHOLD = args.m
    TABLEAU_API = args.ta
    WORKBOOK_NAME = args.w
    AUTO_DATE = args.a
    FIRST_WRITTEN_DATE = date.fromisoformat(args.f)
    LAST_WRITTEN_DATE = date.fromisoformat(args.l)

    if TABLEAU_API:
        pull_files()

    mu()

if __name__ == '__main__':
    main()
