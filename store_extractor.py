import pandas as pd
from pytrends.request import TrendReq
import time
pd.options.display.max_rows = 500
pd.options.display.max_columns = 5100
pd.options.display.width = 2000
pd.options.display.max_colwidth = 1000


def add_date_components(df):
    df.reset_index(inplace=True)

    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['week'] = df['date'].dt.weekÒ

    return df


pytrends = TrendReq(hl='en-US', tz=360, geo='ZA')

area_codes = [
    'ZA',
    'ZA-EC',
    'ZA-FS',
    'ZA-GT',
    'ZA-NL',
    'ZA-LP',
    'ZA-MP',
    'ZA-NW',
    'ZA-NC',
    'ZA-WC'
]

area_table = pd.DataFrame(
    {'area': area_codes}
)

all_brands = [
    'Bradlows',
    'Russells',
    'Rochester',
    'Sleepmasters',
    'OK Furniture',
    'House and Home',
    'House & Home',
    'Lewis',
    'Beares',
    'Best Home & Electric',
    'Best Home and Electric',
    'Best Home',
    'Dial a Bed',
    'The Bed Centre',
    'Bed centre',
    'The Bed Shop',
    'Tafelberg',
    'The Mattress King',
    'Mattress Gallery',
    'Mattress Warehouse',
    'Ericssons',
]

start_date = '2017-01-01'
end_date = '2021-12-31'

overtime_results = []
vs_ok_overtime_results = []

for idx, brand in enumerate(all_brands):
    print(brand)
    if idx != 0 and idx % 5 == 0:
        time.sleep(5 * 60)
    for area in area_codes:
        print(area)
        pytrends.build_payload(
            brand if isinstance(brand, list) else [brand],
            timeframe=f'{start_date} {end_date}',
            geo=area,
            cat='11'
        )
        print('payload_built')
        brand_overtime = pytrends.interest_over_time()

        pytrends.build_payload(
            [brand, 'OK furniture'],
            timeframe=f'{start_date} {end_date}',
            geo=area,
            cat='11'
        )
        vs_ok_overtime = pytrends.interest_over_time()

        brand_overtime.rename(columns={brand: 'interest'}, inplace=True)
        brand_overtime['brand'] = brand
        brand_overtime['area'] = area

        vs_ok_overtime.rename(columns={brand: 'interest'}, inplace=True)
        vs_ok_overtime['brand'] = brand
        vs_ok_overtime['area'] = area

        overtime_results.append(brand_overtime)
        vs_ok_overtime_results.append(vs_ok_overtime)

area_cross_dates = area_table.merge(
    overtime_results[0].reset_index()[['date']],
    how='cross'
)
sealy_cross_dates = area_table.merge(
    vs_ok_overtime_results[0].reset_index()[['date']],
    how='cross'
)

overtime_results = [
    table
    for table
    in overtime_results
    if table.shape[0] > 0
]
vs_ok_overtime_results = [
    table
    for table
    in vs_ok_overtime_results
    if table.shape[0] > 0
]

single_search_3y_results = pd.concat(overtime_results, axis=0)
ok_single_search_3y_results = pd.concat(vs_ok_overtime_results, axis=0)

single_search_3y_results = add_date_components(df=single_search_3y_results)
ok_single_search_3y_results = add_date_components(df=ok_single_search_3y_results)

single_search_3y_results = area_cross_dates.merge(
    single_search_3y_results,
    on=['area', 'date'],
    how='left'
)
ok_single_search_3y_results = area_cross_dates.merge(
    ok_single_search_3y_results,
    on=['area', 'date'],
    how='left'
)

import numpy as np

single_search_3y_results['is_zero'] = np.where(single_search_3y_results['interest'] == 0, 1, 0)
ok_single_search_3y_results['is_zero'] = np.where(ok_single_search_3y_results['interest'] == 0, 1, 0)

single_search_3y_results.groupby(['brand', 'year'], as_index=False)['is_zero'].mean().sort_values('is_zero')

mins_per_brand_year = (
        single_search_3y_results[
            single_search_3y_results['is_zero'] == 0
            ].groupby(
            ['brand', 'year'],
            as_index=True
        )[['interest']].min() / 2
)
mins_per_brand_year.rename(
    columns={'interest': 'estimated_min_interest'},
    inplace=True
)

single_search_3y_results['interest'] = single_search_3y_results['interest'].fillna(0)
single_search_3y_results = single_search_3y_results.merge(
    mins_per_brand_year,
    on=['brand', 'year'],
    how='inner'
)

single_search_3y_results['estimated_interest'] = np.where(
    single_search_3y_results['interest'] == 0,
    single_search_3y_results['estimated_min_interest'],
    single_search_3y_results['interest'],
)

single_search_3y_results.to_csv('data/stores_search_5y_results.csv', index=False)
ok_single_search_3y_results.to_csv('data/ok_comparison_search_5y_results.csv', index=False)

