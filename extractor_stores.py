import pandas as pd
from pytrends.request import TrendReq

pd.options.display.max_rows = 500
pd.options.display.max_columns = 5100
pd.options.display.width = 2000
pd.options.display.max_colwidth = 1000


def add_date_components(df):
    df.reset_index(inplace=True)

    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['week'] = df['date'].dt.week

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

stores = [
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
vs_sealy_overtime_results = []

for store in stores:
    print(store)
    for area in area_codes:
        print(area)
        pytrends.build_payload(
            store if isinstance(store, list) else [store],
            timeframe=f'{start_date} {end_date}',
            geo=area,
            cat='11'
        )
        stores_overtime = pytrends.interest_over_time()
        stores_overtime.rename(columns={store: 'interest'}, inplace=True)
        stores_overtime['brand'] = store
        stores_overtime['area'] = area

        overtime_results.append(stores_overtime)

area_cross_dates = area_table.merge(
    overtime_results[0].reset_index()[['date']],
    how='cross'
)

overtime_results = [
    table
    for table
    in overtime_results
    if table.shape[0] > 0
]
single_search_3y_results = pd.concat(overtime_results, axis=0)
single_search_3y_results = add_date_components(df=single_search_3y_results)
single_search_3y_results = area_cross_dates.merge(
    single_search_3y_results,
    on=['area', 'date'],
    how='left'
)
import numpy as np

single_search_3y_results['is_zero'] = np.where(single_search_3y_results['interest'] == 0, 1, 0)
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

single_search_3y_results.to_csv('data/stores_single_search_3y_results.csv')
#
# pivoted = pd.pivot_table(
#     data=single_search_3y_results,
#     columns='year',
#     values='interest',
#     index=['brand', 'super_brand', 'area'],
#     aggfunc='mean'
# )
#
# pivoted.to_csv('pivoted.csv')
#
# pivoted.columns = [str(int(col)) for col in pivoted.columns]
#
# year_columns = pivoted.columns
#
# unique_years = len(pivoted.columns)
#
# for increment in list(range(unique_years - 1))[::-1]:
#     pivoted[f'growth_{2021 - 1 - increment}_to_{2021 - increment}'] = (
#             (pivoted[f'{2021 - increment}'] / pivoted[f'{2021 - increment - 1}']) - 1
#     )
#
# for increment in list(range(unique_years))[::-1]:
#     pivoted[f'indexed_{2021 - increment}'] = pivoted[f'{2021 - increment}'] / pivoted[year_columns[0]]
#
# pivoted.reset_index(inplace=True)
