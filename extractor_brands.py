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


pytrends = TrendReq(hl='en-US', tz=360, geo='ZA', timeout=(10, 25))

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

own_brands = [
    'Sealy',
    'Edblo',
    'King Koil',
    'King Coil',
    'Slumber Land',  # 5
    'sealy posturepedic',
    'Slumberland'
]

competitor_brands = [
    "Ashleigh's",
    "Ashleighs",
    'Forgeron',  # 10
    'Restonic',
    'Cloud 9',
    'Cloud Nine',
    'Rest Assured',
    'Serta',
    'Simmons',
    'Tempur',
    'Dreamland',
    'Dunlopillo',
    'Forty Winks',
    'Sleepmasters',
]

other_terms = [
    'mattress',
    'bed'
]

# ambiguous_terms = [
#     ['Slumber Land', 'Slumberland'],
#     ['Cloud 9', 'Cloud Nine'],
#     ['Dreamland', 'Dream land'],
#     ['Sleepmasters', 'Sleep masters'],
#
# ]

all_brands = own_brands + competitor_brands + other_terms  # + other_terms + ambiguous_terms

start_date = '2017-01-01'
end_date = '2021-12-31'

overtime_results = []
vs_sealy_overtime_results = []
import time

for idx, brand in enumerate(all_brands):
    print(brand)
    if idx != 0 and idx % 4 == 0:
        time.sleep(7 * 60)
    for area in area_codes:
        print(area)
        pytrends.build_payload(
            brand if isinstance(brand, list) else [brand],
            timeframe=f'{start_date} {end_date}',
            geo=area,
            cat='11',

        )
        print('payload_built')
        brand_overtime = pytrends.interest_over_time()

        pytrends.build_payload(
            [brand, 'sealy'],
            timeframe=f'{start_date} {end_date}',
            geo=area,
            cat='11'
        )
        vs_sealy_overtime = pytrends.interest_over_time()

        brand_overtime.rename(columns={brand: 'interest'}, inplace=True)
        brand_overtime['brand'] = brand
        brand_overtime['area'] = area

        vs_sealy_overtime.rename(columns={brand: 'interest'}, inplace=True)
        vs_sealy_overtime['brand'] = brand
        vs_sealy_overtime['area'] = area

        if brand in own_brands:
            super_brand = 'own'
        elif brand in competitor_brands:
            super_brand = 'competitor_brand'
        elif brand in other_terms:
            super_brand = 'other'
        # elif brand in ambiguous_terms:
        #     super_brand = 'ambiguous_terms'
        else:
            super_brand = 'error'

        brand_overtime['super_brand'] = super_brand
        vs_sealy_overtime['super_brand'] = super_brand

        overtime_results.append(brand_overtime)
        vs_sealy_overtime_results.append(vs_sealy_overtime)

area_cross_dates = area_table.merge(
    overtime_results[0].reset_index()[['date']],
    how='cross'
)
sealy_cross_dates = area_table.merge(
    vs_sealy_overtime_results[0].reset_index()[['date']],
    how='cross'
)

overtime_results = [
    table
    for table
    in overtime_results
    if table.shape[0] > 0
]
vs_sealy_overtime_results = [
    table
    for table
    in vs_sealy_overtime_results
    if table.shape[0] > 0
]

single_search_3y_results = pd.concat(overtime_results, axis=0)
sealy_single_search_3y_results = pd.concat(vs_sealy_overtime_results, axis=0)

single_search_3y_results = add_date_components(df=single_search_3y_results)
sealy_single_search_3y_results = add_date_components(df=sealy_single_search_3y_results)

single_search_3y_results = area_cross_dates.merge(
    single_search_3y_results,
    on=['area', 'date'],
    how='left'
)
sealy_single_search_3y_results = area_cross_dates.merge(
    sealy_single_search_3y_results,
    on=['area', 'date'],
    how='left'
)

import numpy as np

single_search_3y_results['is_zero'] = np.where(single_search_3y_results['interest'] == 0, 1, 0)
sealy_single_search_3y_results['is_zero'] = np.where(sealy_single_search_3y_results['interest'] == 0, 1, 0)

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

single_search_3y_results.to_csv('data/single_search_5y_results.csv', index=False)
sealy_single_search_3y_results.to_csv('data/sealy_single_search_5y_results.csv', index=False)
#
# pivoted = pd.pivot_table(
#     data=single_search_3y_results,
#     columns='year',
#     values='interest',
#     index=['brand', 'super_brand', 'area'],
#     aggfunc='mean'
# )
# # pivoted_with_estimates = pd.pivot_table(
# #     data=single_search_3y_results,
# #     columns='year',
# #     values='estimated_interest',
# #     index=['brand', 'super_brand', 'area'],
# #     aggfunc='mean'
# # )
#
# pivoted.to_csv('pivoted.csv')
# # pivoted_with_estimates.to_csv('pivoted_with_estimates.csv')
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
# pivoted.sort_values(['super_brand', 'brand', '2021'], inplace=True)
#
# pivoted[year_columns] / pivoted[year_columns[0]]
# pivoted
#
# pytrends = TrendReq(hl='en-US', tz=360)
# pytrends.build_payload(
#     ['Sealy'],
#     timeframe='2019-01-01 2021-12-20',
#     # geo='ZA',
#     # gprop=''
# )
# brand_overtime = pytrends.interest_by_region(resolution=['SEALY'])
