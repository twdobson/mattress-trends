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

pytrends.build_payload(
    ['mattress', 'bed', 'sealy'],
    timeframe=f'{start_date} {end_date}',
    geo='ZA',
    cat='11',

)
result = pytrends.interest_over_time()

result = add_date_components(df=result)

result.groupby(['year'], as_index=False)[['mattress', 'bed', 'sealy']].sum().to_clipboard()

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
