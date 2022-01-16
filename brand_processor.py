import pandas as pd

pd.options.display.max_rows = 500
pd.options.display.max_columns = 5100
pd.options.display.width = 2000
pd.options.display.max_colwidth = 1000

area_codes_mapping = {
    'ZA': 'National',
    'ZA-EC': 'Eastern Cape',
    'ZA-FS': 'Free State',
    'ZA-GT': 'Gauteng',
    'ZA-NL': 'KwaZulu-Natal',
    'ZA-LP': 'Limpopo',
    'ZA-MP': 'Mpumalanga',
    'ZA-NW': 'North West',
    'ZA-NC': 'Northern Cape',
    'ZA-WC': 'Western Cape'
}

area_code_order = {
    'National': 0,
    'Eastern Cape': 4,
    'Free State': 6,
    'Gauteng': 1,
    'KwaZulu-Natal': 2,
    'Limpopo': 5,
    'Mpumalanga': 7,
    'North West': 8,
    'Northern Cape': 9,
    'Western Cape': 3,
}

big_areas = [
    'Eastern Cape',
    'Gauteng',
    'KwaZulu-Natal',
    'Western Cape'
]

big_brands = [
    'Seal',  # 1
    'sealy posturepedic',

    'Restonic',  # 2
    'Cloud nine - combined',  # 3
    'Simmons',  # 4
    'Tempur',  # 5
    'Rest Assured',  # 6
    'Serta'  # 7
]

single_search_3y_results = pd.read_csv('data/single_search_5y_results.csv')
sealy_single_search_3y_results = pd.read_csv('data/sealy_single_search_5y_results.csv')

single_search_3y_results['area'] = single_search_3y_results['area'].map(area_codes_mapping)
sealy_single_search_3y_results['area'] = sealy_single_search_3y_results['area'].map(area_codes_mapping)

single_search_3y_results = single_search_3y_results[
    ~single_search_3y_results['brand'].isin(['sealy posturepedic', 'Sleepmasters'])
]
sealy_single_search_3y_results = sealy_single_search_3y_results[
    ~sealy_single_search_3y_results['brand'].isin(['sealy posturepedic', 'Sleepmasters'])
]

sealy_single_search_3y_results['brand'].unique()

brand_combining = {
    'King Koil': 'King Koil - combined',
    'King Coil': 'King Koil - combined',
    'Slumberland': 'Slumberland - combined',
    'Slumber Land': 'Slumberland - combined',
    'Cloud 9': 'Cloud nine - combined',
    'Cloud Nine': 'Cloud nine - combined',

}

brand_remapping = {
    brand: brand_combining.get(brand, brand)
    for brand
    in sealy_single_search_3y_results['brand'].unique()
}

sealy_single_search_3y_results['brand_dirty'] = sealy_single_search_3y_results['brand']
sealy_single_search_3y_results['brand'] = sealy_single_search_3y_results['brand'].map(
    lambda x: brand_remapping.get(x, x))

total_sa_results = single_search_3y_results[
    single_search_3y_results['area'] == 'National'
    ]
sealy_total_sa_results = sealy_single_search_3y_results[
    sealy_single_search_3y_results['area'] == 'National'
    ]

total_sa_results.groupby(['brand', 'year'])['interest'].mean().sort_values(ascending=False)
sealy_total_sa_results.groupby(['brand', 'year'])['interest'].mean().sort_values(ascending=False)

sealy_vs_other_brands_area_year = sealy_single_search_3y_results.groupby(
    ['brand', 'year', 'area'],
    as_index=False
)['interest'].sum()

average_sealy_vs_other_brands_area_year = sealy_single_search_3y_results.groupby(
    ['brand', 'year', 'area'],
    as_index=False
)['interest'].mean().to_clipboard()

total_area_year = sealy_single_search_3y_results.groupby(
    ['area', 'year'],
    as_index=False
)['interest'].sum().rename(columns={'interest': 'total_interest'})

total_year = sealy_total_sa_results.groupby(
    ['area', 'year'],
    as_index=False
)['interest'].sum().rename(columns={'interest': 'total_interest'})

seal_vs_other_brand_proportions = sealy_vs_other_brands_area_year.merge(
    total_area_year,
    on=['area', 'year'],
    how='inner'
)

seal_vs_other_brand_proportions['proportion_of_interest'] = (
        seal_vs_other_brand_proportions['interest'] / seal_vs_other_brand_proportions['total_interest']
)

seal_vs_other_brand_proportions.sort_values('proportion_of_interest', ascending=False)

top_n = [
    'Sealy',
    'Cloud nine - combined',
    'Simmons',
    'Restonic',
    'Rest Assured',
    'Serta'
]

own_brands = [
    'King Koil - combined',
    'Slumberland - combined',
    'Sealy',
    'Edblo'
]

seal_vs_other_brand_proportions['top_n'] = seal_vs_other_brand_proportions['brand'].isin(top_n)
seal_vs_other_brand_proportions['own_brands'] = seal_vs_other_brand_proportions['brand'].isin(own_brands)

seal_vs_other_brand_proportions.groupby(
    ['year', 'area', 'top_n'],
    as_index=False
).agg(
    brands=('brand', 'nunique'),
    proportion_of_interest=('proportion_of_interest', 'sum')
).sort_values(['top_n', 'area', 'year']).to_clipboard()

seal_vs_other_brand_proportions.groupby(
    ['year', 'area', 'own_brands'],
    as_index=False
).agg(
    brands=('brand', 'nunique'),
    proportion_of_interest=('proportion_of_interest', 'sum')
).sort_values(['own_brands', 'area', 'year']).to_clipboard()

analysis_only_sealy = seal_vs_other_brand_proportions[
    seal_vs_other_brand_proportions['own_brands']
].groupby(
    ['year', 'area', 'brand'],
    as_index=False
).agg(
    brands=('brand', 'nunique'),
    proportion_of_interest=('proportion_of_interest', 'sum')
).sort_values(['year', 'brand', 'area'])

analysis_only_sealy['total_year_proportion'] = analysis_only_sealy.groupby(
    ['year', 'area']
)['proportion_of_interest'].transform('sum')

analysis_only_sealy['proportion_of_interest'] = (
        analysis_only_sealy['proportion_of_interest'] / analysis_only_sealy['total_year_proportion']
)
analysis_only_sealy.sort_values(['area', 'year', 'brand']).to_clipboard()

## XXXXXXXX
analysis_only_sealy = seal_vs_other_brand_proportions[
    seal_vs_other_brand_proportions['own_brands']
].groupby(
    ['year', 'area', 'brand'],
    as_index=False
).agg(
    brands=('brand', 'nunique'),
    interest=('interest', 'sum')
).sort_values(['year', 'brand', 'area'])

# 2. Total brand interest by year
# ---------------------- OUTPUT 1
# total_brand_interest_by_year_and_area = sealy_single_search_3y_results.groupby(
#     ['year', 'area'],
#     as_index=False
# )['interest'].sum().sort_values(['area', 'year'])
#
# total_brand_interest_by_year_and_area['first_year_total_interest'] = total_brand_interest_by_year_and_area.groupby(
#     ['area']
# )['interest'].transform('first')
#
# total_brand_interest_by_year_and_area['index_interest'] = (
#         total_brand_interest_by_year_and_area['interest']
#         / total_brand_interest_by_year_and_area['first_year_total_interest']
# )
# total_brand_interest_by_year_and_area.to_clipboard()
# ---------------------- OUTPUT 2
# total_bravo_vs_rest_interest_by_year_and_area = sealy_single_search_3y_results.groupby(
#     ['year', 'area', 'super_brand'],
#     as_index=False
# )['interest'].sum().sort_values(['area', 'super_brand', 'year'])
#
# total_bravo_vs_rest_interest_by_year_and_area['first_year_total_interest'] = total_bravo_vs_rest_interest_by_year_and_area.groupby(
#     ['area', 'super_brand']
# )['interest'].transform('first')
#
# total_bravo_vs_rest_interest_by_year_and_area['index_interest'] = (
#         total_bravo_vs_rest_interest_by_year_and_area['interest']
#         / total_bravo_vs_rest_interest_by_year_and_area['first_year_total_interest']
# )
# total_bravo_vs_rest_interest_by_year_and_area.to_clipboard()

# ----------------------------------- OUTPUT 3
# provincial_share = sealy_single_search_3y_results.loc[
#     sealy_single_search_3y_results['area'] != 'National'
#     ].groupby(
#     ['area', 'year'],
#     as_index=False
# )['interest'].sum().sort_values(['area', 'year'])
#
# provincial_share['year_interest'] = provincial_share.groupby(
#     ['year']
# )['interest'].transform('sum')
#
# provincial_share['first_year_total_interest'] = provincial_share.groupby(
#     ['area']
# )['interest'].transform('first')
#
# provincial_share['index_interest'] = (
#         provincial_share['interest']
#         / provincial_share['year_interest']
# )
# provincial_share['index_interest_vs_2017'] = (
#         provincial_share['interest']
#         / provincial_share['first_year_total_interest']
# )
# provincial_share['province_order'] = provincial_share['area'].map(area_code_order)
# provincial_share.to_clipboard()

# -------------------- OUTPUT 4
# market_share_bravo_vs_rest_interest_by_year_and_area = sealy_single_search_3y_results.groupby(
#     ['year', 'area', 'super_brand'],
#     as_index=False
# )['interest'].sum().sort_values(['area', 'super_brand', 'year'])
#
# market_share_bravo_vs_rest_interest_by_year_and_area['year_total_interest'] = market_share_bravo_vs_rest_interest_by_year_and_area.groupby(
#     ['area', 'year']
# )['interest'].transform('sum')
#
# market_share_bravo_vs_rest_interest_by_year_and_area['index_interest'] = (
#         market_share_bravo_vs_rest_interest_by_year_and_area['interest']
#         / market_share_bravo_vs_rest_interest_by_year_and_area['year_total_interest']
# )
# market_share_bravo_vs_rest_interest_by_year_and_area.to_clipboard()


# ---------------------- OUTPUT 5

# provincial_bravo_vs_rest = sealy_single_search_3y_results.loc[
#     (sealy_single_search_3y_results['area'] != 'National')
#     & (sealy_single_search_3y_results['area'].isin(big_areas))
#     ].groupby(
#     ['area', 'year', 'super_brand'],
#     as_index=False
# )['interest'].sum().sort_values(['year', 'area', 'super_brand'])
#
# provincial_bravo_vs_rest['first_year_total_interest'] = provincial_bravo_vs_rest.groupby(
#     ['area', 'super_brand']
# )['interest'].transform('first')
#
# provincial_bravo_vs_rest['index_interest'] = (
#         provincial_bravo_vs_rest['interest']
#         / provincial_bravo_vs_rest['first_year_total_interest']
# )
# provincial_bravo_vs_rest.to_clipboard()


# OUTPUT 6 - benchmarking
sealy_benchmark = pd.read_csv('data/sealy_single_search_5y_results.csv')

sealy_benchmark['brand_dirty'] = sealy_benchmark['brand']
sealy_benchmark['brand'] = sealy_benchmark['brand'].map(
    lambda x: brand_remapping.get(x, x))

benchmarked_brands = sealy_benchmark.groupby(
    ['brand', 'area', 'year'],
    as_index=False
)['interest'].sum().sort_values(['brand', 'area', 'year'])

benchmarked_brands['first_interest_brand_area'] = benchmarked_brands.groupby(
    ['brand', 'area']
)['interest'].transform('first')

benchmarked_brands['total_interest_area_year'] = benchmarked_brands.groupby(
    ['area', 'year']
)['interest'].transform('sum')

benchmarked_brands['index_interest'] = (
        benchmarked_brands['interest'] / benchmarked_brands['first_interest_brand_area']
)
benchmarked_brands['proportion_of_interest'] = (
        benchmarked_brands['interest'] / benchmarked_brands['total_interest_area_year']
)

brand_rank = benchmarked_brands[
    (benchmarked_brands['year'] == 2021)
    & (benchmarked_brands['area'] == 'ZA')
    ]
brand_rank['rank'] = brand_rank['interest'].rank(method='dense', ascending=False)

brand_to_rank = brand_rank.set_index('brand')['rank'].to_dict()

benchmarked_brands['brand_rank'] = benchmarked_brands['brand'].map(brand_to_rank)

benchmarked_brands.to_clipboard()

benchmarked_brands[(benchmarked_brands['area'] == 'ZA') & (benchmarked_brands['year'] == 2021)].sort_values(
    'interest')

# OUTPUT - data time information
national_date_time = sealy_single_search_3y_results.loc[
    sealy_single_search_3y_results['area'] == 'National'
    ]
national_date_time['month_name'] = pd.to_datetime(national_date_time['date']).dt.month_name()
national_date_time['month_abbreviation'] = pd.to_datetime(national_date_time['date']).dt.month_name().str.slice(0, 3)
national_date_time['year_abbreviation'] = national_date_time['year'].astype(str).str.slice(2, 4)
national_date_time['year_month_shorthand'] = (
        "'"
        + national_date_time['year_abbreviation']
        + "-"
        + national_date_time['month_abbreviation']
)
year_month_interest = national_date_time.groupby(
    ['year', 'month', 'month_name', 'year_month_shorthand']
    , as_index=False
)['interest'].sum().sort_values(['year', 'month'])
year_month_interest['interest_index'] = year_month_interest['interest'] / year_month_interest['interest'].max()
year_month_interest.to_clipboard()

month_interest = national_date_time.groupby(
    ['month', 'month_name']
    , as_index=False
)['interest'].sum().sort_values(['month'])

month_interest['interest_index'] = month_interest['interest'] / month_interest['interest'].max()
month_interest.to_clipboard()
