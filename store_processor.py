import pandas as pd

single_search_3y_results = pd.read_csv('data/stores_search_5y_results.csv')
ok_single_search_3y_results = pd.read_csv('data/ok_comparison_search_5y_results.csv')

yearly_stats = ok_single_search_3y_results.groupby(
    ['brand', 'area', 'year'],
    as_index=False
)['interest'].sum().sort_values(['brand', 'area', 'year'])

yearly_stats['first_year_total_interest'] = yearly_stats.groupby(
    ['brand', 'area']
)['interest'].transform('first')

yearly_stats['index_interest'] = (
        yearly_stats['interest']
        / yearly_stats['first_year_total_interest']
)



store_to_interest = yearly_stats.groupby(
    'brand'
)['interest'].sum().rank(
    method='dense', ascending=False
).sort_values().to_dict()

yearly_stats['store_rank'] = yearly_stats['brand'].map(store_to_interest)


yearly_stats.to_clipboard()
