import dimcli
import pandas as pd

import os

YEAR: int = 2022

result_sdg = dsl.query(f"""
                    search publications
                    where year = {YEAR}
                    return category_sdg limit 1000""").as_dataframe()

result_sdg['level'] = result_sdg.name.apply(lambda n: len(n.split(' ')[0]))
# result = result.assign(year = YEAR)
# result = result[['year'] + [x for x in result.columns if x != 'year']]

result_sdg['cutoff'] = (result_sdg['count'] * .01)#.astype('int')

dfl = []

for r in result_sdg.iterrows():

    result = dsl.query(f"""

           search publications
           where category_sdg.id = "{r[1]['id']}"
           and year = {YEAR}
           return publications[field_citation_ratio]
               sort by field_citation_ratio
               limit 1
               skip {r[1]['cutoff']}

      """).as_dataframe()

    result['name'] = r[1]['name']
    result['id'] = r[1]['id']
    dfl.append(result)
    
cutoffs = pd.concat(dfl)
cutoffs.to_csv('1_percent_cutoffs_sdg.csv', index=False)