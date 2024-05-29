# Adapted from https://api-lab.dimensions.ai/cookbooks/8-organizations/7-benchmarking-organizations.html

import dimcli
import pandas as pd

import os

# Set search parameters
YEAR: int = 2022

# Set paths
HOME_DIR: str = os.path.dirname(os.getcwd())
DATA_DIR: str = os.path.join(HOME_DIR, 'data')
    
# Log into Dimensions and get data
dimcli.login()
dsl = dimcli.Dsl()

result_sdg = dsl.query(f"""
                    search publications
                    where year = {YEAR}
                    return category_sdg limit 1000""").as_dataframe()

result_sdg['cutoff'] = (result_sdg['count'] * .01).astype('int')

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

# Tidy up the data and export
cutoffs = pd.concat(dfl)
cutoffs[['sdg_code','sdg_name']] = cutoffs['name'].str.split(pat=' ', n=1, expand=True)
cutoffs = cutoffs.drop(columns=['id', 'name'])
cutoffs = cutoffs[[col for col in cutoffs.columns if col != 'field_citation_ratio'] + ['field_citation_ratio']]

outfile = ''.join(['sdg_benchmarks_', str(YEAR), '.csv'])
cutoffs.to_csv(os.path.join(DATA_DIR, outfile), index=False)