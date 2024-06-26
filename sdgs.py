import dimcli
import numpy as np
import pandas as pd

import json
import os

# Set search parameters
YEAR: int = 2022
GRIDID: str = 'grid.6268.a'

# Set paths
# HOME_DIR: str = os.path.dirname(os.getcwd())
HOME_DIR: str = os.getcwd()
DATA_DIR: str = os.path.join(HOME_DIR, 'data')
    
# Log into Dimensions and get data
dimcli.login()
dsl = dimcli.Dsl()

df_publications = dsl.query_iterative(f"""search publications 
                                      where research_orgs = "{GRIDID}"
                                      and year = "{YEAR}"
                                      return publications[id+doi+category_for_2020+category_sdg+document_type+field_citation_ratio+times_cited+year]
                                  """).as_dataframe()

df_publications = df_publications.rename(columns={'id': 'publication_id'})

# Data set is limited to research outputs only and excludes other types of publications
document_types = ['research_article', 'review_article', 'research_chapter', 'conference_paper']
df_publications['document_type'] = df_publications['document_type'].str.lower()
df_publications = df_publications[df_publications['document_type'].isin(document_types)]

# Useful to know proportion of papers with assigned sdg so it's necessary to know the total number of publications
total_publications = df_publications['publication_id'].nunique()

# Limit the data set to publications allocated to an SDG
df_publications = df_publications[df_publications['category_sdg'].notnull()]

# Reshape the data
df_publications = pd.json_normalize(json.loads(df_publications.to_json(orient='records')),
                                    record_path=['category_sdg'],
                                    meta=['publication_id', 'category_for_2020', 'document_type', 'doi', 'field_citation_ratio', 'times_cited', 'year'],
                                    record_prefix='sdg_',
                                    errors='ignore')
#df_publications = df_publications.drop(columns=['sdg_id'])
df_publications[['sdg_code','sdg_name']] = df_publications['sdg_name'].str.split(pat=' ', n=1, expand=True)

df_publications = pd.json_normalize(json.loads(df_publications.to_json(orient='records')),
                                    record_path=['category_for_2020'],
                                    meta=['publication_id', 'document_type', 'doi', 'field_citation_ratio', 'times_cited', 'year', 'sdg_name', 'sdg_code'],
                                    record_prefix='for_',
                                    errors='ignore')
df_publications = df_publications.drop(columns=['for_id'])
df_publications[['for_code','for_name']] = df_publications['for_name'].str.split(pat=' ', n=1, expand=True)

df_publications = df_publications[[col for col in df_publications.columns if col != 'for_name'] + ['for_name']]

# Outputs
output_csv: str = ''.join(['sdg_publications_', str(YEAR), '.csv'])
output_txt: str = ''.join(['total_publications_', str(YEAR), '.txt'])

with open(os.path.join(DATA_DIR, output_txt), 'w', encoding='utf-8') as text_file:
    text_file.write(f'total number of publications in {YEAR}: {total_publications}')

df_publications.to_csv(os.path.join(DATA_DIR, output_csv), index=False, encoding='utf-8')