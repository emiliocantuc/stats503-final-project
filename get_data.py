# Script to download data for reproducibility

from urllib import request
import pandas as pd
import os

DATA_DIR = 'data'
BASE_URL = 'https://wwwn.cdc.gov/Nchs/Nhanes'

def download_dataset(year_block, dataset, url = None):
    """Downloads XPT dataset and converts it to CSV"""

    if url is None:
        url = f'{BASE_URL}/{year_block}/{dataset}.XPT'
    else:
        dataset = url.split('/')[-1].split('.')[0]

    print('\tRequesting', url)
    os.makedirs(DATA_DIR, exist_ok = True)
    request.urlretrieve(url, f'{DATA_DIR}/{dataset}.XPT')
    tmp = pd.read_sas(f'{DATA_DIR}/{dataset}.XPT')
    tmp['SEQN'] = tmp['SEQN'].astype(int)
    tmp.to_csv(f'{DATA_DIR}/{dataset}.csv', index = False)
    os.remove(f'{DATA_DIR}/{dataset}.XPT')
    return f'{DATA_DIR}/{dataset}.csv'


if __name__ == '__main__':

    # Read the variables file
    df = pd.read_csv('variables.csv')

    # dataset code -> name
    dataset_names = {}
    # dataset code -> list of (variable code, variable name)
    dataset_variables = {}

    for index, row in df.iterrows():
        code, name = row['Data Set'].strip(), row['Data Set Name'].strip()
        dataset_names[code] = name
        if code not in dataset_variables: dataset_variables[code] = [('SEQN', 'SEQN')]
        dataset_variables[code].append((row['Variable'].strip().upper(), row['Variable Common Name'].strip()))

    for i, (dataset, variables) in enumerate(dataset_variables.items()):

        print(f'Downloading dataset {i + 1}/{len(dataset_variables)}: {dataset_names[dataset]} ({dataset})')
        # Year block 2017-2018 corresponds to 2017-2020 Prepandemic data
        fpath = download_dataset('2017-2018', dataset)
        df = pd.read_csv(fpath)
        for j in variables: assert j[0] in df.columns, f'Variable {j[0]} not found in dataset {dataset_names[dataset]}'
        df = df[[i[0] for i in variables]]
        df.columns = [i[1] for i in variables]
        df.set_index('SEQN', inplace = True)
        df.to_csv(fpath)

    # Read every csv in data / and append it to the dataframe using SEQN as index
    print('Combining all datasets into a single CSV file ...')
    combined = None
    for fname in os.listdir(DATA_DIR):
        if 'csv' not in fname or 'combined' in fname: continue
        tmp = pd.read_csv(os.path.join(DATA_DIR, fname), index_col = 'SEQN')
        if combined is None:
            combined = tmp
        else:
            # combined = combined.merge(combined, on = 'SEQN', how = 'inner')
            combined = combined.join(tmp, how = 'inner')

    combined.to_csv(os.path.join(DATA_DIR, 'combined.csv'), index = False)

    