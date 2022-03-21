import pandas as pd
import numpy as np

np.set_printoptions(threshold=np.inf)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


cic_df = pd.read_csv('datasets/cic_full_dataset.csv', header = 'infer', sep = ',', low_memory = False)
https_df = pd.read_csv('datasets/samples.csv', header = 'infer', sep = ',', low_memory = False)
print('converting CIC dataset')
for col in cic_df:
    cic_df[col] = pd.to_numeric(cic_df[col], errors='coerce')

print('converting HTTPS dataset')
for col in https_df:
    https_df[col] = pd.to_numeric(https_df[col], errors='coerce')

if 'DURATION' in https_df.columns:
    https_df['DURATION'] = https_df['DURATION'].astype(int)

if 'Flow Duration' in cic_df.columns:
    cic_df['Flow Duration'] = cic_df['Flow Duration'].astype(int)
#contador = 0
#for index, row in cic_df.iterrows():
#    for indice, linha in https_df.iterrows():
#        if (row['Flow Pkts/s'] > ( 0.9 * linha['packets_per_sec']) and row['Flow Pkts/s'] < ( 1.1 * linha['packets_per_sec'])) and (row['Flow Duration'] > ( 0.8 * linha['DURATION']) and row['Flow Duration'] > ( 1.2 * linha['DURATION'])) and (row['Flow Byts/s'] > ( 0.8 * linha['bytes_per_sec']) and row['Flow Byts/s'] > ( 1.2 * linha['bytes_per_sec'])):
#            contador = contador + 1
#            print('Nummber of matches: ', contador)

cic_df = cic_df.sort_values(by = 'Flow Duration')
https_df = https_df.sort_values(by = 'DURATION')

intersection_df = pd.merge_asof(cic_df,https_df, left_by=['Flow Pkts/s','Tot Bwd Pkts', 'Flow Byts/s'], right_by=['packets_per_sec', 'PACKETS_REV', 'bytes_per_sec'], direction='nearest', tolerance=100, left_on = 'Flow Duration', right_on='DURATION')

intersection_df.to_csv('intersection.csv', sep=',')
#print(intersection_df.head())
