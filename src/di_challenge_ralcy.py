##############################################################
#                                                            #
#  Program filename: di_challenge_ralcy.py                   #
#                                                            #
#  language: Python (v3.6.3)                                 #
#  last revised: 10/31/2017                                  #
#                                                            #
##############################################################
#                             #
#  Author:  Ralcy Velazco     #
#  E-mail:  ralcyv@gmail.com  #
#                             # 
###############################


import pandas as pd
import numpy as np


###############################################################################################################################
# Read input and generate dataframe containing all data required for median vaules by zip/date output files
# Said data fame contians the following columns: ['CMTE_ID','ZIP_CODE','TRANSACTION_DT','TRANSACTION_AMT','OTHER_ID']
###############################################################################################################################


df_data=pd.read_csv('../input/itcont.txt',sep="|",usecols=[0,10,13,14,15],lineterminator='\n',header=None,dtype={10:'str',13:'str'})

# Assign column indexs, respectively, to the column names below
df_data.columns = ['CMTE_ID','ZIP_CODE','TRANSACTION_DT','TRANSACTION_AMT','OTHER_ID']

# DataFrame remove rows that contain contributer IDs then delete this row that will go unutilized
df_data = df_data[pd.isnull(df_data['OTHER_ID'])]
del df_data['OTHER_ID']

# DataFrame remove rows that do not contain a contributer ID
df_data = df_data[pd.notnull(df_data['CMTE_ID'])]

# DataFrame remove rows that do not contain a transaction amount
df_data = df_data[pd.notnull(df_data['TRANSACTION_AMT'])]


###############################################################################################################################
# Creation of medianvals_by_zip DataFrame & medianvals_by_zip.txt file
# Contains: CMTE_ID|ZIP_CODE|MEDIAN_TRANSACTIONS_BY_ZIP per CMTE_ID|NUM_TRANSACTIONS_BY_ZIP per CMTE_ID|SUM_TRANSACTIONS_BY_ZIP
###############################################################################################################################


# Creates the DataFrame that will become the medianvals_by_zip DataFrame
df_zdata1 = df_data.filter(['CMTE_ID','ZIP_CODE'], axis=1)

# DataFrame Drop malformed Zip Codes and only first 5 digits only
mask = (df_data['ZIP_CODE'].str.len() == 5) | (df_data['ZIP_CODE'].str.len() == 9)
df_zdata1 = df_data.loc[mask]
pd.options.mode.chained_assignment = None  # default='warn'
df_zdata1['ZIP_CODE'] = df_zdata1['ZIP_CODE'].apply('{:0>5}'.format).str[:5].astype(np.str)
df_zdata1['ZIP_CODE'] = pd.to_numeric(df_zdata1['ZIP_CODE'], errors='coerce')
df_zdata1 = df_zdata1[pd.notnull(df_zdata1['ZIP_CODE'])]
df_zdata1['ZIP_CODE'] = df_zdata1['ZIP_CODE'].astype(np.int)
df_zdata1['ZIP_CODE'] = df_zdata1['ZIP_CODE'].apply('{:0>5}'.format).str[:5].astype(np.str)
# Creates MEDIAN_TRANSACTIONS_BY_ZIP Column of the 'CMTE_ID'&'ZIP_CODE'
# Set to Zero here so that MEDIAN_TRANSACTIONS_BY_ZIP is in the correct location
df_zdata1['MEDIAN_TRANSACTIONS_BY_ZIP'] = 0

# Creates NUM_TRANSACTIONS_BY_ZIP Column of the 'CMTE_ID'&'ZIP_CODE'
df_zdata1['NUM_TRANSACTIONS_BY_ZIP'] = df_zdata1.groupby(['CMTE_ID','ZIP_CODE']).cumcount()+1

# Creates SUM_TRANSACTIONS_BY_ZIP column of the 'CMTE_ID'&'ZIP_CODE'
df_zdata1['SUM_TRANSACTIONS_BY_ZIP'] = df_data.groupby(['CMTE_ID','ZIP_CODE']).cumsum()

# Creates MEDIAN_TRANSACTIONS_BY_ZIP Column of the 'CMTE_ID'&'ZIP_CODE'
df_zdata1['MEDIAN_TRANSACTIONS_BY_ZIP'] = df_zdata1['SUM_TRANSACTIONS_BY_ZIP']/df_zdata1['NUM_TRANSACTIONS_BY_ZIP']
df_zdata1.MEDIAN_TRANSACTIONS_BY_ZIP = df_zdata1.MEDIAN_TRANSACTIONS_BY_ZIP.round().apply(int)

# Creates medianvals_by_zip_df
medianvals_by_zip_df = df_zdata1

# Outputs medianvals_by_zip_txt to the current working directory
medianvals_by_zip_df.to_csv('../output/medianvals_by_zip.txt', sep='|', header=None, index=False, line_terminator='\n')


########################################################################################################################################
# Creation of medianvals_by_date DataFrame & medianvals_by_date.txt file
# Contains: CMTE_ID|TRANSACTION_DT|MEDIAN_TRANSACTIONS_BY_DATE per CMTE_ID|NUM_TRANSACTIONS_BY_DATE per CMTE_ID|SUM_TRANSACTIONS_BY_DATE
########################################################################################################################################


# Creates NUM_TRANSACTIONS_BY_DATE Column of the 'CMTE_ID'&'TRANSACTION_DT'
df_ddata1 = df_data.groupby(['CMTE_ID','TRANSACTION_DT']).size().reset_index(name='NUM_TRANSACTIONS_BY_DATE')
df_ddata1.set_index(['CMTE_ID','TRANSACTION_DT'],inplace=True)

# Creates SUM_TRANSACTIONS_BY_DATE column of the 'CMTE_ID'&'TRANSACTION_DT'
df_ddata2 = df_data.groupby(['CMTE_ID','TRANSACTION_DT'])['TRANSACTION_AMT'].sum().reset_index(name='SUM_TRANSACTIONS_BY_DATE')
df_ddata2 = df_ddata2
df_ddata2.set_index(['CMTE_ID','TRANSACTION_DT'],inplace=True)

# Creates MEDIAN_TRANSACTIONS_BY_DATE Column of the 'CMTE_ID'&'TRANSACTION_DT'
df_ddata3 = df_data.groupby(['CMTE_ID','TRANSACTION_DT'])['TRANSACTION_AMT'].median().reset_index(name='MEDIAN_TRANSACTIONS_BY_DATE')
df_ddata3.MEDIAN_TRANSACTIONS_BY_DATE = df_ddata3.MEDIAN_TRANSACTIONS_BY_DATE.round().apply(int)
df_ddata3 = df_ddata3
df_ddata3.set_index(['CMTE_ID','TRANSACTION_DT'],inplace=True)

# Creates medianvals_by_date_df
medianvals_by_date_df = df_ddata3.join(df_ddata1).join(df_ddata2)
medianvals_by_date_df = medianvals_by_date_df.reset_index()

medianvals_by_date_df.sort_values(['CMTE_ID','TRANSACTION_DT'], ascending=[True, True])

# Outputs medianvals_by_date_txt to the current working directory
medianvals_by_date_df.to_csv('../output/medianvals_by_date.txt', sep='|', header=None, index=False, line_terminator='\n')
