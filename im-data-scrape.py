import pandas as pd
import os
import re
from pandas.api.types import CategoricalDtype

path = "C:/Users/hsweat/Python Notebooks/IM Data Scrape/Data1"
program_name = '14A Complete Op2'

#assign variables
df=pd.DataFrame()
os.chdir(path)
master_dataframe_foldername = 'Master_df'
master_dataframe_filename = 'Master_df.csv'
master_cpk_dataframe_filename = 'Master_cpk_df.csv'

#get list of relevant files
all_files = os.listdir()
files = []
for f in all_files:
    #get part name using regex
    text = f
    m = re.search(f'({program_name})', text)
    if m:
        file_name = m.group(1)
        files.append(f)

#print(files)

#put all relevant files into one dataframe
i = 0
for f in files:
    if i == 0:
        #create dfSpec dataframe using first file only
        dfSpec = pd.read_csv('20210420_14A Complete Op2 (-2 THOU) REV6_200838-211Opti Comparator.csv',nrows = 3)

        del dfSpec['Measurement time']
        del dfSpec['Lot No.']
        del dfSpec['Serial Counter']
        del dfSpec['Judgment']
        del dfSpec['Name']
        del dfSpec['Number ']

        dfSpec = dfSpec.transpose()

        new_header = list(dfSpec.iloc[0]) #grab the first row for the header
        #new_header.insert(0,'Characteristic')
        dfSpec = dfSpec[1:] #take the data less the header row
        dfSpec.columns = new_header #set the header row as the df header

        dfSpec = dfSpec.reset_index()
        dfSpec.rename(columns={ dfSpec.columns[0]: "Characteristic" }, inplace = True)
        dfSpec.rename(columns={ dfSpec.columns[1]: "Nominal" }, inplace = True)
        dfSpec['USL'] = dfSpec['Nominal'] + dfSpec['Upper Limit']
        dfSpec['LSL'] = dfSpec['Nominal'] + dfSpec['Lower Limit']
        del dfSpec['Upper Limit']
        del dfSpec['Lower Limit']

    dfCSV = pd.read_csv(f)
    dfCSV = dfCSV.drop([0,1,2])
    if i == 0:
        new_headers = dfCSV.columns
        
    print(dfCSV.columns)
    dfCSV.columns = new_headers
    print(dfCSV.columns)
    print(len(dfCSV))
    del dfCSV['Number ']
    dfCSV = pd.melt(dfCSV, id_vars =['Program name','Measurement time','Lot No.','Serial Counter','Judgment','Name'],var_name ='Characteristic',value_name ='Actual')
    #dfCSV = dfCSV.sort_values('Characteristic')
    #dfCSV = dfCSV.sort_values('Serial Counter')
    dfCSV.reset_index(inplace = True, drop = True)
    dfCSV['Actual'] = dfCSV['Actual'].astype(float)
    df = df.append(dfCSV, ignore_index = True)

    i += 1

#categorical sort based on Characteristic
sort = list(df['Characteristic'].unique())
char_order = CategoricalDtype(
    sort,
    ordered=True
)
df['Characteristic'] = df['Characteristic'].astype(char_order)
df = df.sort_values('Characteristic')

#add dfSpec information to df
column_names = list(dfSpec['Characteristic'])

nom_col = []
LSL_col = []
USL_col = []

i = 0
for f in column_names:
    #get values
    nominal = float(dfSpec.at[i,'Nominal'])
    LSL = float(dfSpec.at[i,'LSL'])
    USL = float(dfSpec.at[i,'USL'])
    #assign to list
    nom_col_piece = [nominal] * len(df[df['Characteristic'] == f])
    LSL_col_piece = [LSL] * len(df[df['Characteristic'] == f])
    USL_col_piece = [USL] * len(df[df['Characteristic'] == f])
    #add to ongoing list
    nom_col += nom_col_piece
    LSL_col += LSL_col_piece
    USL_col += USL_col_piece
    i += 1

df.insert(loc = len(df.columns), column = 'Nominal', value = nom_col)
df.insert(loc = len(df.columns), column = 'LSL', value = LSL_col)
df.insert(loc = len(df.columns), column = 'USL', value = USL_col)

#Split Lot No. in to Work Order and Work Position
df[['Work Order','Work Position']] = df['Lot No.'].str.split('-',expand=True)
df["Work Order"] = pd.to_numeric(df["Work Order"])
df["Work Position"] = pd.to_numeric(df["Work Position"])

#add part # column
part_num = '14A'
new_col = [part_num] * len(df)
df.insert(loc = 0, column = 'Part #', value = new_col)

save_filepath = f"{path}/{master_dataframe_foldername}/{master_dataframe_filename}"
df.to_csv(save_filepath, index = False)

    
#create Master_cpk_df

cpk_df = df

customer = []
part = []
feature = []
mu = []
sigma = []
nominal = []
USL = []
LSL = []
CP = []
CPK = []

for f in cpk_df['Part #'].unique():
    temp_df = df[df['Part #'] == f]
    unique_features = list(temp_df['Characteristic'].unique())

    for u in unique_features:
        temp_df2 = temp_df[temp_df['Characteristic'] == u]
        customer.append('SMTC')
        part.append(f)
        feature.append(u)
        mu.append(temp_df2['Actual'].mean())
        sigma.append(temp_df2['Actual'].std())
        nominal.append(temp_df2.iloc[0]['Nominal'])
        USL.append(temp_df2.iloc[0]['USL'])
        LSL.append(temp_df2.iloc[0]['LSL'])

        #calc CP and CPK
        mu_var = temp_df2['Actual'].mean()
        sigma_var = temp_df2['Actual'].std()
        USL_var = temp_df2.iloc[0]['USL']
        LSL_var = temp_df2.iloc[0]['LSL']
        CP_var = (USL_var - LSL_var)/(6 * sigma_var)
        #if statement for CPK to compensate for LSL of 0
        if LSL_var == 0:
            CPK_var = max((USL_var - mu_var)/(3 * sigma_var),(mu_var - LSL_var)/(3 * sigma_var))
        else:
            CPK_var = min((USL_var - mu_var)/(3 * sigma_var),(mu_var - LSL_var)/(3 * sigma_var))
        CP.append(CP_var)
        CPK.append(CPK_var)


cpk_df = pd.DataFrame()

cpk_df['Customer'] = customer
cpk_df['Part #'] = part
cpk_df['Feature'] = feature
cpk_df['Mean'] = mu
cpk_df['Std. Dev.'] = sigma
cpk_df['Nominal'] = nominal
cpk_df['USL'] = USL
cpk_df['LSL'] = LSL
cpk_df['CP'] = CP
cpk_df['CPK'] = CPK
cpk_df['CPK Inverse'] = 1 / cpk_df['CPK']

cpk_df = cpk_df.round(3)

save_cpk_filepath = f"{path}/{master_dataframe_foldername}/{master_cpk_dataframe_filename}"
cpk_df.to_csv(save_cpk_filepath, index = False)
