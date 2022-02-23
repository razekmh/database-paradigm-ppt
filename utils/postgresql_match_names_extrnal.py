'''
match names from enron roles file with user emails from enron emails file
'''

# import modules
import pathlib
import pandas as pd
import re

# get current file path
path = pathlib.Path(__file__).parent.absolute()

# set the input paths
names_path = pathlib.PurePath(path,'data/roles.txt')
users_path = pathlib.PurePath(path,'data/enron_postgres/unique_users.csv')

# set the output paths
output_path = pathlib.PurePath(path,'data/enron_postgres/unique_users_with_names.csv')

# read in roles file
df_roles = pd.read_csv(names_path, sep='\t', header=None, names=['email', 'name', 'rank', 'role'], index_col='email')

# read the unique users file
df_users = pd.read_csv(users_path, sep=',', header=0)
df_users['email_stem'] = df_users['user_email'].str.lower().str.replace(r'@.*', '').str.strip()

# merge the two dataframes
df = pd.merge(df_users, df_roles, left_on='email_stem', right_on='email', how='left')

# modify the name columns based on the new name column
def get_name(row):
    if row['first_name'] == 'None' and row['last_name'] == 'None' and row['name'] is not pd.np.nan and len(row['name'].split()) > 1:
        row['first_name'] = row['name'].split(' ')[0].strip().lower()
        row['last_name'] = row['name'].split(' ')[1].strip().lower()
    return row

# get the company email domain
def get_company(row):
    try:
        row['company'] = ('.').join(row['user_email'].split('@')[1].strip().lower().split('.')[0:-1])
    except IndexError:
        print(row['user_email'])
    return row

# apply the enriching functions to the dataframe
df = df.apply(get_name, axis=1)
df = df.apply(get_company, axis=1)

df.to_csv(output_path, columns= ["user_id","user_email","first_name","last_name","rank","role","company"],sep=',', index=False)