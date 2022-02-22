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

df = pd.merge(df_users, df_roles, left_on='email_stem', right_on='email', how='left')
df.to_csv(output_path, sep=',', index=False)