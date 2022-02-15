import os
import pandas as pd

# detect current file path
path = os.path.dirname(os.path.abspath(__file__))

# define the location of the enron dataset
enron_file = os.path.join(path, 'data/enron_postgres')

# read users table
users_table = os.path.join(enron_file, 'users.csv')
users_df = pd.read_csv(users_table, encoding="utf-8")

# count number of unique emails vs number of all emails
print(users_df['user_email'].nunique() / len(users_df['user_email']))

# drop duplicates emails
users_df = users_df.drop_duplicates(subset=['user_email'])

# export to csv
users_df.to_csv(os.path.join(enron_file, 'unique_users.csv'), index=False)