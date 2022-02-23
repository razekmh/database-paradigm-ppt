import os
import pandas as pd

# detect current file path
path = os.path.dirname(os.path.abspath(__file__))

# define the location of the enron dataset
enron_file = os.path.join(path, 'data/enron_postgres')

# read users table
users_table = os.path.join(enron_file, 'unique_users.csv')
df_users = pd.read_csv(users_table, encoding="utf-8")
print("read users table")

# read email_users table
email_users_table = os.path.join(enron_file, 'email_users.csv')
df_email_users = pd.read_csv(email_users_table, encoding="utf-8")
print("read email_users table")


# count unique sender, receiver and email_id vs all transactions
print(df_email_users.duplicated(subset=['sender', 'receiver', 'email_message_id']).value_counts())

# drop duplicates
df_email_users = df_email_users.drop_duplicates(subset=['sender', 'receiver', 'email_message_id'])

# add external or internal flag
def is_external(row):
    if row['sender'] == 'None' or row['receiver'] == 'None' or '@' not in row['sender'] or '@' not in row['receiver']:
        return 'unknown'
    elif row['sender'].split('@')[1] != 'enron.com' or row['receiver'].split('@')[1] != 'enron.com':
        return 'external'
    else:
        return 'internal'

# add external or internal flag
df_email_users['external_or_internal'] = df_email_users.apply(is_external, axis=1)

# replace sender form email_users table with user_id from users table
df_email_users['sender'] = df_email_users['sender'].map(df_users.set_index('user_email')['user_id'])
df_email_users['receiver'] = df_email_users['receiver'].map(df_users.set_index('user_email')['user_id'])


# fill null values with 0
df_email_users['receiver'] = df_email_users['receiver'].fillna(0)

# print head to check
# print(df_email_users.head())

# strip whitespace from email_message_id
df_email_users['email_message_id'] = df_email_users['email_message_id'].str.strip()

# strip whitespace from type
df_email_users['transaction_type'] = df_email_users['transaction_type'].str.strip()

# export to csv with cast to int
df_email_users.astype({"receiver": "int32"}).to_csv(os.path.join(enron_file, 'unique_email_users.csv'), index=False)