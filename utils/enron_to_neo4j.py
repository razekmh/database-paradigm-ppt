'''
create a neo4j input file from the enron dataset
'''

# import libraries
import pathlib
import pandas as pd

# find path to this file
path = pathlib.Path(__file__).parent.absolute()

# define the path to the enron dataset
enron_pg_path = pathlib.Path(path, pathlib.Path('data/enron_postgres'))

# define the path to the neo4j file
enron_neo4j_path = pathlib.Path(path, pathlib.Path('data/enron_neo4j'))

# read users csv
df_users = pd.read_csv(enron_pg_path / 'unique_users_with_names.csv')

# read emails csv
df_emails = pd.read_csv(enron_pg_path / 'emails.csv', usecols=['email_message_id', 'email_date', 'email_subject'])

# read transactions csv
df_transactions = pd.read_csv(enron_pg_path / 'unique_email_users.csv')

# create user nodes
with open (enron_neo4j_path / 'users_emails.txt', 'w') as users_file:
    users_file.write("CREATE (user_000000:Person);" + '\n')
    for index, row in df_users.iterrows():
        user_id = "user_" + str(row['user_id']).zfill(6)
        users_file.write(f"CREATE ({user_id}:Person {{email_address:'{row['user_email']}'")
        users_file.write(f",user_id:'{user_id}'")
        if row['first_name'] != 'None':
            users_file.write(f", first_name:'{row['first_name']}'")
        if row['last_name'] != 'None':
            users_file.write(f", last_name:'{row['last_name']}'")
        if not pd.isna(row['rank']):
            users_file.write(f", rank:'{row['rank'].lower()}'")
        if not pd.isna(row['role']):
            users_file.write(f", role:'{row['role'].lower()}'")
        if not pd.isna(row['company']):
            users_file.write(f", company:'{row['company'].lower()}'")
        users_file.write("})\n")

# create email relationships
df_relationships = pd.merge(df_transactions, df_emails, on='email_message_id', how='left')

with open (enron_neo4j_path / 'users_emails.txt', 'a') as emails_file:
    for index, row in df_relationships.iterrows():
        sender_id = "user_" + str(row['sender']).zfill(6)
        receiver_id = "user_" + str(row['receiver']).zfill(6)
        emails_file.write(f"CREATE ({sender_id})-[:SENT_TO{{message_id:'{row['email_message_id']}', subject:'{row['email_subject']}', date:'{row['email_date']}', type: '{row['transaction_type']}', routing: '{row['external_or_internal']}'}}]->({receiver_id})\n")
    emails_file.write(";")