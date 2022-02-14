import os
import pandas as pd

# detect current file path
path = os.path.dirname(os.path.abspath(__file__))

# define the location of the enron dataset
enron_file = os.path.join(path, 'data/enron_postgres')

# read emails table
emails_table = os.path.join(enron_file, 'emails.csv')
emails_df = pd.read_csv(emails_table, encoding="utf-8", sep=",", header=0)

# check if email_id is unique
print(emails_df.email_message_id.nunique())
