'''
converts enron json dump data to elasticsearch format
'''

# import modules
import json
import pathlib

# get current file path
path = pathlib.Path(__file__).parent.absolute()
print(path)

# set the input and output paths
input_path = pathlib.PurePath(path,'data/enron_emails.json')
output_path = pathlib.PurePath(path,'data/enron_es/enron_emails_elasticsearch.json')

# read in json file line by line
with open(input_path, 'r') as f:
    for line in f:
        # convert line to json
        email = json.loads(line)

        print (email)
        break