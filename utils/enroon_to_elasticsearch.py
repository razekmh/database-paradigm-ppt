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

# set counter for the records
counter = 0 

# writer function
def write_line(line):
    '''
    writes a line to the output file
    '''
    with open(output_path, 'a') as f:
        creator_line = f'{{"create": {{"_index": "enron_emails", "_type": "email", "_id": "{line["email_message_id"]}"}} }}\n'
        f.write(creator_line)
        f.write(json.dumps(line))
        f.write('\n')

# read in json file line by line
with open(input_path, 'r') as f:
    for line in f:
        if counter < 5000:
            # convert line to json
            email = json.loads(line)
            write_line(email)
            counter += 1