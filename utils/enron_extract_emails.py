'''extract the emails and their attributes from the enron dataset'''

# import modules
import json
import os
import re
from email import message_from_string

def list_folders(parent_folder):
    # list the folders in the parent folder
    onlyfolder = [os.path.join(parent_folder, f) for f in os.listdir(parent_folder) if not os.path.isfile(os.path.join(parent_folder, f))]
    return onlyfolder

def list_files(parent_folder):
    # list the files in the parent folder
    onlyfiles = [os.path.join(parent_folder, f) for f in os.listdir(parent_folder) if os.path.isfile(os.path.join(parent_folder, f))]
    return onlyfiles

def clean_email_attr(email_attr):
    # clean the email attributes
    # takes in a string of email attributes and resturns a list of cleaned email attributes
    cleaned_email = re.findall(r'[\w.+-]+@[\w-]+\.[\w.-]+', email_attr)
    return cleaned_email

def clean_subject_body(email_attr):
    # clean the email attributes
    # takes in a string of email attributes and resturns a list of cleaned email attributes
    cleaned_email = re.sub('\t|\n|\r| +',' ', email_attr)
    cleaned_email = re.sub('"','\'', cleaned_email)
    return cleaned_email

def merge_attr_and_x(email_attr, email_attr_x):
    # merge the email attributes and X-attributes
    # takes in a string of email attributes and a string of X-attributes
    # returns a list of merged email attributes
    if email_attr is None:
        return None
    if email_attr_x is None:
        return ', '.join(clean_email_attr(email_attr))
    email_attr = clean_email_attr(email_attr)
    email_attr_x = clean_email_attr(email_attr_x)
    merged_email = [x for x in email_attr if not x.startswith('.')] + email_attr_x
    return ' ,'.join(merged_email)

def extract_email_data(main_folder, file_path, sub_folder=None):
    # extract the email data from the file
    with open(file_path, encoding = "ISO-8859-1") as f:
            print("file read: " + file)
            # read the file
            content = f.read()
            # parse the email data
            parsed_mail = message_from_string(content)
            # extract the email message id
            email_message_id = parsed_mail['Message-ID']
            # extract the email date
            email_date = parsed_mail['Date']
            # extract the email from
            email_from = parsed_mail['From']
            # extract the email to
            email_to = parsed_mail['To']
            # extract the email subject
            email_subject = parsed_mail['Subject']
            # extract the email cc
            email_cc = parsed_mail['Cc']
            # extract the email bcc
            email_bcc = parsed_mail['Bcc']
            # extract X-To
            email_x_to = parsed_mail['X-To']
            # extract X-cc
            email_x_cc = parsed_mail['X-cc']
            # extract X-bcc
            email_x_bcc = parsed_mail['X-bcc']
            # extract the email body
            email_body = parsed_mail.get_payload().replace('"','\'')



            # create a dictionary to store the email attributes
            email_dict = {}
            # add the email attributes to the dictionary
            email_dict['email_message_id'] = email_message_id
            email_dict['email_date'] = email_date
            email_dict['email_from'] = email_from
            email_dict['email_to'] = merge_attr_and_x(email_to, email_x_to)
            email_dict['email_subject'] = clean_subject_body(email_subject)
            email_dict['email_cc'] = merge_attr_and_x(email_cc, email_x_cc)
            email_dict['email_bcc'] = merge_attr_and_x(email_bcc, email_x_bcc)
            email_dict['email_body'] = clean_subject_body(email_body)
            email_dict['main_folder'] = main_folder
            email_dict['sub_folder'] = sub_folder
            
            # write the dictionary to the output file
            with open(out_file, 'a', encoding="utf-8") as outfile:
                json.dump(email_dict, outfile)
                outfile.write('\n')

# detect current file path
path = os.path.dirname(os.path.abspath(__file__))

# define the input and output file paths
in_folder = os.path.join(path, 'data/enron')

# define the path to the output file
out_file = os.path.join(os.path.join(path, 'data'), 'enron_emails.json')

# extract list of folders in the enron dataset
enron_folders = list_folders(in_folder)


for folder in enron_folders:
    # print(f"Processing folder: {folder}")
    # extract list of files in each folder
    enron_files = list_files(folder)
    
    # get main folder name
    main_folder = (folder.split('/')[-1])

    # extract emails in the main folder 
    for file in enron_files:
        extract_email_data(main_folder, file)
    
    # extract emails in the sub folders
    sub_folders = list_folders(folder)

    for sub_folder in sub_folders:
        # print(f"Processing sub folder: {sub_folder}")
        # extract list of files in each sub folder
        sub_files = list_files(sub_folder)
        
        # get sub folder name
        sub_folder = (sub_folder.split('/')[-1])

        # extract emails in the sub folder
        for file in sub_files:
            extract_email_data(main_folder, file, sub_folder)

