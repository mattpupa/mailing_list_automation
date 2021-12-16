#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec 12 23:28:22 2021

For this automation we are going to assume that some marketing/customer support
tool adds a new file labeled with the current date, to a folder in a directory.
Each day, we need to open that file, clean the data, and then split the file 
into multiple new files based on user input. The user will input how many 
records should be in each file.

For example, if the system adds a file with 100 records, and the user selects
25 records per file, this automation will produce 4 new files.

Once those new files are created, they will be saved in that same directory.

One added detail is that we need to exclude any data (we'll use
email address in this case) that matches a table in our database (bigquery).
We'll use the bigquery api to automate this.

@author: Matt
"""

# Import libraries
import pandas as pd
import numpy as np
import os
from os import listdir 
from os.path import isfile, join
from google.cloud import bigquery
from google.oauth2 import service_account

"""
Before we get started, we'll make the connection to our bigquery database
"""

# https://googleapis.dev/python/google-api-core/latest/auth.html
credentials = service_account.Credentials.from_service_account_file(
    'bigquerykey.json', scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

client = bigquery.Client(credentials=credentials, project=credentials.project_id,)


# Save the folder name to a variable
mailinglistfolder = '/Python/Madapup/Automations/'


# Because the file name in the folder will change each day because of the date
# we need import the file based on the starting characters
for file in listdir(mailinglistfolder):
    if isfile(join(mailinglistfolder, file)) and file.startswith('mailinglisttosplit'):
        global todays_file, df_mailinglist
        todays_file = file
        df_mailinglist = pd.read_csv(mailinglistfolder+todays_file, na_values= ' ')
        

# Since we only need a few columns from the data, we'll create a new dataframe
data_to_split = df_mailinglist[['Name', 'Location', 'Phone', 'Email', 'Website']]

# We need records with valid email addresses, so we'll first remove any blank values
data_to_split = data_to_split[data_to_split['Email'].notna()]

# Then, we'll remove any email addreses that may have incorrect values
data_to_split = data_to_split[~data_to_split['Email'].astype(str).str.startswith('https://')]
data_to_split = data_to_split[~data_to_split['Email'].astype(str).str.startswith('www.')]

"""
Now that we cleaned our data, we want to remove any records with email
addresses that already exist in our bigquery database
"""

# Get records from bigquery database and put them into a dataframe
bigquery_emails_dataframe = client.list_rows('datafaux-ab0de.datafaux_100K.mailing_list_emails').to_dataframe()

# Convert that to list
bigquery_emails_list = bigquery_emails_dataframe.values.tolist()

# List needs to be flattened since it's currently a list of lists
bigquery_emails_list_flat = []
for email_list in bigquery_emails_list:
    for email in email_list:
        bigquery_emails_list_flat.append(email)


# Remove matching emails  
# https://stackoverflow.com/questions/27965295/dropping-rows-from-dataframe-based-on-a-not-in-condition           
matching_email_records = data_to_split[data_to_split['Email'].isin(bigquery_emails_list_flat)]

# Remove matching emails
# https://stackoverflow.com/questions/18180763/set-difference-for-pandas 
data_to_split_matches_removed = pd.concat([data_to_split, matching_email_records, matching_email_records]).drop_duplicates(keep=False)


# add today's email addresses to bigquery database
data_to_add_to_bigquery = pd.DataFrame(data_to_split_matches_removed['Email'])
data_to_add_to_bigquery.rename({'Email': 'string_field_0'}, axis=1, inplace=True)

# Load email addresses from today's file into the bigquery database
# https://cloud.google.com/bigquery/docs/samples/bigquery-load-table-dataframe
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("string_field_0", bigquery.enums.SqlTypeNames.STRING)
    ])

job = client.load_table_from_dataframe(
    data_to_add_to_bigquery, 'datafaux-ab0de.datafaux_100K.mailing_list_emails', job_config=job_config
)  # Make an API request.
job.result()  # Wait for the job to complete.

table = client.get_table('mailing_list_emails')  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), 'mailing_list_emails'
    )
)


"""
We want to give a user the option to select how many records will be included
in each list. However, we need to make sure they enter a correct value. In this
case, we need a positive number above 0. 

If they give us a negative number or
an an input in the wrong format, we need to send them a message to input a
different number.

Here's a good solution from the web
# https://stackoverflow.com/questions/26198131/check-if-input-is-positive-integer

"""

# As long as the conditions we need are true (user enters a positive number
# above 0 as input), we can take that input and create the lists we need

while True:
    input_given = input("How many records per list?: ")
    try:
        records_per_list = int(input_given)
        if records_per_list < 0:  # if not a positive int print message and ask for input again
            print("Sorry, lists must have more than 0 records, try again")
            continue 
        break
    except ValueError:
        print("Please enter a valid number above 0")    
    
        
# else all is good, val is >=  0 and an integer
print("We'll get those lists for you ASAP!")

# We'll use numpy arange to save the data into groups of 20 records each
groups = data_to_split_matches_removed.groupby(np.arange(len(data_to_split_matches_removed.index))//records_per_list)

# We'll use a for loop to save each group as it's own CSV file and
# save it in the same folder


for (group_num, data) in groups:
    data.to_csv(f"mailing_list_{group_num}.csv")
    

    
os.remove(todays_file)





    
