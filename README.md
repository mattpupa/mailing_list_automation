# mailing_list_automation
Repository

A simple automation for a customer success email campaign. A data file with email address, website, etc. is taken from a separate system and is placed on a shared drive. This automation takes that data file, checks the emails against a bigquery table (via API) to remove any records that have already been emailed, then splits the data into a number of lists based on user input. The bigquery table is then updated with the new data.
