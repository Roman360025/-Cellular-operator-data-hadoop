import sys

import pandas as pd
from hdfs import InsecureClient
import pandas

client_hdfs = InsecureClient('http://localhost:9870')

general_df = None

for i in sys.stdin:
    i = i.rstrip()
    name_of_h = i.split('.')[1]
    file_name_read = '/new_files/' + i
    with client_hdfs.read(file_name_read, encoding='utf-8') as File:
        if general_df is None:
            general_df = pd.read_csv(File)
        else:
            df = pd.read_csv(File)
            general_df = pd.concat([df, general_df], axis = 0)
            # general_df = general_df.groupby(['Day', 'Time']).sum()

general_df = general_df.groupby(['Day', 'Time']).sum()
new_name_of_file = '/Result/' + name_of_h + '.csv'
with client_hdfs.write(new_name_of_file, encoding='utf-8') as File:
    general_df.to_csv(File)
