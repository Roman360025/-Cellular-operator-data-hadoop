import matplotlib.pyplot as plt
import pandas as pd
from hdfs import InsecureClient

client_hdfs = InsecureClient('http://localhost:9870')

name_of_files = ['h31.csv', 'h55.csv', 'h86.csv', 'h80.csv']
file_name_read = '/Result/'

first_h = name_of_files.pop()
with client_hdfs.read(file_name_read + first_h, encoding='utf-8') as File:
    df = pd.read_csv(File)
    # df = df.pivot_table(values='Speed', index='Day', aggfunc='median')
    print(df)
    df = df.groupby(['Day'])['Speed'].median()
    print(df)
    # print(df.columns)
    ax = df.plot(x='Day', y='Speed', marker='o', label=first_h)


for i in name_of_files:
    with client_hdfs.read(file_name_read + i, encoding='utf-8') as File:
        df = pd.read_csv(File)
        df = df.groupby(['Day'])['Speed'].median()
        df.plot(x='Day', y='Speed', marker='o', label=i, ax=ax)

# plt.show()
plt.title('Скорости и ошибки в одни и те же моменты времени')
plt.xlabel('Скорости')
plt.ylabel('Ошибки')
plt.legend()
plt.savefig('result.png')
