from hdfs import InsecureClient
import pandas as pd
import sys
import csv


def process_files(log_file):
    file_name_read = '/list_h/' + log_file
    file_name_write = '/new_files/' + log_file
    with client_hdfs.read(file_name_read, encoding='utf-8') as File:
        with client_hdfs.write(file_name_write, encoding='utf-8') as NewFile:
            day = 1
            reader = csv.reader(File, delimiter=',')
            writer = csv.writer(NewFile, delimiter=',')

            writer.writerow(["Day", 'Time', 'Speed'])
            next(reader)  # Пропускаем первую строку
            first_row = next(reader)
            first_minute, speed = int(float(first_row[1][1:]) / 60), float(
                first_row[2][1:])  # Получаем первый отсчёт и количество байт

            number_speeds_in_minute = 1

            for row in reader:
                minute = int(float(row[1][1:]) / 60)

                if int(row[0]) != day or minute != first_minute:
                    average_in_minute = speed / number_speeds_in_minute
                    writer.writerow([day, first_minute, average_in_minute])
                    first_minute = minute
                    day = int(row[0])
                    speed = 0
                    speed += float(row[2][1:])
                    number_speeds_in_minute = 1
                else:
                    speed += float(row[2][1:])
                    number_speeds_in_minute += 1

            average_in_minute = speed / number_speeds_in_minute
            writer.writerow([day, first_minute, average_in_minute])


def chunk_process(chunk):
    list(map(process_files, chunk))


def chunkIt(seq, num):
    avg = len(seq) / float(num)
    out = []
    last = 0.0

    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg

    return out


client_hdfs = InsecureClient('http://localhost:9870')

for i in sys.stdin:
    i = i.rstrip()
    process_files(i)
    print(i)
