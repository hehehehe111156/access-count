import datetime
from genericpath import exists
import os
import re
import sys
import json


# 获取当前时间
def getCurrentTime():
    today = datetime.date.today()
    year = str(today.year)
    if today.month < 10:
        month = "0" + str(today.month)
    else:
        month = str(today.month)
    if today.day < 10:
        day = "0" + str(today.day)
    else:
        day = str(today.day)
    date = {'year': year, 'month': month, 'day': day}
    return date


def getVisistedCount():
    try:
        with open("logs.json", "r") as f:
            visited_files = json.load(f)
    except:
        visited_files = {}
    return visited_files


def accessCount(date = getCurrentTime(), rule = "specific-date"):
    if rule == "all":
        dest_file_path = "./cos-access-log/"
    else:
        year = date['year']
        month = date['month']
        day = date['day']
        dest_file_path = "./cos-access-log/" + year + "/" + month + "/" + day + "/"
    
    visited_files = getVisistedCount()
    for roots, dirs, files in os.walk(dest_file_path):
        for file in files:
            with open(os.path.join(roots, file), 'r') as f_read:
                for line in f_read:
                    line = line.split(' ')
                    if 'index.md' in line[11]:
                        if '"' in line[11]:
                            line[11] = eval(line[11])
                        if line[11] in visited_files.keys():
                            visited_files[line[11]] += 1
                        else:
                            visited_files[line[11]] = 1
    with open("logs.json", "w") as f:
        json.dump(visited_files, f, indent=4)


def main():
    accessCount(rule="all")


if __name__ == '__main__':
    main()