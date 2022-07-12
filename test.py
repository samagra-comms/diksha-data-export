import requests
import json
import logging
import time
import uuid
from deps.config.exhaust_config import config as exhaust_config

import re
import csv


path = '1ffc932f-1220-458d-9d9a-a5a377a42139_response_20220624.csv'
arr = []
with open(path, mode='r') as f:
    csv_file = csv.reader(f)
    for lines in csv_file:
        arr.append(lines)


for i in range(1, len(arr)):
    if arr[i][5] == 'mcq':
        lst = re.findall(r'text\\\":\\\"(.+?)\\', arr[i][11])
        lst = [x.strip() for x in lst]
        arr[i][11] = ';'.join(lst)
    else:
        arr[i][11] = ''
    print(arr[i][14])
    print()
