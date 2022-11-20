import argparse
import datetime
import re
import json
import os
import pandas as pd

parser = argparse.ArgumentParser(description='Обработка логов.')
parser.add_argument('--path', dest='path', action='store',
                    default='/Users/ekaterinavoloskan/Downloads/drivers/access.log',
                    help='Путь к файлу/папке логов')
args = parser.parse_args()

paths = []
if os.path.isfile(args.path):
    if args.path.endswith(".log"):
        paths.append(args.path)
else:
    for file in os.listdir(args.path):
        if file.endswith(".log"):
            paths.append(args.path + file)

if len(paths) == 0:
    raise ValueError(f"Файлов с расширением *.log не найдено в папке/файл {args.path}")

pattern = r"(?P<ip>.*?) (?P<remote_log_name>.*?) (?P<userid>.*?) \[(?P<date>.*?)(?= ) " \
            r"(?P<timezone>.*?)\] \"(?P<request_method>.*?) (?P<path>.*?) (?P<request_version>HTTP/.*)?\" " \
            r"(?P<status>.*?) (?P<length>.*?) \"(?P<referrer>.*?)\" \"(?P<user_agent>.*)\" " \
            r"(?P<generation_time_micro>.*)"
headers = ['ip', 'remote_log_name', 'userid', 'date', 'timezone', 'request_method', 'path',
           'request_version', 'status', 'length', 'referrer', 'user_agent', 'generation_time_micro']

reg = re.compile(pattern)
matches = []

for file in paths:
    print(f'Обрабатывается файл: {file}')
    with open(file, mode='r') as log_file:
        matches += reg.findall(log_file.read())

df = pd.DataFrame(matches, columns=headers)
df['generation_time_micro'] = df['generation_time_micro'].astype(int)

methods = df.groupby('request_method')['request_method'].count().reset_index(name='counts').to_dict('records')

ips = df.groupby('ip')['ip'].count().reset_index(name='counts').sort_values(by=['counts'], ascending=False)\
    .head(3).to_dict('records')

requests = df.sort_values(by=['generation_time_micro'], ascending=False)\
    .loc[:, ['request_method', 'path', 'ip', 'generation_time_micro', 'date']].head(3).to_dict('records')

os.makedirs("results", exist_ok=True)
with open(f"./results/result_{datetime.datetime.now()}.json", "w+") as f:
    res = dict(total=len(matches), methods=methods, top_ip=ips, top_long_requests=requests)
    json.dump(res, f, indent=4)
    f.seek(0)
    for line in f:
        print(line.strip('\n'))

"""
 
for match in matches:
    time = int(match[12])
    req = next((u for u in requests if u['url'] == match[6]), None)
    if req:
        if req['time'] < time:
            req['method'] = match[5]
            req['url'] = match[6]
            req['ip'] = match[0]
            req['time'] = time
            req['date'] = match[3]
    else:
        requests.append(dict(method=match[5], url=match[6], ip=match[0],
                             time=time, date=match[3]))

ips_sorted = [k for k in sorted(ips, key=lambda x: x['ctn'], reverse=True)[:3]]
requests_sorted = [k for k in sorted(requests, key=lambda x: x['time'], reverse=True)[:3]]
"""