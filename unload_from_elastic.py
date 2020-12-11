#!/usr/bin/env python3

import requests
from parser_log_crpt import take_properties
from datetime import date, datetime
import time


def check_elastic(login, password, host, port, inn, rnm, fn, fromdate):
    headers = {
        'Content-Type': 'application/json',
    }

    params = (
        ('pretty', ''),
    )
    if rnm == '' and fn == '':
        data = '{"size" : 0, "query" : {"bool" : {"must" : ' \
               '[{"exists" : {"field" : "requestmessage.items*.productCode"}},' \
               ' {"term" : {"requestmessage.userInn" : "%s"}}, {"range" : ' \
               '{"requestmessage.dateTime" : {"gte" : %d}}}]}},' \
               '"aggs": {"id": {"terms": {"field": "id","size": 400000}}}}' % (inn, fromdate)
    elif fn == '' and inn == '':
        data = '{"size" : 0, "query" : {"bool" : {"must" : ' \
               '[{"exists" : {"field" : "requestmessage.items*.productCode"}},' \
               ' {"term" : {"requestmessage.kktRegId.raw" : "%s"}}, ' \
               '{"range" : {"requestmessage.dateTime" : {"gte" : %d}}}]}},' \
               '"aggs": {"id": {"terms": {"field": "id","size": 400000}}}}' % (rnm, fromdate)
    elif rnm == '' and inn == '':
        data = '{"size" : 0, "query" : {"bool" : {"must" : ' \
               '[{"exists" : {"field" : "requestmessage.items*.productCode"}},' \
               ' {"term" : {"requestmessage.fiscalDriveNumber.raw" : "%s"}}, ' \
               '{"range" : {"requestmessage.dateTime" : {"gte" : %d}}}]}},' \
               '"aggs": {"id": {"terms": {"field": "id","size": 400000}}}}' % (fn, fromdate)
    elif inn == '':
        data = '{"size" : 0, "query" : {"bool" : {"must" : ' \
               '[{"exists" : {"field" : "requestmessage.items*.productCode"}},' \
               ' {"term" : {"requestmessage.kktRegId.raw" : "%s"}}, ' \
               '{"term" : {"requestmessage.fiscalDriveNumber.raw" : "%s"}}, ' \
               '{"range" : {"requestmessage.dateTime" : {"gte" : %d}}}]}},' \
               '"aggs": {"id": {"terms": {"field": "id","size": 400000}}}}' % (rnm, fn, fromdate)

    response = requests.post(f'http://{host}:{port}/receipt.*/_search', headers=headers, params=params, data=data,
                             auth=(login, password))

    return response.json()['aggregations']['id']['buckets']


def main():
    start = datetime.now()
    print(f'Время старта [{start}]')
    user_elastic, password_elastic, host_elastic, port_elastic = take_properties('elastic')
    with open('inn_from_date.txt', 'r') as info:
        for line in info:
            if line.startswith('inn'):
                inn = line.split('=')[1].strip()
            elif line.startswith('kktRegId'):
                rnm = line.split('=')[1].strip()
            elif line.startswith('fiscalDriveNumber'):
                fn = line.split('=')[1].strip()
            elif line.startswith('fromDate'):
                fromdate = int(time.mktime(date.fromisoformat(line.split('=')[1].strip()).timetuple()))

    rnm_fn_fd_json_list = check_elastic(user_elastic, password_elastic, host_elastic, port_elastic, inn,
                                        rnm, fn, fromdate)
    print(f'Найдено {len(rnm_fn_fd_json_list)} ФД')
    with open('docs.txt', 'w') as file:
        for container in rnm_fn_fd_json_list:
            file.write(f"{container['key']}\n")

    print(f'Затрачено времени [{datetime.now() - start}]')


if __name__ == '__main__':
    main()
