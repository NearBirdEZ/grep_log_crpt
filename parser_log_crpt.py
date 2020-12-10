#!/usr/bin/env python3

import datetime
from datetime import timedelta
import paramiko
import requests
import re
import os.path


def take_properties(type_auth):
    with open('properties', 'r') as prop:
        for line in prop:
            if line.strip().startswith(f'user_{type_auth}'):
                user = line.strip().split('=')[1]
            elif line.strip().startswith(f'host_{type_auth}'):
                host = line.strip().split('=')[1]
            elif line.strip().startswith(f'port_{type_auth}'):
                port = int(line.strip().split('=')[1])
            elif line.strip().startswith(f'password_{type_auth}'):
                password = line.strip().split('=')[1]
    return user, password, host, port


def check_elastic(login, password, host, port, reg_num, fiscal_num, fd):
    headers = {
        'Content-Type': 'application/json',
    }
    params = (
        ('pretty', ''),
    )

    data = '{ "query" : { "bool" : { "must" : [{ "term" : {"requestmessage.fiscalDriveNumber.raw" : ' \
           '"%s"} }, {"term" : {"requestmessage.kktRegId.raw" : "%s"}}, {"term" : {"requestmessage.' \
           'fiscalDocumentNumber" : "%s"}}] } } }' % (fiscal_num, reg_num, fd)

    response = requests.post(f'http://{host}:{port}/_search', headers=headers, params=params, data=data,
                             auth=(login, password))
    response_json = response.json()['hits']['hits'][0]['_source']
    date_receipt = int(response_json['meta']['dateTimeMs']) // 1000
    date_receipt = datetime.datetime.utcfromtimestamp(date_receipt).strftime('%Y-%m-%d %H:%M:%S')
    date_send_to_crpt = None
    if response_json.get('crptInfo'):
        date_send_to_crpt = []
        for crpt_box in response_json['crptInfo']['sendInfo']:
            date_send = int(crpt_box['crptResponseDate']) // 1000
            date_send = date_receipt = datetime.datetime.utcfromtimestamp(date_send).strftime('%Y-%m-%d %H:%M:%S')
            date_send_to_crpt.append(date_send)
    return date_receipt, date_send_to_crpt


def eqv_date(get_receipt, get_talon):
    if not get_talon:
        get_talon = [get_receipt]
    min_date = datetime.datetime.fromisoformat(min(*[get_receipt], *get_talon))
    max_date = datetime.datetime.fromisoformat(max(*[get_receipt], *get_talon))
    period = max_date.date() - min_date.date()
    return min_date, period.days


def get_cmd_log(date, period):
    name_list = []
    if datetime.datetime.now().date() == date.date():
        cmd = 'grep'
        name_list.append(f'yellow_prom-ofd-send-to-crpt_{date.strftime("%Y_%m_%d")}.log')
    else:
        cmd = 'zgrep'
        for i in range(period + 1):
            next_date = date + timedelta(days=1)
            name_list.append(
                f'yellow_prom-ofd-send-to-crpt_{date.strftime("%Y_%m_%d")}.log-{next_date.strftime("%Y%m%d")}.gz')
            date = date + timedelta(days=1)
    return cmd, name_list


def connect_to_ssh(login, password, host, port, doc_id, cmd, name_log):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=host, username=login, port=port, password=password)
    stdin, stdout, stderr = client.exec_command(
        f'{cmd} -A150 {doc_id} /var/log/prom/prom-ofd-send-to-crpt/{name_log}')
    data, error = stdout.read().decode('utf-8').strip().split('\n'), stderr.read().decode('utf-8').strip().split('\n')
    client.close()
    return data, error


def parsing_log(log: list, id_doc):
    big_code = 'заглушка'
    container = []
    for line in log:
        line = re.sub(r'yellow prom-ofd-send-to-crpt', '', line)
        fiscal_doc_number = f'"fiscalDocumentNumber":{id_doc.split(":")[2]}'

        if re.search(id_doc, line) or re.search(big_code, line) or re.search(fiscal_doc_number, line):

            if re.search(r'\w{2,}-\w{2,}-\w{2,}-\w{2,}-\w{2,}', line) and (
                    re.search(id_doc, line) or re.search(fiscal_doc_number, line)):
                big_code = re.findall(r'\w{2,}-\w{2,}-\w{2,}-\w{2,}-\w{2,}', line)[0]
            line = re.sub(r'\[\d{3,}\]:\s\d{3,}-\d{1,}-\d{1,}\s\d{1,}:\d{1,}:\d{1,},\d{1,}\s\[\w{1,9}\]', '', line)
            container.append(line)
    return container


def main():
    start = datetime.datetime.now()
    print(f'Время старта [{start}]')

    user_server, password_server, host_server, port_server = take_properties('server')
    user_elastic, password_elastic, host_elastic, port_elastic = take_properties('elastic')

    doc_id_list = []
    with open('docs.txt', 'r') as docs:
        for line in docs:
            if line.strip() != '':
                doc_id_list.append(line.strip().split(':'))

    if not os.path.isdir("../log_crpt"):
        os.mkdir("../log_crpt")

    for _id in doc_id_list:
        print(f'Выполняется документ {":".join(_id)}')
        date_get_receipt, date_send_talon = check_elastic(user_elastic, password_elastic, host_elastic, port_elastic,
                                                          _id[0], _id[1], _id[2])
        low_date, period_days = eqv_date(date_get_receipt, date_send_talon)

        cmd_grep, name_log = get_cmd_log(low_date, period_days)
        with open(f'../log_crpt/{_id[0]}_{_id[0]}_{_id[0]}', 'w') as file:
            for name in name_log:
                logs, errors = connect_to_ssh(user_server, password_server, host_server, port_server, ":".join(_id), cmd_grep,
                                              name)
                if '' not in errors:
                    file.write(f'{errors}\n')
                if '' not in logs:
                    for line in parsing_log(logs, ":".join(_id)):
                        file.write(f'{line}\n')
                else:
                    file.write(f'Информации по ФД {_id} не найдена в логе {name}\n')

    print(f'Время выполнения [{datetime.datetime.now() - start}]')


if __name__ == '__main__':
    main()