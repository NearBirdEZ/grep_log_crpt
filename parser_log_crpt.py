#!/usr/bin/env python3

import datetime
from datetime import date
from datetime import timedelta
import paramiko
import requests
import re
import os.path
from threading import Thread
import json
import base64


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

    response = requests.post(f'http://{host}:{port}/receipt.*/_search', headers=headers, params=params, data=data,
                             auth=(login, password))
    response_json = response.json()['hits']['hits'][0]['_source']
    for item in response_json['requestmessage']['items']:
        item['productCode'] = [f"base64 = {item['productCode']}",
                               f"Длинна = {len(item['productCode'])}",
                               f"hex = {base64.b64decode(item['productCode']).hex()}"]

    with open(f'../log_crpt/{reg_num}_{fiscal_num}_receipt.json', 'w', encoding='utf-8') as file:
        json.dump(response_json,
                  file,
                  indent=4,
                  ensure_ascii=False,
                  sort_keys=False)

    date_receipt = int(response_json['meta']['dateTimeMs']) // 1000
    date_receipt = datetime.datetime.utcfromtimestamp(date_receipt).strftime('%Y-%m-%d %H:%M:%S')
    date_send_to_crpt = None
    if response_json.get('crptInfo'):
        date_send_to_crpt = []
        for crpt_box in response_json['crptInfo']['sendInfo']:
            date_send = int(crpt_box['crptResponseDate']) // 1000
            date_send = datetime.datetime.utcfromtimestamp(date_send).strftime('%Y-%m-%d %H:%M:%S')
            date_send_to_crpt.append(date_send)
    return date_receipt, date_send_to_crpt


def eqv_date(get_receipt, get_talon):
    if not get_talon:
        get_talon = [get_receipt]
    min_date = date.fromisoformat(min(*[get_receipt], *get_talon).split()[0])
    max_date = date.fromisoformat(max(*[get_receipt], *get_talon).split()[0])
    period = max_date - min_date
    return min_date, period.days


def get_cmd_log(date_low, period):
    cmd_log_dict = {}
    for i in range(period + 1):
        if datetime.datetime.now().date() == date_low:
            cmd_grep = 'grep'
            name_log = f'yellow_prom-ofd-send-to-crpt_{date_low.strftime("%Y_%m_%d")}.log'
        else:
            cmd_grep = 'zgrep'
            next_date = date_low + timedelta(days=1)
            name_log = f'yellow_prom-ofd-send-to-crpt_{date_low.strftime("%Y_%m_%d")}.log-{next_date.strftime("%Y%m%d")}.gz'
        date_low = date_low + timedelta(days=1)
        cmd_log_dict.update({cmd_grep: name_log})
    return cmd_log_dict


def connect_to_ssh(login, password, host, port, doc_id, cmd, name_log):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=host, username=login, port=port, password=password)
    stdin, stdout, stderr = client.exec_command(
        f'{cmd} -A150 {doc_id} /var/log/prom/prom-ofd-send-to-crpt/{name_log}')
    data, error = stdout.read().decode('utf-8').strip().split('\n'), stderr.read().decode('utf-8').strip().split('\n')
    client.close()
    return data, error


def parsing_log(log: list, id_doc, big_code):
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
    return container, big_code


def glue_log(cmd_name, doc_id, user_server, password_server, host_server, port_server):
    code = 'заглушка'
    errors_container = []
    log_container = []
    for cmd, name in cmd_name.items():
        logs, errors = connect_to_ssh(user_server, password_server, host_server, port_server, doc_id, cmd, name)
        if '' not in errors:
            errors_container += errors
        if '' not in logs:
            log, code = parsing_log(logs, doc_id, code)
            log_container += log
        else:
            log_container += [f'Информации по ФД {doc_id} не найдена в логе {name}\n']
    return errors_container, log_container


def run(num_thread, doc_id_list, user_server, password_server, host_server, port_server, user_elastic, password_elastic,
        host_elastic, port_elastic):
    for i in range(num_thread, len(doc_id_list), 4):
        _id = doc_id_list[i]
        print(f'Выполняется документ {":".join(_id)}. Используется поток № {num_thread + 1}')
        date_get_receipt, date_send_talon = check_elastic(user_elastic, password_elastic, host_elastic, port_elastic,
                                                          _id[0], _id[1], _id[2])
        low_date, period_days = eqv_date(date_get_receipt, date_send_talon)

        grep_dict = get_cmd_log(low_date, period_days)
        with open(f'../log_crpt/{_id[0]}_{_id[1]}_{_id[2]}_log.txt', 'w') as file:
            container_with_errors, container_with_log = glue_log(grep_dict, ":".join(_id), user_server, password_server,
                                                                 host_server, port_server)
            if container_with_errors:
                for er in container_with_errors:
                    file.write(f'{er}\n')
            for line in container_with_log:
                file.write(f'{line}\n')


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

    treads = []
    for i in range(5):
        t = Thread(target=run, args=(i, doc_id_list, user_server, password_server, host_server, port_server,
                                     user_elastic, password_elastic, host_elastic, port_elastic))
        t.start()
        treads.append(t)
    for i in range(5):
        treads[i].join()

    print(f'Время выполнения [{datetime.datetime.now() - start}]')


if __name__ == '__main__':
    main()
