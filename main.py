from datetime import datetime as dt
from datetime import date, timedelta
import re
import os
from threading import Thread
import base64
from lib import Connections, JsonJob, get_version


def get_docs_list(file: str) -> list:
    doc_list = []
    with open(file, 'r') as docs:
        for line in docs:
            if line.strip() != '':
                doc_list.append(tuple(line.strip().split(':')))
    return doc_list


def make_work_dir() -> None:
    if not os.path.isdir("./CRPT_LOGS"):
        os.mkdir("./CRPT_LOGS")
    os.chdir('./CRPT_LOGS')


def make_fd_dir(reg_num: str, fiscal_number: str, fd: str) -> str:
    name = f"./{reg_num}.{fiscal_number}.{fd}"
    if not os.path.isdir(name):
        os.mkdir(name)
    return name


def create_el_request(reg_num: str, fiscal_number: str, fd: str) -> str:
    el_request = """
    {
        "query": {
            "bool": {
                "must": [
                    {"term": {"requestmessage.fiscalDriveNumber.raw": "%s"}}, 
                    {"term": {"requestmessage.kktRegId.raw": "%s"}}, 
                    {"term": {"requestmessage.fiscalDocumentNumber": "%s"}}
                    ]
                }
            }
        }""" % (fiscal_number, reg_num, fd)
    return el_request


def receipt_parsing(receipt: list) -> tuple:
    if not receipt:
        return ()
    receipt = receipt[0]['_source']
    if receipt['requestmessage'].get('items'):
        for item in receipt['requestmessage']['items']:
            if item.get('productCode'):
                item['productCode'] = [f"base64 = {item['productCode']}",
                                       f"Длинна = {len(item['productCode'])}",
                                       f"hex = {base64.b64decode(item['productCode']).hex()}"]
    date_receipt_ts = int(receipt['meta']['dateTimeMs']) // 1000
    date_receipt = dt.utcfromtimestamp(date_receipt_ts).strftime('%Y-%m-%d %H:%M:%S')
    date_send_to_crpt = None
    if receipt.get('crptInfo'):
        date_send_to_crpt = []
        for crpt_box in receipt['crptInfo']['sendInfo']:
            date_send = int(crpt_box['crptResponseDate']) // 1000 + 10800
            date_send = dt.utcfromtimestamp(date_send).strftime('%Y-%m-%d %H:%M:%S')
            date_send_to_crpt.append(date_send)
    return receipt, date_receipt, date_send_to_crpt


def get_cmd_log(date_low, period):
    cmd_log_dict = {}
    for i in range(period + 1):
        if dt.now().date() == date_low:
            cmd_grep = 'grep'
            name_log = f'yellow_prom-ofd-send-to-crpt_{date_low.strftime("%Y_%m_%d")}.log'
        else:
            cmd_grep = 'zgrep'
            next_date = date_low + timedelta(days=1)
            name_log = f'yellow_prom-ofd-send-to-crpt_{date_low.strftime("%Y_%m_%d")}.log-{next_date.strftime("%Y%m%d")}.gz'
        date_low = date_low + timedelta(days=1)
        if cmd_log_dict.get(cmd_grep):
            cmd_log_dict[cmd_grep] += [name_log]
        else:
            cmd_log_dict[cmd_grep] = [name_log]
    return cmd_log_dict


def eqv_date(get_receipt, get_talon):
    if not get_talon:
        get_talon = [get_receipt]
    min_date = date.fromisoformat(min(*[get_receipt], *get_talon).split()[0])
    max_date = date.fromisoformat(max(*[get_receipt], *get_talon).split()[0])
    period = max_date - min_date
    return min_date, period.days


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


def glue_log(cmd_name, doc_id):
    code = 'заглушка'
    errors_container = []
    log_container = []
    for cmd, names in cmd_name.items():
        for name in names:
            print(f'Для {doc_id} смотрим лог {name}')
            full_command = f'{cmd} -A1000 "{doc_id}" /var/log/prom/prom-ofd-send-to-crpt/{name}'
            logs, errors = Connections.to_ssh(full_command)
            if '' not in errors:
                errors_container += errors
            if '' not in logs:
                log, code = parsing_log(logs, doc_id, code)
                log_container += log
            else:
                log_container += [f'Информации по ФД {doc_id} не найдена в логе {name}\n']
    return errors_container, log_container


def run(number_thread: int, threads: int, documents: list):
    for i in range(number_thread, len(documents), threads):
        _id = documents[i]
        doc_name = '_'.join(_id)
        receipt = Connections.elastic_search(create_el_request(*_id), "receipt.20*")['hits']['hits']
        receipt, date_get_receipt, date_send_talon = receipt_parsing(receipt)
        name_dir = make_fd_dir(*_id)
        JsonJob.write_json(f"{name_dir}/{doc_name}.json", receipt)
        low_date, period_days = eqv_date(date_get_receipt, date_send_talon)
        grep_dict = get_cmd_log(low_date, period_days)
        with open(f'{name_dir}/{doc_name}_log.txt', 'w') as file:
            container_with_errors, container_with_log = glue_log(grep_dict, ":".join(_id))
            if container_with_errors:
                for er in container_with_errors:
                    file.write(f'{er}\n')
            for line in container_with_log:
                file.write(f'{line}\n')


def start_threading(threads: int, documents: list) -> None:
    treads = []
    for i in range(threads):
        t = Thread(target=run, args=(i, threads, documents))
        t.start()
        treads.append(t)
    for i in range(threads):
        treads[i].join()


def main():
    fd_documents = get_docs_list('docs.txt')
    make_work_dir()
    start_threading(5, fd_documents)


if __name__ == '__main__':
    if get_version():
        start = dt.now()
        main()
        print(f'Время выполнения [{dt.now() - start}]')
    else:
        print('Просьба обновить версию приложения по ссылке: https://github.com/NearBirdEZ/grep_log_crpt')


