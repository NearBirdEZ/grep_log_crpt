import csv
from threading import Thread, Lock
import requests
from urllib.request import urlopen
from lxml import etree
import certifi
import psycopg2
import paramiko
import json
from config import Config


class CsvJob:
    @staticmethod
    def open_csv(name: str) -> list:
        """Open CSV files"""
        file_list = []
        with open(name, "r", newline="") as file:
            reader = csv.reader(file)
            for row in reader:
                row = row[0].split(',')
                file_list.append(row)
        return file_list

    @staticmethod
    def write_file(name: str, mode: str, row: list) -> None:
        """Write to CSV file"""
        with open(name, mode, encoding='utf-8') as file:
            writer = csv.writer(file, delimiter=',')
            writer.writerow(row)

    def glue_csv(self, name_list: list, new_name: str) -> None:
        """Соединить несколько csv файлов"""
        with open(new_name, 'a', encoding='utf-8') as file:
            writer = csv.writer(file, delimiter=',')
            for count_file, name in enumerate(name_list):
                for count_line, line in enumerate(self.open_csv(name)):
                    writer.writerow(line)


class JsonJob:
    @staticmethod
    def write_json(name: str, response_json: dict or list) -> None:
        if not name.endswith('.json'):
            name = name + '.json'
        with open(name, 'w', encoding='utf-8') as file:
            json.dump(response_json,
                      file,
                      indent=4,
                      ensure_ascii=False,
                      sort_keys=False)

    @staticmethod
    def print_json(response_json) -> None:
        print(json.dumps(response_json,
                         indent=4,
                         ensure_ascii=False,
                         sort_keys=False))

    @staticmethod
    def read_json(name: str) -> dict or list:
        with open(name, 'r') as file:
            return json.loads(file.read())


class __AnyClass:

    def __init__(self, threads):
        self.threads = threads
        self.lock = Lock()
        self.count = 0

    def count_any_one_in_threads(self):
        self.lock.acquire()
        self.count += 1
        self.lock.release()

    def job_function(self, num_thread, lst, a, b):
        for i in range(num_thread, len(lst), self.threads):
            return self.threads * a * b * num_thread

    def start_threading(self):
        tread_list = []
        lst = []
        for i in range(self.threads):
            t = Thread(target=self.job_function, args=(i, lst, 10, 20))
            t.start()
            tread_list.append(t)
        for i in range(self.threads):
            tread_list[i].join()


def get_version():
    url = 'https://github.com/NearBirdEZ/grep_log_crpt/blob/master/config.py'
    response = urlopen(url, cafile=certifi.where())
    html_parser = etree.HTMLParser()
    tree = etree.parse(response, html_parser)
    online_version = float(tree.xpath('//*[@id="LC25"]/span[3]/text()')[0])
    return online_version == Config.local_version


class Connections:

    @staticmethod
    def to_ssh(cmd: str) -> tuple:
        """На вход передаются команды для сервера, на выход отдается результат"""
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=Config.HOST_SSH,
                       username=Config.USER_SSH,
                       port=Config.POST_SSH,
                       password=Config.POST_SSH)
        stdout, stderr = (out.read().decode('utf-8').strip().split('\n') for out in client.exec_command(cmd)[1:])
        client.close()
        return stdout, stderr

    @staticmethod
    def elastic_search(data: str, index: str = '*') -> dict or list:
        """
        На вход принимает запрос для поиска, возвращает json

        Примеры запросов

        {"size" : 1 }

        { "query" : { "bool" : { "must" :
        [{ "term" : {"requestmessage.fiscalDriveNumber.raw" : "9999999999"} },
        {"term" : {"requestmessage.kktRegId.raw" : "7777777777"}},
        {"term" : {"requestmessage.fiscalDocumentNumber" : "888888888"}}] } } }
        """

        headers = {
            'Content-Type': 'application/json',
        }
        params = (
            ('pretty', ''),
        )

        response = requests.post(f'http://{Config.HOST_EL_PROM}:{Config.PORT_EL_PROM}/{index}/_search',
                                 headers=headers, params=params, data=data,
                                 auth=(Config.USER_EL_PROM, Config.PASSWORD_EL_PROM))
        return response.json()

    @staticmethod
    def elastic_count(data: str, index: str = '*') -> dict or list:
        """
        возвращает значение count
        """

        headers = {
            'Content-Type': 'application/json',
        }
        params = (
            ('pretty', ''),
        )

        response = requests.post(f'http://{Config.HOST_EL_PROM}:{Config.PORT_EL_PROM}/{index}/_count',
                                 headers=headers, params=params, data=data,
                                 auth=(Config.USER_EL_PROM, Config.PASSWORD_EL_PROM))
        return response.json()['count']

    def __sql(func):
        def wrapper(request: str):
            connect_db = psycopg2.connect(
                database=Config.NAME_DATABASE_PROM,
                user=Config.USER_DB_PROM,
                password=Config.PASSWORD_DB_PROM,
                host=Config.HOST_EL_PROM,
                port=Config.PORT_DB_PROM
            )
            cursor = connect_db.cursor()
            return func(request, connect_db, cursor)

        return wrapper

    @staticmethod
    @__sql
    def sql_select(request: str, *args) -> list:
        """
        На вход подается sql запрос
        На выходе массив построчно.
        :param request:
        :return row: list:
        """
        _, cursor = args
        cursor.execute(request)
        rows = cursor.fetchall()
        return rows

    @staticmethod
    @__sql
    def sql_update(request: str, *args) -> None:
        """
        На вход подается sql запрос
        На выходе массив построчно.
        :param request:
        :return:
        """
        connect_db, cursor = args
        cursor.execute(request)
        connect_db.commit()
        return


if __name__ == '__main__':
    pass
