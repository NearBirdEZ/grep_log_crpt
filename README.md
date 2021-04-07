# grep_log_crpt <br>

Данный скрипт позволяет получать фискальные документы из эластика, а так же логи с сервера.<br>
Лишняя информация отсекается с помощью регулярных выражений.<br>

Установка:<br>
Первый запуск:<br>
1. python -m venv venv<br>
2. source venv/bin/activate<br>
3. pip3 install -r requirements.txt<br>
4. Заполнить документ docs.txt в формате:<br>
rnm:fn:fd<br>
rnm:fn:fd<br>
...<br>
5. python main.py<br>
<br>
<br>
Последующие запуски:<br>
1. source venv/bin/activate<br>
2. Заполнить документ docs.txt в формате:<br>
rnm:fn:fd<br>
rnm:fn:fd<br>
...<br>
3. python main.py<br>
