/pnl_bot/ - folder with main project
/pnl_bot/tele_bot.py - Main

Python ver: 3.8.10


Расположение проекта на сервере:

/var/pnl_bot/pnl_bot


Команды для работы с сервером:

ps -ef | grep python - найти питоновский процесс, полезно при деплоее нового кода, когда нужно убить старый процесс
sudo kill -9 process_id - kill process


Отправить обновленные скрипты на сервер:

scp tele_bot.py root@139.180.205.252:/var/pnl_bot/pnl_bot/
scp pnl_functions.py root@139.180.205.252:/var/pnl_bot/pnl_bot/


Команды в cron для работы проекта:

39 13 17 9 * cd /var/pnl_bot/pnl_bot/ && bin/python "tele_bot.py"
*/15 * * * * cd /var/pnl_bot/pnl_bot/ && bin/python "update_orders_base.py" > cronlog1.txt 2>&1


Конфиг:

Конфиг лежит в файле config.json
Структура:

{"bot_token": "***",
  "users": ["dampall", "Theoreticaly", "aysolovyev", "pnl_binance_bot", "mkrln92"],
  "keys_secrets_tickers": [{"api_key": "***",
                           "secret": "***",
                           "tickers": ["QI/BUSD", "OOKI/BUSD", "IDEX/BUSD", "DODO/BUSD"]},
                          {"api_key": "***",
                           "secret": "***",
                           "tickers": ["BAND/BUSD", "ALPINE/BUSD", "FXS/BUSD"]},
                          {"api_key": "***",
                           "secret": "***",
                           "tickers": ["ZIL/BUSD", "SPELL/BUSD", "KP3R/BUSD", "UNI/BUSD"
                                       ]}],
  "orders_path": "orders_base.csv",
  "logs_bot_path": "logs_bot.txt"}


Логи:

Логи для update_orders_base.py и tele_bot.py лежат в logs_bot.txt