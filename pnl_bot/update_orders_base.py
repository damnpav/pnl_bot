import ccxt
import json
from datetime import datetime as dt
from datetime import timedelta as td
import pandas as pd
import traceback
import warnings
warnings.filterwarnings('ignore')

from pnl_functions import search_trades, trades_to_df


# вскрываем json с конфигами
config_path = r'config.json'
with open(config_path) as f:
    config_data = json.load(f)

orders_path = config_data['orders_path']
logs_bot_path = config_data['logs_bot_path']
kst_dict = config_data['keys_secrets_tickers']

# итерируемся по ключам в конфиге
# по итогу будет список с кортежами по два элемента в каждом: первый - ccxt.binance, второй - список с тикерами
exchange_list = [(ccxt.binance({'apiKey': x['api_key'], 'secret': x['secret'],
                               'enableRateLimit': True, 'rateLimit': 200}), x['tickers']) for x in kst_dict]


def update_orders(orders_df, exchange, all_tickers):

    if exchange.apiKey in list(orders_df['apiKey'].unique()):
        # последняя дата ордеров по данному apiKey
        #start_date = orders_df.loc[orders_df['apiKey'] == exchange.apiKey, 'Date(UTC)'].max()[:19].replace(' ', 'T')
        start_date = (dt.utcnow() - td(hours=2)).strftime('%Y-%m-%dT%H:%M:%S')
    else:
        # если apiKey новый, выгрузим по нему историю с прошлых 2х дней
        start_date = str(dt.utcnow() - td(2))[:19].replace(' ', 'T')

    end_date = str(dt.utcnow())[:19].replace(' ', 'T')  # конец - текущий момент
    my_trades = search_trades(all_tickers, start_date, end_date, exchange)

    if len(my_trades) > 0:
        new_orders_df = trades_to_df(my_trades, exchange)
        merged_df = pd.concat([new_orders_df.astype(str), orders_df], ignore_index=True)
        merged_df = merged_df.sort_values(by='Date(UTC)', ascending=True)
        merged_df = merged_df.drop_duplicates(subset=['id', 'order'], keep='last')
        return merged_df
    else:
        return orders_df


def logging_errors(log_message):
    """
    Функция логгирования. Путь к файлу с логами тянется из config'а по global переменной (logs_bot_path)
    Parameters
    ----------
    log_message - сообщения для лога
    -------
    """
    print(f'log_message: {log_message}')
    with open(logs_bot_path, 'a') as log_file:
        log_file.write(log_message + '\n')


try:
    print('Update orders...')
    k = 0
    new_orders_api = []  # в каких апи были новые ордера?
    for exchange_item in exchange_list:
        k += 1
        print(f'\nApiKey {k} from {len(exchange_list)} keys')
        orders_df = pd.read_csv(orders_path, sep=';', converters={'id': str, 'order': str})
        updated_df = update_orders(orders_df, exchange_item[0], exchange_item[1])

        # если есть новые трейды - записываем их
        if len(updated_df) != len(orders_df):
            updated_df['Type'] = updated_df['Type'].str.lower()
            updated_df.astype(str).to_csv(orders_path, index=False, sep=';')
            new_orders_api.append(exchange_item[0].apiKey[:6])
        else:
            logging_errors(f'{str(dt.now())[:19]}: UPDATE ORDERS: no new orders for ApiKey: '
                           f'{exchange_item[0].apiKey[:6]}; for tickers: {str(exchange_item[1])}')

    if len(new_orders_api) > 0:
        logging_errors(f'{str(dt.now())[:19]}: UPDATE ORDERS: SUCCESSFUL, '
                       f'new orders for apiKeys: {str(new_orders_api)}')
    else:
        logging_errors(f'{str(dt.now())[:19]}: UPDATE ORDERS: no new orders at all!')

except Exception as e:
    print(f'Exception:\n{e}\n\nTraceback:\n{traceback.format_exc()}')
    logging_errors(f'{str(dt.now())[:19]}: UPDATE ORDERS ERROR:\n{e}\n\nTraceback:\n{traceback.format_exc()}')

