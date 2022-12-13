import ccxt
import json
from datetime import datetime as dt
from datetime import timedelta as td
import pandas as pd
import traceback
import warnings
warnings.filterwarnings('ignore')

from pnl_functions import search_trades, trades_to_df


start_day = '2022-12-08'
end_day = '2022-12-08'


config_path = r'config.json'
with open(config_path) as f:
    config_data = json.load(f)

orders_path = config_data['orders_path']
logs_bot_path = config_data['logs_bot_path']
kst_dict = config_data['keys_secrets_tickers']

# итерируемся по ключам в конфиге
# по итогу будет список с кортежами по два элемента в каждом: первый - ccxt.binance, второй - список с тикерами
exchange_list = [(ccxt.binance({'apiKey': x['api_key'], 'secret': x['secret'],
                               'enableRateLimit': True, 'rateLimit': 100}), x['tickers']) for x in kst_dict]


def update_orders(orders_df, exchange, all_tickers, start_day, end_day):
    """
    Fuction for custom loading of orders from start_day to end_day
    Parameters
    ----------
    orders_df - dataframe with orders
    exchange - handle of exchange
    all_tickers - tickers to load orders
    start_day - start of period %YYYY-%mm-%dd, e.g. '2022-11-05'
    end_day - end of period %YYYY-%mm-%dd, e.g. '2022-11-06'

    Returns
    -------

    """
    start_date = f'{start_day}T00:00:00'
    end_date = f'{end_day}T23:59:59'
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
    with open(logs_bot_path, 'a') as log_file:
        log_file.write(log_message + '\n')
    print(f'LogMessage: {log_message}')


try:
    print('Update orders...')
    k = 0
    new_orders_api = []  # в каких апи были новые ордера?
    for exchange_item in exchange_list:
        k += 1
        print(f'\nApiKey {k} from {len(exchange_list)} keys')
        orders_df = pd.read_csv(orders_path, sep=';')
        updated_df = update_orders(orders_df, exchange_item[0], exchange_item[1], start_day, end_day)

        # если есть новые трейды - записываем их
        if len(updated_df) != len(orders_df):
            updated_df['Type'] = updated_df['Type'].str.lower()
            updated_df.astype(str).to_csv(orders_path, index=False, sep=';')
            new_orders_api.append(exchange_item[0].apiKey[:6])
        else:
            logging_errors(f'{str(dt.now())[:19]}: UPDATE ORDERS CUSTOM [{start_day} - {end_day}]: '
                           f'no new orders for ApiKey: {exchange_item[0].apiKey[:6]}; '
                           f'for tickers: {str(exchange_item[1])}')
    if len(new_orders_api) > 0:
        logging_errors(f'{str(dt.now())[:19]}: UPDATE ORDERS CUSTOM [{start_day} - {end_day}]: SUCCESSFUL, '
                       f'new orders for apiKeys: {str(new_orders_api)}')
    else:
        logging_errors(f'{str(dt.now())[:19]}: UPDATE ORDERS CUSTOM [{start_day} - {end_day}]: no new orders at all!')

except Exception as e:
    print(f'Exception:\n{e}\n\nTraceback:\n{traceback.format_exc()}')
    logging_errors(f'{str(dt.now())[:19]}: UPDATE ORDERS ERROR:\n{e}\n\nTraceback:\n{traceback.format_exc()}')

