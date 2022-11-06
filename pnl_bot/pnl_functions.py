import ccxt
import pandas as pd
import numpy as np
from datetime import datetime as dt
import time
import json


# нужны: функция saveOrders которая от старта до енда
# загружает ордера,и либо делает append
# в существующий csv или создает новый
# формат дат - '2022-08-01T00:00:00'

json_folder = r'jsons_data'

def search_trades(all_tickers, start_date, end_date, exchange) -> list:
    """
    Поиск трейдов за выделенный период
    Parameters
    ----------
    all_tickers - list; Внутри список
    start_date - string; Дата в формате '2022-08-01T00:00:00', начало просматриваемого периода
    end_date - string; Дата в формате '2022-08-01T00:00:00', конец просматриваемого периода
    exchange - exchange; Объект exchange из ccxt подключенный к конкретной бирже

    Returns - list; список с трейдами (трейды представлены в виде словарей)
    -------

    """
    hour = 1 * 60 * 60 * 1000
    all_trades = []
    k = 0

    for ticker in all_tickers:
        time.sleep(1)
        k += 1
        print(f'{ticker}: {k} from {len(all_tickers)}')
        start_time = exchange.parse8601(start_date)
        end_time = exchange.parse8601(end_date)
        if dt.utcfromtimestamp(end_time / 1e3) > dt.utcnow():
            end_time = (dt.utcnow() - dt(1970, 1, 1)).total_seconds() * 1e3

        # если запрос более часа режем по часам
        if (end_time - start_time) > 1 * 60 * 60 * 1000:
            while start_time < end_time:
                end_period = start_time + hour

                # проверяем, что конец искомого периода не залетел за текущее время
                # если залетел - то ставим конец периода на текущее время по UTC + конвертим в секунды от epoch
                if dt.utcfromtimestamp(end_period / 1e3) > dt.utcnow():
                    end_period = (dt.utcnow() - dt(1970, 1, 1)).total_seconds() * 1e3

                trades = exchange.fetch_my_trades(ticker, start_time, None, {'endTime': end_period})

                ### save trades to json ###
                with open(json_folder + f"//{dt.now().strftime('%H_%M_%S_%d_%m_%Y')}.json", 'w') as json_file:
                    json_file.write(json.dumps(trades))
                ### ###

                if len(trades):
                    start_time = trades[-1]['timestamp'] + 1  # следующая миллисекунда от последнего трейда
                    all_trades += trades
                else:
                    start_time = end_period
        else:
            trades = exchange.fetch_my_trades(ticker, start_time, None, {'endTime': end_time})

            ### save trades to json ###
            with open(json_folder + f"//{dt.now().strftime('%H_%M_%S_%d_%m_%Y')}.json", 'w') as json_file:
                json_file.write(json.dumps(trades))
            ### ###

            all_trades += trades
    return all_trades


def trades_to_df(all_trades, exchange) -> pd.DataFrame():
    """
    Преобразование списка со словарями из search_trades в dataframe
    Внутри происходят всякие фильтрации, переименования, чтобы привести таблицу к кастомному виду
    Parameters
    ----------
    all_trades - list; Список со словарями внутри которых - ордера исполненные, результат работы search_trades

    Returns - DataFrame; ['Date(UTC)', 'Market', 'Type', 'Price', 'Amount', 'Total', 'Fee', 'Fee Coin']
    -------

    """
    trades_df = pd.DataFrame(all_trades)
    needed_cols = ['id', 'datetime', 'symbol', 'side', 'price', 'amount', 'cost', 'fee']
    output_df = trades_df[needed_cols]
    output_df['Date(UTC)'] = pd.to_datetime(output_df['datetime'])
    output_df['Fee Coin'] = output_df['fee'].apply(lambda x: x['currency'])
    output_df['Fee'] = output_df['fee'].apply(lambda x: x['cost'])
    output_df['symbol'] = output_df['symbol'].apply(lambda x: str(x).replace('/', ''))
    rename_cols = {'symbol': 'Market', 'side': 'Type', 'price': 'Price', 'amount': 'Amount', 'cost': 'Total'}
    output_df = output_df.rename(columns=rename_cols)
    final_cols = ['id', 'Date(UTC)', 'Market', 'Type', 'Price', 'Amount', 'Total', 'Fee', 'Fee Coin']
    final_df = output_df[final_cols]
    final_df['commission'] = final_df.apply(lambda x: apply_commission(x['Fee Coin'], x['Total'], x['Amount'],
                                                                       x['Fee'], x['Date(UTC)'], exchange), axis=1)
    final_df['apiKey'] = exchange.apiKey
    return final_df


def grouping_pnl(orders_df, period_hours):
    """
    Функция для группировки списка ордеров и подсчета pnl
    Parameters
    ----------
    orders_df - dataframe; Таблица со списком ордеров из функции trades_to_df

    Returns - (dataframe, dataframe); два датафрейма со сгруппированными ордерами и рассчитанным pnl
    -------

    """
    grouped_df = orders_df.groupby(['Market', 'Type', 'Fee Coin']).agg({'Total': 'sum', 'Amount': 'sum',
                                                                        'commission': 'sum', 'Type': 'count'})
    grouped_df = grouped_df.rename(columns={'Type': 'OrdersCount'})
    grouped_df = grouped_df.reset_index()
    markets = list(grouped_df['Market'].unique())  # список инструментов
    pnl_df = pd.DataFrame(columns=['Type'] + markets)  # новый df куда соберем pnl

    # словарь - строка в будущем pnl_df
    buy_total_dict = {'Type': 'buy_total_sum'}
    sell_total_dict = {'Type': 'sell_total_sum'}
    buy_amount_dict = {'Type': 'buy_amount_sum'}
    sell_amount_dict = {'Type': 'sell_amount_sum'}
    price_buy_dict = {'Type': 'price_buy'}
    price_sell_dict = {'Type': 'price_sell'}
    delta_dict = {'Type': 'delta'}
    profit_dict = {'Type': 'profit'}
    comm_dict = {'Type': 'commission'}
    total_dict = {'Type': 'total'}
    p_to_vol_dict = {'Type': 'profit/volume'}
    count_orders_dict = {'Type': 'Сделок'}
    deals_per_hours_dict = {'Type': 'Сделок в час'}
    average_count_or_dict = {'Type': 'average'}
    price_delta = {'Type': 'price_delta'}

    # по каждому инструменту посчитаем строки
    # логику брали из excel pnl, можно сравнить с ее формулами
    # в delta_dict есть округления, нужны для правильного расчета маленьких чисел (где много знаков после запятой)
    for market in markets:
        buy_total_dict[market] = grouped_df.loc[((grouped_df['Market'] == market) &
                                                (grouped_df['Type'] == 'buy')), 'Total'].sum()
        sell_total_dict[market] = grouped_df.loc[((grouped_df['Market'] == market) &
                                                 (grouped_df['Type'] == 'sell')), 'Total'].sum()
        buy_amount_dict[market] = grouped_df.loc[((grouped_df['Market'] == market) &
                                                  (grouped_df['Type'] == 'buy')), 'Amount'].sum()
        sell_amount_dict[market] = grouped_df.loc[((grouped_df['Market'] == market) &
                                                   (grouped_df['Type'] == 'sell')), 'Amount'].sum()
        price_buy_dict[market] = buy_total_dict[market] / buy_amount_dict[market]
        price_sell_dict[market] = sell_total_dict[market] / sell_amount_dict[market]
        delta_dict[market] = round(price_sell_dict[market] - price_buy_dict[market], 6)
        profit_dict[market] = delta_dict[market] * sell_amount_dict[market]
        comm_dict[market] = grouped_df.loc[(grouped_df['Market'] == market), 'commission'].sum()
        total_dict[market] = float(profit_dict[market]) - comm_dict[market]
        p_to_vol_dict[market] = total_dict[market] / grouped_df.loc[(grouped_df['Market'] == market), 'Total'].sum()
        count_orders_dict[market] = grouped_df.loc[(grouped_df['Market'] == market), 'OrdersCount'].sum()
        deals_per_hours_dict[market] = count_orders_dict[market] / \
                                        ((pd.to_datetime(orders_df.loc[orders_df['Market'] == market, 'Date(UTC)'].max())
                                        - pd.to_datetime(orders_df.loc[orders_df['Market'] == market,
                                          'Date(UTC)'].min())).total_seconds() // 3600)
        first_price = orders_df.loc[orders_df['Market'] == market].reset_index()['Price'][0]
        last_price = orders_df.loc[orders_df['Market'] == market].reset_index()['Price'][-1:].values[0]
        price_delta[market] = (last_price/first_price - 1) * 100  # изменение цены за выбранный период

        if deals_per_hours_dict[market] == np.inf:
            deals_per_hours_dict[market] = count_orders_dict[market]

        average_count_or_dict[market] = grouped_df.loc[(grouped_df['Market'] == market), 'Total'].sum() / \
                                        count_orders_dict[market]

        # проведем дополнительные округления для красивого форматирования
        # делаем округления после расчетов, чтобы округления не влияли на расчеты
        buy_total_dict[market] = round(buy_total_dict[market], 2)
        sell_total_dict[market] = round(sell_total_dict[market], 2)
        buy_amount_dict[market] = round(buy_amount_dict[market], 2)
        sell_amount_dict[market] = round(sell_amount_dict[market], 2)
        count_orders_dict[market] = round(count_orders_dict[market])
        deals_per_hours_dict[market] = round(deals_per_hours_dict[market])
        average_count_or_dict[market] = round(average_count_or_dict[market])
        profit_dict[market] = round(profit_dict[market], 2)
        comm_dict[market] = round(comm_dict[market], 2)
        total_dict[market] = round(total_dict[market], 2)
        price_delta[market] = str(round(price_delta[market], 2)) + ' %'

        # 'переводим в проценты'
        p_to_vol_dict[market] = str(round(p_to_vol_dict[market] * 100, 4)) + ' %'

    #  складываем строки-словари в pnl_df
    pnl_df = pnl_df.append(price_buy_dict, ignore_index=True)
    pnl_df = pnl_df.append(price_sell_dict, ignore_index=True)
    pnl_df = pnl_df.append(buy_total_dict, ignore_index=True)
    pnl_df = pnl_df.append(sell_total_dict, ignore_index=True)
    pnl_df = pnl_df.append(buy_amount_dict, ignore_index=True)
    pnl_df = pnl_df.append(sell_amount_dict, ignore_index=True)
    pnl_df = pnl_df.append(delta_dict, ignore_index=True)
    pnl_df = pnl_df.append(profit_dict, ignore_index=True)
    pnl_df = pnl_df.append(comm_dict, ignore_index=True)
    pnl_df = pnl_df.append(total_dict, ignore_index=True)
    pnl_df = pnl_df.append(p_to_vol_dict, ignore_index=True)
    pnl_df = pnl_df.append(count_orders_dict, ignore_index=True)
    pnl_df = pnl_df.append(deals_per_hours_dict, ignore_index=True)
    pnl_df = pnl_df.append(average_count_or_dict, ignore_index=True)
    pnl_df = pnl_df.append(price_delta, ignore_index=True)

    # транспонируем таблицы, выносим первую строку как заголовок
    grouped_df = grouped_df.T
    grouped_df.columns = grouped_df.iloc[0]
    grouped_df = grouped_df[1:]
    
    pnl_df = pnl_df.T
    pnl_df.columns = pnl_df.iloc[0]
    pnl_df = pnl_df[1:]

    # считаем суммы
    summ_dict={}
    summ_dict['Type'] = 'summ'
    summ_dict['buy_total_sum'] = pnl_df['buy_total_sum'].sum()
    summ_dict['sell_total_sum'] = pnl_df['sell_total_sum'].sum()
    summ_dict['delta'] = pnl_df['delta'].sum()
    summ_dict['profit'] = pnl_df['profit'].sum()
    summ_dict['commission'] = pnl_df['commission'].sum()
    summ_dict['total'] = pnl_df['total'].sum()
    summ_dict['Сделок'] = pnl_df['Сделок'].sum()

    pnl_df = pnl_df.reset_index()
    pnl_df = pnl_df.rename(columns={'index': 'Type'})
    pnl_df = pnl_df.append(summ_dict, ignore_index=True)
    pnl_df = pnl_df.fillna('-')
    pnl_df = pnl_df.set_index('Type')
    pnl_df.index.name = None
    return grouped_df, pnl_df


def data_recency(orders_path):
    """
    Группировка базы с ордерами по минимальным и максимальным датам в разрезе тикеров и apiKeys
    Parameters
    ----------
    orders_path - путь к csv файлу с ордерами
    Returns - grouped_df; dataFrame с группировкой
    -------

    """
    orders_df = pd.read_csv(orders_path, sep=';')
    orders_df['apiKey'] = orders_df['apiKey'].apply(lambda x: str(x)[:5])
    orders_df['Date(UTC)_min'] = orders_df['Date(UTC)']
    orders_df['Date(UTC)_max'] = orders_df['Date(UTC)']
    orders_df['Count'] = orders_df['Type']
    grouped_orders = orders_df.groupby(['Market', 'apiKey']).agg({'Count': 'count', 'Date(UTC)_min': 'min',
                                                                  'Date(UTC)_max': 'max'})
    grouped_orders['Date(UTC)_min'] = grouped_orders['Date(UTC)_min'].apply(lambda x: str(x)[:19])
    grouped_orders['Date(UTC)_max'] = grouped_orders['Date(UTC)_max'].apply(lambda x: str(x)[:19])
    grouped_orders = grouped_orders.sort_values(by='Date(UTC)_max', ascending=False)
    return grouped_orders

# TODO много раз запрашивать курс в цикле apply для каждой секунды - плохая практика
# TODO надо сделать умнее - идти окнами по требуемым секундам, чтобы уменьшить кол-во запросов
def apply_commission(fee_coin, total, amount, fee, date_utc, exchange):
    """
    Функция для приложения к таблице ордеров и подсчета комиссии в busd
    Комиссия - либо монета в которой и идёт трейд, тогда цену из этого трейд и взять,
    лишний запрос не делать, либо bnb busd и в случае с bnb пересчитать его в busd
    (как и всю комиссию по сути)
    Parameters
    ----------
    fee_coin - монета в которой считается комиссия
    total - общая стоимость ордера в busd
    amount - кол-во коинов в ордере
    fee - размер комиссии
    date_utc - дата ордера
    exchange - ccxt объект API handle к бирже, нужен для запроса цены BNB/BUSD в моменты оредов

    Returns - comission (int) - размер комиссии в busd
    -------

    """
    if fee == 0:
        return 0

    if fee_coin not in ['BNB', 'BUSD']:
        commission = (total / amount) * fee
    elif fee_coin == 'BNB':
        # вот тут надо запросить курс bnb/busd
        # приводим строку ордера к типу exchange (timestamp в int)
        order_time = exchange.parse8601(str(date_utc)[:19].replace(' ', 'T'))

        # лучшие практики программирования
        k = 0
        while k < 2:
            k += 1
            try:
                rp = exchange.fetch_ohlcv('BNB/BUSD', '1m', order_time, 1)  # запрашиваем курс bnb/busd на момент ордера
                commission = rp[0][1] * fee
                k += 1
            except Exception as e:
                time.sleep(1)
                commission = 0

    elif fee_coin == 'BUSD':
        commission = fee
    else:
        print('ERROR! Ошибка в расчете комиссий (apply_comission)')

    return commission
