import telebot
import time
from telebot import types
from datetime import datetime as dt
from datetime import timedelta as td
import traceback
import json
import dataframe_image as dfi
import pandas as pd
import re
from pnl_functions import grouping_pnl, data_recency
from fifo_module import fifo_pnl, fifo_pnl_embedded

import warnings
import sys

warnings.filterwarnings('ignore')


#  вскрываем json с конфигами
config_path = r'config.json'
with open(config_path) as f:
    config_data = json.load(f)

bot_token = config_data['bot_token']
bot_users = config_data['users']
orders_path = config_data['orders_path']
logs_bot_path = config_data['logs_bot_path']


message_dict = {'welcome': 'Привет!\nЭтот бот показывает pnl, выбери период\действие:',
                'available': 'В базе ордеров\nминимальная дата: *min_date*,\nмаксимальная дата: *max_date*',
                'in_development': 'Функция находится в разработке...'}

try:
    bot = telebot.TeleBot(bot_token)


    @bot.message_handler(commands=['start'])
    def start_handler(message):
        username = message.from_user.username
        chat_id = message.chat.id
        if username in bot_users:
            bot.send_message(chat_id, message_dict['welcome'], reply_markup=welcoming_buttons(), parse_mode='HTML')


    @bot.message_handler(commands=['send_orders'])
    def start_handler(message):
        """
        Function to send current orders_base file
        Parameters
        ----------
        message

        Returns
        -------

        """
        print('send_orders command')
        username = message.from_user.username
        chat_id = message.chat.id
        if username not in bot_users:
            print(f'User {username} not in bot_users')
            logging_errors(f'{str(dt.now())[:19]}: User {username} not in bot_users')
            return False
        orders_file = open(orders_path, 'rb')
        bot.send_document(chat_id, orders_file,
                          visible_file_name=f"orders_base{dt.strftime(dt.now(), '%H_%M__%d%m%Y')}.csv")


    @bot.message_handler(regexp="h\d+")
    def regex_handler(message):
        """
        Function to catch callbacks with regex in commands, like 'h1, h2 and so on...'
        Parameters
        ----------
        message - callback with regex

        Returns
        -------

        """
        print('Handle regex')
        username = message.from_user.username
        chat_id = message.chat.id
        msg = message.text
        if username not in bot_users:
            print(f'User {username} not in bot_users')
            logging_errors(f'{str(dt.now())[:19]}: User {username} not in bot_users')
            return False

        hour_message = re.findall('h\d+', msg)
        if hour_message:
            if hour_message[0].replace('h', '').isdigit():
                hour_int = int(hour_message[0].replace('h', ''))
                print(f'h{hour_int}_cb')
                grouped_png_path, pnl_png_path = make_group_pnl(hour_int)

                if grouped_png_path == 0:
                    bot.send_message(chat_id, 'No data for that period yet..', parse_mode='HTML')
                    time.sleep(1)
                else:
                    #grouping_photo = open(grouped_png_path, 'rb')
                    pnl_photo = open(pnl_png_path, 'rb')
                    # bot.send_message(chat_id, f'Grouping for {hour_int} hour:', parse_mode='HTML')
                    # time.sleep(1)
                    # bot.send_photo(chat_id=chat_id, photo=grouping_photo, parse_mode='HTML')
                    # time.sleep(1)1
                    bot.send_message(chat_id, f'PnL for {hour_int} hour:', parse_mode='HTML')
                    bot.send_photo(chat_id=chat_id, photo=pnl_photo, parse_mode='HTML')
                    time.sleep(1)


    @bot.message_handler(regexp="fifo\d+")
    def regex_fifo_handler(message):
        print('Handle fifo regex')
        username = message.from_user.username
        chat_id = message.chat.id
        msg = message.text
        if username not in bot_users:
            print(f'User {username} not in bot_users')
            logging_errors(f'{str(dt.now())[:19]}: User {username} not in bot_users')
            return False

        hour_message = re.findall('fifo\d+', msg)
        if hour_message:
            if hour_message[0].replace('fifo', '').isdigit():
                hour_int = int(hour_message[0].replace('fifo', ''))
                print(f'fifo{hour_int}_cb')
                fifo_plot_path, fifo_df_path = fifo_pnl(hour_int, orders_path)

                if fifo_plot_path == 0:
                    bot.send_message(chat_id, 'No data for that period yet..', parse_mode='HTML')
                    time.sleep(1)
                else:
                    fifo_df_photo = open(fifo_df_path, 'rb')
                    fifo_plot_photo = open(fifo_plot_path, 'rb')
                    bot.send_message(chat_id, f'FIFO dataframe for {hour_int} hour:', parse_mode='HTML')
                    time.sleep(1)
                    bot.send_photo(chat_id=chat_id, photo=fifo_df_photo, parse_mode='HTML')
                    time.sleep(1)
                    bot.send_message(chat_id, f'FIFO plot for {hour_int} hour:', parse_mode='HTML')
                    time.sleep(1)
                    bot.send_photo(chat_id=chat_id, photo=fifo_plot_photo, parse_mode='HTML')
                    time.sleep(1)



    @bot.callback_query_handler(func=lambda call: True)
    def handle_buttons(call):
        print('Handle buttons')
        msg = str(call.data)
        chat_id = call.message.chat.id
        username = call.message.from_user.username

        # если не наш юзер - просто выйдет
        # это кстати спорный моментик надо оттестить как следует
        if username not in bot_users:
            print(f'User {username} not in bot_users')
            logging_errors(f'{str(dt.now())[:19]}: User {username} not in bot_users')
            return False

        if 'h' in msg:
            print('hour button')
            hour_value = msg.replace('/h', '').replace('_cb', '')   # вытаскиваем час
            if not hour_value.isdigit():
                print(f'Не смогли вытащить значение часа из кнопки с часами')
                logging_errors('Не смогли вытащить значение часа из кнопки с часами')
            else:
                hour_value = int(hour_value)
                grouped_png_path, pnl_png_path = make_group_pnl(hour_value)
                if grouped_png_path == 0:
                    bot.send_message(chat_id, 'No data for that period yet..', parse_mode='HTML')
                else:
                    # grouping_photo = open(grouped_png_path, 'rb')
                    pnl_photo = open(pnl_png_path, 'rb')
                    # bot.send_message(chat_id, f'Grouping for {hour_value} hour:', parse_mode='HTML')
                    # time.sleep(1)
                    # bot.send_photo(chat_id=chat_id, photo=grouping_photo, parse_mode='HTML')
                    bot.send_message(chat_id, f'PnL for {hour_value} hour:', parse_mode='HTML')
                    bot.send_photo(chat_id=chat_id, photo=pnl_photo, parse_mode='HTML')
                time.sleep(1)
        elif msg == '/custom_cb':
            bot.send_message(chat_id, message_dict['in_development'], parse_mode='HTML')
            time.sleep(1)
        elif msg == '/available_cb':
            recency_df = data_recency(orders_path)
            recency_df_styled = recency_df.style.background_gradient()
            recency_df_path = f'PNGs/recency_df_{dt.now().strftime("%H%M%S%d%m%Y")}.png'
            dfi.export(recency_df_styled, recency_df_path, table_conversion='matplotlib')
            recency_photo = open(recency_df_path, 'rb')
            bot.send_message(chat_id, 'Data recency per Ticker, apiKey:', parse_mode='HTML')
            time.sleep(1)
            bot.send_photo(chat_id=chat_id, photo=recency_photo, parse_mode='HTML')
            time.sleep(1)
            bot.send_message(chat_id, f"Текущее utc время:\n{dt.utcnow().strftime('%d-%m-%Y %H:%M:%S')}")
            time.sleep(1)
        elif msg == '/stop_cb':
            bot.send_message(chat_id, f"tele_bot.py скрипт будет остановлен, перезапустить его можно через cronn\n"
                                      f"update_orders_base.py (регулярное обновление ордеров) продолжает работать, "
                                      f"его можно выключить через cron")
            sys.exit()
        else:
            pass
        bot.send_message(chat_id, message_dict['welcome'], reply_markup=welcoming_buttons(), parse_mode='HTML')
        time.sleep(1)


    def available_periods():
        """
        Смотрим доступные периоды в базе с ордерами
        Returns - min, max; (str)
        -------
        """
        orders_df = pd.read_csv(orders_path, sep=';')
        return orders_df['Date(UTC)'].min(), orders_df['Date(UTC)'].max()


    def make_group_pnl(period_hours):
        """
        Функция для фильтрации таблицы с ордерами на нужное кол-во дней
        Подсчета pnl и закатки таблицы с группировкой и pnl в картинку для отправки
        Parameters
        ----------
        period_days - за сколько дней считаем pnl
        Returns - grouped_png_path, pnl_png_path; пути где лежат созданные картинки
        -------

        """
        orders_df = pd.read_csv(orders_path, sep=';')
        orders_df = orders_df.drop_duplicates()
        orders_df['Date(UTC)'] = pd.to_datetime(orders_df['Date(UTC)'])
        selected_df = orders_df.loc[(pd.Timestamp.utcnow() - td(hours=period_hours)) < orders_df['Date(UTC)']]
        grouped_df, pnl_df = grouping_pnl(selected_df, period_hours)
        fifo_path, result_df = fifo_pnl_embedded(period_hours, orders_path)
        fifo_df = result_df.T.rename(columns={'position': 'position_fifo', 'pnl': 'pnl_fifo'})
        pnl_df.join(fifo_df)
        final_df = pnl_df.join(fifo_df)
        final_df['pnl_fifo']['summ'] = final_df['pnl_fifo'].sum()
        final_df['position_fifo']['summ'] = final_df['position_fifo'].sum()
        final_df = final_df[['price_buy', 'price_sell', 'buy_total_sum', 'sell_total_sum',
                             'delta', 'profit', 'commission', 'total', 'pnl_fifo', 'position_fifo', 'profit/volume',
                             'Сделок', 'Сделок в час', 'average', 'price_delta']]
        final_df = final_df.rename(columns={'total': 'avco_total'})
        pnl_df = final_df

        if len(grouped_df.columns) == 0 or len(pnl_df.columns) == 0:
            return 0, 0

        # добавим разделители к наименованию столбцов
        grouped_df.columns = ['  | ' + str(x) + ' |  ' for x in grouped_df.columns]
        pnl_df.columns = ['  | ' + str(x) + ' |  ' for x in pnl_df.columns]


        grouped_png_path = f'PNGs/grouped_df_{dt.now().strftime("%H%M%S%d%m%Y")}.png'
        pnl_png_path = f'PNGs/pnl_df_{dt.now().strftime("%H%M%S%d%m%Y")}.png'
        dfi.export(grouped_df, grouped_png_path, table_conversion='matplotlib', max_rows=-1, max_cols=-1)
        dfi.export(pnl_df, pnl_png_path, table_conversion='matplotlib', max_rows=-1, max_cols=-1)
        return grouped_png_path, pnl_png_path


    def welcoming_buttons():
        """
        Функция для публикации кнопок
        В кнопке указан текст и команда которую она возвращает в бэк для обработки ответа
        """
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton(text='1h period', callback_data='/h1_cb'),
                   types.InlineKeyboardButton(text='2h period', callback_data='/h2_cb'),
                   types.InlineKeyboardButton(text='4h period', callback_data='/h4_cb'),
                   types.InlineKeyboardButton(text='8h period', callback_data='/h8_cb'),
                   types.InlineKeyboardButton(text='24h period', callback_data='/h24_cb'),
                   types.InlineKeyboardButton(text='custom', callback_data='/custom_cb'),
                   types.InlineKeyboardButton(text='available periods', callback_data='/available_cb'),
                   types.InlineKeyboardButton(text='stop bot', callback_data='/stop_cb')
                   )
        return markup


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


    while 1:
        try:
            print('start tele_bot')
            bot.polling()
        except Exception as e:
            # если часто будет падать, надо попробовать удалять и пересоздавать объект bot
            print(f'Exception tele_bot:\n{e}\n\nTraceback:\n{traceback.format_exc()}')
            logging_errors(f'{str(dt.now())[:19]}: Exception:\n{e}\n\nTraceback:\n{traceback.format_exc()}')
            time.sleep(30)


except Exception as e:
    print(f'Exception:\n{e}\n\nTraceback:\n{traceback.format_exc()}')
    with open('error_log.txt', 'w') as log_file:
        log_file.write(traceback.format_exc())

