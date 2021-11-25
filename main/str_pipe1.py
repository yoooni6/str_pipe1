from pybit import HTTP
import pandas as pd
import numpy as np
from datetime import datetime as dt
import pandas_ta as ta
import warnings
import traceback as tb
import plotly.graph_objects as go
import plotly.subplots as ms
import telegram as telgr
import json
import math
import time


def create_session(test, api_key, api_secret):
    TESTNET = 'https://api-testnet.bybit.com'
    MAINNET = 'https://api.bybit.com'

    session = HTTP(
        endpoint=TESTNET if test else MAINNET,
        api_key=api_key,
        api_secret=api_secret
    )
    return session

def fetch_ohlcv_bybit(session, symbol, interval, num_candle, **kwargs):
    if "to_time" in kwargs:
        time_now = kwargs["to_time"]
    else:
        time_now = int(dt.utcnow().timestamp())
    since = time_now-interval*60*num_candle
    # print(f"Fetching new bars since {dt.fromtimestamp(since)}")

    response=session.query_kline(symbol=symbol,interval=interval,**{'from':since, 'limit':num_candle})
    if response['ret_msg'] == 'OK':
        df = pd.json_normalize(response["result"])
        df["datetime"] = pd.to_datetime(df.start_at, unit='s')
        return df
    else:
        raise Exception


def supertrend(df, period=5, atr_multiplier=2):
    df[["SUPER_trend", "SUPER_direction", "SUPER_long", "SUPER_short"]] = df.ta.supertrend(high=df['high'], low=df['low'], close=df['close'], length=period, multiplier=atr_multiplier)
        
    return df.iloc[period:,:].reset_index(drop=True)

def create_telegram_bot():
    with open('~/str_pipe1/main/notistar_bot.json', mode="r") as f:
        notistar_json = json.load(f)
    bot = telgr.Bot(token=notistar_json["bot_token"])
    return bot, notistar_json["chat_id"]


def send_message_telegram(bot, chat_id, df, open_time):
    time_now = pd.to_datetime(open_time, unit='s')
    try:
        my_text = f"""
        Symbol : {df.iloc[-1,:]["symbol"]}
        Interval : {df.iloc[-1,:]["interval"]}
        Datatime : {time_now}
        Current_price : {df.iloc[-1,:]["close"]}

        Supertrend_channel : {df.iloc[-2,:]["SUPER_trend"]}
        Supertrend_direction : {df.iloc[-1,:]["SUPER_direction"]}
        Open_position : {"Long" if df.iloc[-2,:]["SUPER_direction"]==-1 else "Short"}
        Stop_loss : 0
        """
    except:
        my_text = "Error"
    finally:
        bot.sendMessage(chat_id=chat_id, text=my_text)


def check_open_long_short_signals(df):
    global in_long_position
    global open_long_datetime
    global in_short_position
    global open_short_datetime

    last_row_index = len(df.index) - 1
    previous_row_index = last_row_index - 1

    if df['SUPER_direction'][previous_row_index] == -1 and df['close'][last_row_index] >= df['SUPER_trend'][previous_row_index]:
        if not in_long_position:
            in_long_position = True
            open_long_datetime = int(dt.utcnow().timestamp())
            # send message
            bot, chat_id = create_telegram_bot()
            send_message_telegram(bot, chat_id, df, open_long_datetime)
        else:
            pass
    if df['SUPER_direction'][previous_row_index] == -1 and df['SUPER_direction'][last_row_index] == -1:
        if in_long_position:
            if pd.to_datetime(int(open_long_datetime)//300*300, unit='s') < pd.to_datetime(int(dt.utcnow().timestamp())//300*300, unit='s'):
                open_long_datetime = None
                in_long_position = False
        else:
            pass
    
    if df['SUPER_direction'][previous_row_index] == 1 and df['close'][last_row_index] <= df['SUPER_trend'][previous_row_index]:
        if not in_short_position:
            in_short_position = True
            open_short_datetime = int(dt.utcnow().timestamp())
            # send message
            bot, chat_id = create_telegram_bot()
            send_message_telegram(bot, chat_id, df, open_short_datetime)
        else:            
            pass

    if df['SUPER_direction'][previous_row_index] == 1 and df['SUPER_direction'][last_row_index] == 1:
        if in_short_position:
            if pd.to_datetime(int(open_short_datetime)//300*300, unit='s') < pd.to_datetime(int(dt.utcnow().timestamp())//300*300, unit='s'):
                open_short_datetime = None
                in_short_position = False
        else:
            pass


def run_bot(test=False, symbol="BTCUSDT", itv = 5, num_candle = 200, plot_yn = False):
    try:
        # print(pd.to_datetime(int(dt.utcnow().timestamp()), unit='s'))
        # Read json - check history

        # Fetch private wallet data - have position, check open-order, modify/cancel order
        session = create_session(test=test, api_key='api_key', api_secret='api_secret')

        # Fetch ohlcv data
        df = fetch_ohlcv_bybit(session=session, symbol=symbol, interval=itv, num_candle=num_candle)

        # Calculate Indicator
        supertrend_data = supertrend(df, period=5, atr_multiplier=2)

        # Check OpenLong/OpenShort signals
        check_open_long_short_signals(supertrend_data)

        # Save history to json
        # if plot_yn:
        #     plot_candlestick(df)

        # return df
    except:
        print(time.ctime())
        print(tb.format_exc())


if __name__=="__main__":
    in_long_position = False
    open_long_datetime = None
    in_short_position = False
    open_short_datetime = None
    
    while True:
        run_bot()
        time.sleep(0.5)
