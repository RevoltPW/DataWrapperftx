import websocket, json
import pandas as pd
import time
import csv
from datetime import datetime


SOCKET='wss://ftx.com/ws/'
SYMBOL = 'ETH-PERP'
CHANNEL = 'trades'

closes = []
in_position = False
global df, BOOL, ts

# client = Client(config.API_KEY, config.API_SECRET, tld='us')


def on_open(ws):
    print('opened connection')
    chanel_data = {'op': 'subscribe', 'channel': CHANNEL, 'market': SYMBOL}
    ws.send(json.dumps(chanel_data))
    createframe(True)
    #createcsv()

def on_close(ws):
    chanel_data = {'op': 'unsubscribe', 'channel': CHANNEL, 'market': SYMBOL}
    ws.send(json.dumps(chanel_data))
    print('closed connection')


def on_message(ws, message):
    json_message = json.loads(message)

    print(json_message)
    updateframe(json_message)

def createframe(Bo):
    global df
    df = pd.DataFrame()
    df.columns = ['date','side','price', 'size']
    df.price = df.price.astype(float)
    df.bid = df.bid.astype(float)
    df.ask = df.ask.astype(float)
    df.bid = df.bid.astype(float)
    df.set_index('date', inplace=True)

    #if Bo: df.to_csv(f"{SYMBOL}_{CHANNEL}.csv", mode='w', index=False, header=True)

def updateframe(msg):
    global df, BOOL, ts
    msg = msg['data']
    for item in msg:
        #print(item)
        #DATE= pd.Timestamp(msg['time'], unit='s')
        data = {'date': item['time'], 'side': item['side'], 'price': item['price'], 'size': item['size']}
        df = pd.append([data,df],ignore_index=true)

    
    if BOOL:
        df = df.iloc[:-1]
        #df.date = pd.to_datetime(df.date, unit='ms')
        df.to_csv(f"{SYMBOL}_{CHANNEL}_{ts}.csv", mode='a', index=False, header=True)
        BOOL=False
    #print(df)

    mSize = int(df.memory_usage(deep=True).sum())
    #print(mSize)

    if mSize > 1048:#10485760

        updatecsv()
        df ={}
        createframe(False)

def updatecsv():
    global df, ts

    #df.date = pd.to_datetime(df.date, unit='ms')
    df.to_csv(f"{SYMBOL}_{CHANNEL}_{ts}.csv", mode='a', index=False, header=False)
    print('UPDATED')
    return

if __name__ == '__main__':
    BOOL=True
    ts = int(time.time())
    ws = websocket.WebSocketApp(SOCKET, on_open=on_open, on_close=on_close, on_message=on_message)
    ws.run_forever()

