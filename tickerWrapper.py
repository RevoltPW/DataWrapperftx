import websocket, json
import pandas as pd
import csv
import time
from datetime import datetime


SOCKET='wss://ftx.com/ws/'
SYMBOL = 'ETH-PERP'
CHANNEL = 'ticker'
global df, firstRun ,ts, priorData
priorData = ""

def on_open(ws):
    print('opened connection')
    chanel_data = {'op': 'subscribe', 'channel': CHANNEL, 'market': SYMBOL}
    ws.send(json.dumps(chanel_data))
    createframe(True)
    #createcsv()

def on_close(ws):
    chanel_data = {'op': 'subscribe', 'channel': CHANNEL, 'market': SYMBOL}
    ws.send(json.dumps(chanel_data))
    print('closed connection')

def on_message(ws, message):
    json_message = json.loads(message)

    #print(json_message)
    updateframe(json_message)

def createframe(firstExecution):
    global df
    df = pd.DataFrame()
    df.columns = ['date', 'price', 'bid', 'ask', 'bidSize', 'askSize']
    df.price = df.price.astype(float)
    df.bid = df.bid.astype(float)
    df.ask = df.ask.astype(float)
    df.bidSize = df.bidSize.astype(float)
    df.askSize = df.askSize.astype(float)
    df.set_index('date', inplace=True)
    print(df.keys())
    if firstExecution: df.to_csvpreviousData(f"{SYMBOL}_{CHANNEL}.csv", mode='w', index=False, header=True)

def updateframe(msg):
    global df, firstRun, ts, priorData 
    Data = {}

    lastData = (f"{msg['data']['last']}{msg['data']['bid']}{msg['data']['ask']}{msg['data']['bidSize']}{msg['data']['askSize']}")
    
    if priorData != lastData:

        DATE = pd.Timestamp(msg['data']['time'], unit='s')
        Data = {'date': DATE, 'price': float(msg['data']['last']), 'bid': msg['data']['bid'], 'ask': msg['data']['ask'], 'bidSize': msg['data']['bidSize'], 'askSize': msg['data']['askSize']}
        print(Data)
        df = df.append(Data, ignore_index=True) 
        priorData = lastData
        if firstRun:
            df = df.iloc[:-1]
            #df.date = pd.to_datetime(df.date, unit='ms')
            df.to_csv(f"{SYMBOL}_{CHANNEL}_{ts}.csv", mode='a', index=False, header=True)
            firstRun=False

        mSize = int(df.memory_usage(deep=True).sum())
        print(humanbytes(mSize),humanbytes(10485760) )
        
        #Save dataframe to CSV when the memory size reach 10mb (customize or replace for a shedule method)
        #just to not write de disk each time a message is recived  
        if mSize > 10485760:#10485760
            #Save dataframe to CSV 
            updatecsv()
            df ={}
            createframe(False)

def humanbytes(B):
    """Return the given bytes as a human friendly KB, MB, GB, or TB string."""
    B = float(B)
    KB = float(1024)
    MB = float(KB ** 2) # 1,048,576
    GB = float(KB ** 3) # 1,073,741,824
    TB = float(KB ** 4) # 1,099,511,627,776

    if B < KB:
        return '{0} {1}'.format(B,'Bytes' if 0 == B > 1 else 'Byte')
    elif KB <= B < MB:
        return '{0:.2f} KB'.format(B / KB)
    elif MB <= B < GB:
        return '{0:.2f} MB'.format(B / MB)
    elif GB <= B < TB:
        return '{0:.2f} GB'.format(B / GB)
    elif TB <= B:
        return '{0:.2f} TB'.format(B / TB)

def createcsv():

    with open(f"{SYMBOL}_{CHANNEL}.csv", 'w') as csvfile:
        # creating a csv dict writer object
        writer = csv.DictWriter(csvfile, fieldnames=['date', 'price', 'bid', 'ask', 'bidSize', 'askSize'])

        # writing headers (field names)
        writer.writeheader()
        data=['date', 'price', 'bid', 'ask', 'bidSize', 'askSize']
        # writing data rows
        writer.writerows(data)


#update csv "a" - Append - will append to the end of the file
def updatecsv():
    global df, ts

    #df.date = pd.to_datetime(df.date, unit='ms')
    df.to_csv(f"{SYMBOL}_{CHANNEL}_{ts}.csv", mode='a', index=False, header=False)
    print('UPDATED')
    return

if __name__ == '__main__':

    ts = int(time.time())
    firstRun = True
    ws = websocket.WebSocketApp(SOCKET, on_open=on_open, on_close=on_close, on_message=on_message)
    ws.run_forever()