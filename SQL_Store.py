# -*- coding: utf-8 -*-
"""
Created on Tue Mar 17 11:49:07 2020

@author: EternalBliss
"""
import websocket
import json
import sqlite3

conn = sqlite3.connect("Renko.db")
i = {"LYFT":0,"AMZN":0,"MSFT":0,"AAPL":0,"BTCUSDT":0}
Close_prev = {"LYFT":0,"AMZN":0,"MSFT":0,"AAPL":0,"BTCUSDT":0}
length = {"LYFT":0,"AMZN":0,"MSFT":0,"AAPL":0,"BTCUSDT":0}
Open_prev = {"LYFT":0,"AMZN":0,"MSFT":0,"AAPL":0,"BTCUSDT":0}
Previous = {"LYFT":0,"AMZN":0,"MSFT":0,"AAPL":0,"BTCUSDT":0}

c = conn.cursor()

tickers = ["LYFT","AMZN","MSFT","AAPL","BTCUSDT"]

for ticker in tickers:
    try:
        c.execute("CREATE TABLE {} (Date real,Open real,Close real,Volume real, Conditon real)".format(ticker))
        conn.commit()
    except sqlite3.OperationalError:
        pass

def on_message(ws, message):
    global i, Close_prev, Open_prev, Previous
    print(message)
    # global length
    Symbol = json.loads(message)["data"][0]["s"]
    if (Symbol == "BINANCE:BTCUSDT"):
        Symbol = "BTCUSDT"
    Date = json.loads(message)["data"][0]["t"]
    Close = json.loads(message)["data"][0]["p"]
    Volume = json.loads(message)["data"][0]["v"]
    length = Close*0.3*0.01
    
    if(i[Symbol]==0):
        Close_prev[Symbol]=Close
        Open_prev[Symbol]=Close
        with conn:
            c.execute("INSERT INTO {} VALUES ({},{},{},{},1)".format(Symbol, Date, Close, Close, Volume))
        i[Symbol]=1
    elif(i[Symbol]==1 and Previous[Symbol]==0):
        if((Close-Close_prev[Symbol])/length>=1):
            for j in range(int(abs(Close-Close_prev[Symbol])//length)):
                Close=Close_prev[Symbol]+length*(j+1)
                Close_prev[Symbol]=Close_prev[Symbol]+length*j
                with conn:
                    c.execute("INSERT INTO {} VALUES ({},{},{},{},2)".format(Symbol, Date, Close_prev[Symbol], Close, Volume))
            Open_prev[Symbol]=Close_prev[Symbol]
            Close_prev[Symbol]=Close
            Previous[Symbol]=1
        elif((Close-Close_prev[Symbol])/length<=-1):
            for k in range(int(abs(Close-Close_prev[Symbol])//length)):
                Close=Close_prev[Symbol]-length*(k+1)
                Close_prev[Symbol]=Close_prev[Symbol]-length*k
                with conn:
                    c.execute("INSERT INTO {} VALUES ({},{},{},{},3)".format(Symbol, Date, Close_prev[Symbol], Close, Volume))
            Open_prev[Symbol]=Close_prev[Symbol]
            Close_prev[Symbol]=Close
            Previous[Symbol]=-1
    elif(i[Symbol]==1 and Previous[Symbol]==1):
        if((Close-Close_prev[Symbol])/length>=1):
            for j in range(int(abs(Close-Close_prev[Symbol])//length)):
                Close=Close_prev[Symbol]+length*(j+1)
                Close_prev[Symbol]=Close_prev[Symbol]+length*j
                with conn:
                    c.execute("INSERT INTO {} VALUES ({},{},{},{},4)".format(Symbol, Date, Close_prev[Symbol], Close, Volume))
            Open_prev[Symbol]=Close_prev[Symbol]
            Close_prev[Symbol]=Close
            Previous[Symbol]=1
        elif((Close-Open_prev[Symbol])/length<=-1):
            for k in range(int(abs(Close-Open_prev[Symbol])//length)):
                Close=Open_prev[Symbol]-length*(k+1)
                Close_prev[Symbol]=Open_prev[Symbol]-length*k
                with conn:
                    c.execute("INSERT INTO {} VALUES ({},{},{},{},5)".format(Symbol, Date, Close_prev[Symbol], Close, Volume))
            Open_prev[Symbol]=Close_prev[Symbol]
            Close_prev[Symbol]=Close
            Previous[Symbol]=-1
    elif(i[Symbol]==1 and Previous[Symbol]==-1):
        if((Close-Open_prev[Symbol])/length>=1):
            for j in range(int(abs(Close-Open_prev[Symbol])//length)):
                Close=Open_prev[Symbol]+length*(j+1)
                Close_prev[Symbol]=Open_prev[Symbol]+length*j
                with conn:
                    c.execute("INSERT INTO {} VALUES ({},{},{},{},6)".format(Symbol, Date, Close_prev[Symbol], Close, Volume))
            Open_prev[Symbol]=Close_prev[Symbol]
            Close_prev[Symbol]=Close
            Previous[Symbol]=1
        elif((Close-Close_prev[Symbol])/length<=-1):
            for k in range(int(abs(Close-Close_prev[Symbol])//length)):
                Close=Close_prev[Symbol]-length*(k+1)
                Close_prev[Symbol]=Close_prev[Symbol]-length*k
                with conn:
                    c.execute("INSERT INTO {} VALUES ({},{},{},{},7)".format(Symbol, Date, Close_prev[Symbol], Close, Volume))
            Open_prev[Symbol]=Close_prev[Symbol]
            Close_prev[Symbol]=Close
            Previous[Symbol]=-1
        

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###\n")
    global tickers
    for ticker in tickers:
        with conn:
            c.execute("INSERT INTO {} VALUES ({},{},{},{},0)".format(ticker, 0, 0, 0, 0))
        c.execute("SELECT * FROM {}".format(ticker))
        print(c.fetchall())

def on_open(ws):
    tickers = ["LYFT","AMZN","MSFT","AAPL","BINANCE:BTCUSDT"]
    for ticker in tickers:
        print('{{"type":"subscribe","symbol":"{}"}}'.format(ticker))
        ws.send('{{"type":"subscribe","symbol":"{}"}}'.format(ticker))

if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://ws.finnhub.io?token=bpp3tkvrh5rb5khcrlsg",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()

