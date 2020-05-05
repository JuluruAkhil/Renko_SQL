# -*- coding: utf-8 -*-
"""
Created on Tue Mar 17 11:49:07 2020

@author: EternalBliss
"""
import websocket
import json
import sqlite3

conn = sqlite3.connect("Renko.db")
c = conn.cursor()

tickers = ["SPY","AMZN","MSFT","AAPL","BTCUSDT"]

i = {"SPY":0,"AMZN":0,"MSFT":0,"AAPL":0,"BTCUSDT":0}
Close_prev = {"SPY":0,"AMZN":0,"MSFT":0,"AAPL":0,"BTCUSDT":0}
length = {"SPY":0,"AMZN":0,"MSFT":0,"AAPL":0,"BTCUSDT":0}
Open_prev = {"SPY":0,"AMZN":0,"MSFT":0,"AAPL":0,"BTCUSDT":0}
Previous = {"SPY":0,"AMZN":0,"MSFT":0,"AAPL":0,"BTCUSDT":0}

c.execute("CREATE TABLE IF NOT EXISTS Prev_Data (Symbol text,i real,Close_prev real,length real,Open_prev real, Previous real)")
conn.commit()

for ticker in tickers:
    c.execute("CREATE TABLE IF NOT EXISTS {} (Date real,Open real,Close real,Volume real, Conditon real)".format(ticker))
    conn.commit()
        
for ticker in tickers:
    with conn:
        c.execute("SELECT * FROM Prev_Data WHERE Symbol='{}'".format(ticker))
        p = c.fetchall()
        if len(p) != 0:
            i[ticker] = p[-1][1]
            Close_prev[ticker] = p[-1][2]
            length[ticker] = p[-1][3]
            Open_prev[ticker] = p[-1][4]
            Previous[ticker] = p[-1][5]
            

def on_message(ws, message):
    global i, Close_prev, Open_prev, Previous
    # print(message)
    # global length
    Symbol = json.loads(message)["data"][0]["s"]
    if (Symbol == "BINANCE:BTCUSDT"):
        Symbol = "BTCUSDT"
    Date = json.loads(message)["data"][0]["t"]
    Close = json.loads(message)["data"][0]["p"]
    Volume = json.loads(message)["data"][0]["v"]
    length = Close*0.7*0.01
    
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
            c.execute("INSERT INTO Prev_Data VALUES ('{}',{},{},{},{},{})".format(ticker, i[ticker], Close_prev[ticker], length[ticker], Open_prev[ticker], Previous[ticker]))
            c.execute("INSERT INTO {} VALUES ({},{},{},{},0)".format(ticker, 0, 0, 0, 0))
        # c.execute("SELECT * FROM {}".format(ticker))
        # print(c.fetchall())

def on_open(ws):
    tickers = ["SPY","AMZN","MSFT","AAPL","BINANCE:BTCUSDT"]
    for ticker in tickers:
        print('{{"type":"subscribe","symbol":"{}"}}'.format(ticker))
        ws.send('{{"type":"subscribe","symbol":"{}"}}'.format(ticker))

if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://ws.finnhub.io?token=bqmh11vrh5rc5ul5lhg0",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()

