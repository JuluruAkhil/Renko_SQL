# -*- coding: utf-8 -*-
"""
Created on Wed May  6 14:21:04 2020

@author: EternalBliss
"""

# http://188.166.204.146:4000/

import json
import sqlite3
import requests
import time


def Request_Data():
    headers = {"Accept-Encoding": "gzip","User-Agent": "okhttp/3.10.0"}
    r = requests.get("http://163.47.9.131:4000/api/scrips/feed?channel_names=nse_cm_nifty_50%20nse_cm_nifty_bank%20nse_cm_reliance%20nse_cm_hdfcbank%20nse_cm_tatasteel", headers = headers)
    j = json.loads(r.text)
    for Symbol in j:
        j[Symbol] = float(j[Symbol].split("|")[0])
    return j

conn = sqlite3.connect("Renko_Nse_0.5.db")
c = conn.cursor()

tickers = ["nse_cm_reliance","nse_cm_nifty_bank","nse_cm_nifty_50","nse_cm_tatasteel","nse_cm_hdfcbank"]

i = {"nse_cm_reliance":0,"nse_cm_nifty_bank":0,"nse_cm_nifty_50":0,"nse_cm_tatasteel":0,"nse_cm_hdfcbank":0}
Close_prev = {"nse_cm_reliance":0,"nse_cm_nifty_bank":0,"nse_cm_nifty_50":0,"nse_cm_tatasteel":0,"nse_cm_hdfcbank":0}
length = {"nse_cm_reliance":0,"nse_cm_nifty_bank":0,"nse_cm_nifty_50":0,"nse_cm_tatasteel":0,"nse_cm_hdfcbank":0}
Open_prev = {"nse_cm_reliance":0,"nse_cm_nifty_bank":0,"nse_cm_nifty_50":0,"nse_cm_tatasteel":0,"nse_cm_hdfcbank":0}
Previous = {"nse_cm_reliance":0,"nse_cm_nifty_bank":0,"nse_cm_nifty_50":0,"nse_cm_tatasteel":0,"nse_cm_hdfcbank":0}

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
            
def main():
    while (True):
        d = Request_Data()
        global i, Close_prev, Open_prev, Previous
        # global length
        for Symbol in d:
            Date = int(time.time())
            Close = d[Symbol]
            Volume = 0
            length = Close*0.5*0.01
            
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
        
        
try:
    main()
except KeyboardInterrupt:
    print("### closed ###\n")
    for ticker in tickers:
        with conn:
            c.execute("INSERT INTO Prev_Data VALUES ('{}',{},{},{},{},{})".format(ticker, i[ticker], Close_prev[ticker], length[ticker], Open_prev[ticker], Previous[ticker]))
            c.execute("INSERT INTO {} VALUES ({},{},{},{},0)".format(ticker, 0, 0, 0, 0))
        # c.execute("SELECT * FROM {}".format(ticker))
        # print(c.fetchall())
