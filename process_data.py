import sqlite3
import pandas as pd
import io
import requests
import time

def google_stocks(symbol, startdate = (1, 1, 2005), enddate = None):
 
    startdate = str(startdate[0]) + '+' + str(startdate[1]) + '+' + str(startdate[2])
 
    if not enddate:
        enddate = time.strftime("%m+%d+%Y")
    else:
        enddate = str(enddate[0]) + '+' + str(enddate[1]) + '+' + str(enddate[2])
 
    stock_url = "http://www.google.com/finance/historical?q=" + symbol + \
                "&startdate=" + startdate + "&enddate=" + enddate + "&output=csv"
 
    raw_response = requests.get(stock_url).content
 
    stock_data = pd.read_csv(io.StringIO(raw_response.decode('utf-8')))
 
    return stock_data




def top_nth_volume_stocks(n, startdate = (1,1,2016), enddate = (1,1,2017)):
    conn = sqlite3.connect('symbols_volumes.db')
    c = conn.cursor()
    
    symbols = c.execute('SELECT * FROM symbols_by_volume')
    most_volume_symbols = symbols.fetchall()[:n]
    
    top_volume_stocks = []
    
    for symbol in most_volume_symbols:
        top_volume_stocks.append(google_stocks(symbol[0], startdate=startdate, enddate=enddate))
    
    return top_volume_stocks





    



