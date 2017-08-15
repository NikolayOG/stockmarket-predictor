import re
import pandas as pd
import io
import requests
import time
import operator

with open('stock_symbols') as f:
    read_data = f.read()

# get all of the stock symbols
regex = re.compile('(?<=Symbol\|)([A-Z]*)?(?=}})')
symbols = re.findall(regex, read_data)

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


total_volume = {}
for symbol in symbols:
    try:
        df = google_stocks(symbol, startdate=(1,1,2015), enddate=(1,1,2017))
        total_volume[symbol] = df['Volume'].sum()
    except:
        pass

# sort them before putting them into the database
sorted_total_volume = sorted(total_volume.items(), key=operator.itemgetter(1), reverse=True)
    
import sqlite3
conn = sqlite3.connect('symbols_volumes.db')

c = conn.cursor()

c.execute('''CREATE TABLE symbols_by_volume
             (symbol text, volume real)''')

for pair in sorted_total_volume:
    c.execute("INSERT INTO symbols_by_volume VALUES ('" + str(pair[0]) + "','" + str(pair[1]) + "')")

conn.commit()