
# -*- coding: utf-8 -*-

import datetime
import MySQLdb as mdb
import mysql.connector
import bs4 as bs
import pandas as pd
import pandas_datareader.data as web
from urllib.request import urlopen
import quandl
import time
quandl.ApiConfig.api_key = "Y9_EnCeHAc1ZWrxc3U2J"


# Obtain a database connection to the MySQL instance
db_host = 'localhost'
db_user = 'sec_user'
db_pass = 'hedgedluck'
db_name = 'securities_master'

con = mysql.connector.connect(host = db_host, user = db_user, passwd = db_pass, database = db_name)

def obtain_db_tickers():
    """ Obtains a list of the ticker symbols in the database."""
    cur = con.cursor()
    cur.execute("SELECT id, ticker FROM symbol")
    data = cur.fetchall()

    return [(d[0], d[1]) for d in data]

def get_daily_historic_data(ticker, start_date = datetime.datetime(2000, 1, 1), end_date = datetime.date.today()):
    """ Obtains data from Quandl until 3/27/18 and returns a list of tuples.
        ticker: ticker symbol
        start_date: Start date in (YYYY, M, D) format
        end_date: End date in (YYYY, M, D) format"""

    #Construct the Yahoo URL with the correct integer query parameters

    #Try connecting to Yahoo Finance to obtain the data
    #On failure, print an error message
    try:
        table = quandl.get('WIKI/' + ticker, start_date = start_date, end_date = end_date, paginate = True)
        #data = quandl.get_table('MER/F1', ticker = 'ABT', date = { 'gte': end_date, 'lte': start_date })
        prices = table.drop(['Adj. Volume', 'Adj. Open','Adj. High','Adj. Low','Ex-Dividend','Split Ratio'], axis = 1)
        prices.reset_index(inplace = True)

    except Exception as e:
        print("Could not download Quandl data: %s" % e)

    return prices

def insert_daily_data(data_vendor_id, symbol_id, daily_data):
    """ Takes a list of tuples of daily data and adds it to the
        MySQL database. Appends the vendor ID and symbol ID to the data.

        daily_data: List of tuples of the OHLC data (with adj_close and volume)"""

    # Create the time now
    now = datetime.datetime.utcnow()

    # Append the data to include the vendor ID and symbol ID
    daily_data['data_vendor_id'] = data_vendor_id
    daily_data['created_date'] = now
    daily_data['last_updated_date'] = now
    daily_data['symbol_id'] = symbol_id
    daily_data = daily_data[['data_vendor_id', 'symbol_id', 'Date', 'created_date', 'last_updated_date', 'Open', 'High', 'Low', 'Close', 'Adj. Close', 'Volume']]

    # Create insert strings
    column_str = """ data_vendor_id, symbol_id, price_date, created_date,
                    last_updated_date, open_price, high_price, low_price,
                    close_price, volume, adj_close_price"""

    insert_str = ("%s, " * 11)[:-2]
    final_str = "INSERT INTO daily_price (%s) VALUES (%s)" % (column_str, insert_str)

    # Using the MySQL connection, carry out an INSERT INTO for every symbol
    cur = con.cursor()
    cur.execute(final_str, daily_data, multi = True)


class GetOutOfLoop( Exception ):
    pass

if __name__ == "__main__":
    # Loop over the tickers and insert the daily historical data into the database
    counter = 1
    tickers = obtain_db_tickers()
    try:
        for t in tickers:
            if counter == 55:
                con.commit()
                con.close()
                raise GetOutOfLoop
            else:
                counter += 1
            print("Adding data for %s" % t[1])
            quandl_data = get_daily_historic_data(t[1])
            insert_daily_data('1', t[0], quandl_data)

            # if t[0] % 10 == 0:
            #     time.sleep(30)
    except GetOutOfLoop:
        pass
