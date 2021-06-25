#!/usr/bin/python
# -*- coding: utf-8 -*-

import datetime
import lxml.html
import MySQLdb as mdb
import mysql.connector
import bs4 as bs
from urllib.request import urlopen

from math import ceil

def obtain_parse_wiki_snp500():
    """Download and parse the Wikipedia list of S&P500 companies
    using requests and libxml.

    Returns a list of tuples to add to MySQL."""

    # Stores the current time, for the created_at record
    now = datetime.datetime.utcnow()

    # Use libxml to download the list of S&P500 companies and obtain the symbol
    url = 'http://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    page = urlopen(url).read()
    soup = bs.BeautifulSoup(page, 'html.parser')
    table = soup.find('table',{'class':'wikitable sortable'}).tbody
    rows = table.find_all('tr')

    # Obtain the symbol information for each row in the S&P500 table
    symbols = []
    for symbol in range(1,len(rows)):
        tds = rows[symbol].find_all('td')
        sd = {'ticker': tds[0].text.replace('\n', ''),
                'name': tds[1].text,
                'sector': tds[3].text}

        # Create a tuple (for the DB format) and append to the grand list
        symbols.append((sd['ticker'], 'stock', sd['name'],
                        sd['sector'], 'USD', now, now))

    return symbols

def insert_snp500_symbols(symbols):
    """ Insert the S&P500 symbols into the MySQL database."""

    # Connect to the MySQL instance
    db_host = 'localhost'
    db_user = 'sec_user'
    db_pass = 'hedgedluck'
    db_name = 'securities_master'

    con = mysql.connector.connect(host = db_host, user = db_user, passwd = db_pass, database = db_name)
    if con.is_connected():

        db_Info = con.get_server_info()
        print("Connected to MySQL database... MySQL Server version on ",db_Info)

        # Create the insert string
        column_str = "ticker, instrument, name, sector, currency, created_date, last_updated_date"
        insert_str = ("%s, " * 7)[:-2]
        final_str = "INSERT INTO symbol (%s) VALUES (%s)" % (column_str, insert_str)
        print(final_str, len(symbols))

        # Using the MySQL connection, carry out an INSERT INTO for every symbol
        cur = con.cursor()
        # This line avoids the MySQL MAX_PACKET_SIZE
        for i in range(0, int(ceil(len(symbols) / 100.0 ))):
            cur.executemany(final_str, symbols[i * 100: (i + 1) * 100 - 1])

        con.commit()
        cur.close()
        con.close()
        print("MySQL connection is closed")

if __name__ == "__main__":
    symbols = obtain_parse_wiki_snp500()
    insert_snp500_symbols(symbols)
