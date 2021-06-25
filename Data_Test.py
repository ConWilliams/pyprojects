import datetime
import numpy as np
import pandas as pd
# import pandas_datareader.data as web
# from urllib.request import urlopen
# import bs4 as bs
import quandl
quandl.ApiConfig.api_key = "Y9_EnCeHAc1ZWrxc3U2J"

start = datetime.datetime(2000, 1, 1)
end = datetime.date.today()

ticker = "AJG"
table = quandl.get('WIKI/' + ticker, start_date = start, end_date = end, paginate = True)
# url = "https://finance.yahoo.com/quote/AAPL/history?period1=946710000&period2=1559545200&interval=1d&filter=history&frequency=1d"
# page = urlopen(url).read()
# soup = bs.BeautifulSoup(page, 'html.parser')
# table = soup.find('table',{'class':'W(100%) M(0)'}).tbody
# rows = table.find_all('tr')
# print(rows)
print(table)
