
# coding: utf-8

# In[1]:

get_ipython().magic(u'matplotlib inline')
get_ipython().magic(u'load_ext watermark')
import pandas as pd
from pandas import DataFrame, Series
from pandas.io import sql
import japandas as jpd

import numpy as np
import pymysql
from datetime import datetime, timedelta
from decimal import *
from plotly import __version__
from plotly.offline import download_plotlyjs, init_notebook_mode, iplot, plot
init_notebook_mode()
from decimal import *
import re
from dateutil.parser import parse
from IPython.display import display, HTML
import json
print(pd.__version__)

figsize=(20, 8)


# In[19]:

# %watermark


# In[20]:

# start_date =''


# In[4]:

with open('config.json') as f:
    conf = json.load(f)

con = pymysql.connect(host=conf['host'], user=conf['user'], passwd=conf['password']) #, database=conf['database'])


# In[5]:

adv_attr = pd.read_sql("""
SELECT
  a.processDate, a.advisor,
  SUM(a.RHAttribution) * 100 AS Attribution,
  SUM(a.RHMktAlpha) * 100    AS Alpha
FROM hkg02p.t05PortfolioResponsibilities AS a
WHERE a.processDate >= '2014-01-01' AND a.advisor <> ''
GROUP BY a.processDate, a.advisor
""", con, parse_dates=['processDate'], index_col='processDate')


# In[6]:

sector_map = {
    'AP': 'Autos',
    'TI': 'Machinery',
    'CS': 'Tech',
    'PK': 'Prior',
    'AO': 'Financials',
    'HA': 'HA',
    'Bal': 'Balance',
    'AQ': 'Asia',
    'Adv': 'Others',
    'DH': 'Asia',
    'DL': 'Asia',
    'EL': 'Asia',
    'KW': 'Asia',
    'NJD': 'Asia',
    'PK-A': 'Asia',
    'RW': 'RW',
    'SJ': 'Internet',
    'SM': 'Retail',
    'TT': 'Internet',
    'CET': 'Tech',
    'KOt': 'Prior',
    'NJA': 'Asia',
    'TNi': 'Machinery',
    '': 'N.A.'
}


# ## Attribution and alpha per sector 2017 YTD

# In[7]:

(adv_attr['2017-01-01':]
 .assign(adv_sector = lambda x: x['advisor'].apply(lambda y: sector_map[y]))
 .groupby('adv_sector')
 .sum()
 .applymap(lambda x: '{:.2f}%'.format(x))
)


# ## Attribution and alpha per sector 2016

# In[8]:

(adv_attr['2016-09-22':'2016-12-31']
 .assign(adv_sector = lambda x: x['advisor'].apply(lambda y: sector_map[y]))
 .groupby('adv_sector')
 .sum()
 .applymap(lambda x: '{:.2f}%'.format(x))
)


# In[9]:

index_df = pd.read_sql("""
SELECT a.priceDate, c.indexCode, a.close, b.rate AS JPY
FROM hkg02p.t06DailyIndex a
  INNER JOIN hkg02p.t06DailyCrossRate b ON a.priceDate=b.priceDate
    AND b.base="USD" AND b.quote="JPY"
  INNER JOIN hkg02p.t07Index c ON a.indexID=c.indexID
WHERE a.priceDate>'2012-06-03' AND (a.indexID=1 Or a.indexID=1524)
""", con, parse_dates=['priceDate'])


# In[10]:

# index = get_index_df('2012-06-03', '2017-04-27', con)
# index.unstack('indexCode')
# print(index_df.head())
# print(index_df.tail())
tpx = index_df[index_df['indexCode'] == 'TPX'][['priceDate', 'close']].set_index('priceDate')
tpxtr = index_df[index_df['indexCode'] == 'TPXDDVD'][['priceDate', 'close']].set_index('priceDate')
jpy = index_df[index_df['indexCode'] == 'TPX'][['priceDate', 'JPY']].set_index('priceDate')


# In[17]:

ax = (tpx.pct_change().dropna()*100).rolling(20).std().dropna().plot(kind='line', figsize=figsize, title='Rolling Daily Volatility - Topix & Yen', label='Topix 20D Vol')
_ = (jpy.pct_change().dropna()*100).rolling(20).std().dropna().plot(kind='line', ax=ax, figsize=figsize, label='Yen 20D Vol')


# In[12]:

attribution = pd.read_sql("""
SELECT
  a.processDate,
  SUM(a.RHAttribution) * 100 AS Attribution,
  SUM(a.RHMktAlpha) * 100    AS Alpha
FROM hkg02p.t05PortfolioResponsibilities AS a
WHERE a.processDate >= '2016-01-01'
GROUP BY a.processDate
""", con, parse_dates=['processDate'], index_col='processDate')


# In[13]:

att_cum = attribution.cumsum()
_ = att_cum.plot(figsize=figsize)


# In[14]:

exposure = pd.read_sql("""
SELECT
  a.processDate,
  SUM(a.RHExposure) * 100                                           AS GrossExposure,
  SUM(IF(a.side = "L", a.RHExposure, -a.RHExposure)) * 100          AS NetExposure,
  SUM(IF(a.side = "L", a.RHExposure, -a.RHExposure) * a.beta) * 100 AS NetBeta
FROM hkg02p.t05PortfolioResponsibilities AS a
WHERE a.processDate >= '2016-01-01'
GROUP BY a.processDate;
""", con, parse_dates=['processDate'], index_col='processDate')


# In[18]:

# 21st September BOJ Comprehensive Assessment
boj_asses_date = '2016-09-21'
_ = exposure[boj_asses_date:].plot(figsize=figsize)


# In[16]:

# con.close()

