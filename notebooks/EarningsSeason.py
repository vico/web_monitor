
# coding: utf-8

# In[1]:

get_ipython().magic(u'matplotlib inline')
import pandas as pd
from pandas import DataFrame, Series
from pandas.io import sql

import numpy as np
import pymysql
from datetime import datetime, timedelta
from decimal import *
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
from plotly import __version__
from plotly.offline import download_plotlyjs, init_notebook_mode, iplot, plot
init_notebook_mode()
from decimal import *
import re
import math
from dateutil.parser import parse
from IPython.display import display, HTML
from earning_season import get_earning_df, get_jp_equity_pl
import json
__version__


# In[2]:

print(pd.__version__)


# In[3]:

start_date = '2017-04-24'
end_date = '2017-05-12'


# In[4]:

with open('config_hk.json') as f:
    conf = json.load(f)

con = pymysql.connect(host=conf['host'], user=conf['user'], passwd=conf['password'], port=conf['port']) #, database=conf['database'])


# In[5]:

earning_df = get_earning_df(con, start_date, end_date)


# In[6]:

earning_df['instrumentID'].size  #.unique().size


# In[7]:

# earning_df[earning_df['quick']=='5110']


# In[8]:

# t = earning_df[earning_df['quick']=='5110'].index + pd.Timedelta(hours=9) + cday*0
# print(t)
# t.normalize()


# In[9]:

# position_df = pd.read_sql("""
#     select a.instrumentID, a.processDate, SUM(a.quantity) as quantity
#        FROM hkg02p.t05PortfolioPosition a
#        WHERE a.processDate >= '2017-01-01' AND a.processDate <= '2017-03-31' AND a.equityType='EQ'
#        GROUP BY a.instrumentID, a.processDate
# """, con)


# In[10]:

# position_df


# In[11]:

pl_df = get_jp_equity_pl(con, start_date, end_date)


# In[12]:

pl_df.head()


# In[13]:

def filter_after_earning(df):
    df = df.copy()
    # filter trades after earning announcement date
    df = df[df['processDate'] > df['datetime']]
    return df

pos_attr = (
    pl_df
    .merge(earning_df.reset_index(), how='left',
           left_on = ['instrumentID'],
           right_on = ['instrumentID']
          )
    .dropna()
    .pipe(filter_after_earning)
    .groupby(['instrumentID', 'advisor'])
    .sum()[['RHAttribution', 'YAAttribution', 'LRAttribution', 'RHMktAlpha', 'YAMktAlpha', 'LRMktAlpha']]
    .reset_index()
    .rename(columns={ 'RHAttribution': 'PosRHAtt', 'YAAttribution': 'PosYAAtt', 'LRAttribution': 'PosLRAtt',
                      'RHMktAlpha': 'PosRHAlph', 'YAMktAlpha': 'PosYAAlph', 'LRMktAlpha': 'PosLRAlph'
                    })
    
)
pos_attr.head()


# In[14]:

raw_df = (earning_df
          .reset_index()
          .merge(pl_df, how='inner', 
                 left_on=['datetime', 'instrumentID'], 
                 right_on=['processDate', 'instrumentID']
                )
          .merge(pos_attr, how='left',
                left_on=['instrumentID', 'advisor'],
                right_on=['instrumentID', 'advisor'])
         )
raw_df.head()


# In[15]:

raw_df.tail()


# In[16]:

#ax = raw_df.groupby('datetime').sum()['RHAttribution'].plot(kind='bar', figsize=(20,8))
fig, ax = plt.subplots(1, 1, figsize=(20, 8))
rh_pl = raw_df.groupby('datetime').sum()['RHAttribution']
rh_pl.plot(ax=ax, kind='bar', title="RH Earning season PL")
yticks = ax.get_yticks()
ax.set_yticklabels(['{:3.2f}%'.format(x*100) for x in yticks])
xticks = rh_pl.index
ax.axhline(0, color='k', linestyle='-', linewidth=1)
ax.set_xticklabels([dtz.strftime('%Y-%m-%d') for dtz in xticks], rotation=45)
plt.tight_layout()
# ax.set_xticks(raw_df['datetime'].map(lambda x :x.strftime('%Y-%m-%d')).values)


# In[17]:

grouped = raw_df.groupby('datetime')
def win_lose(s, column):
    win = len(s[s[column] > 0])
    lose = -len(s[s[column] < 0])
    return pd.Series([win, lose], index=['win', 'lose'])


# In[18]:

rh_win = grouped.apply(win_lose, ('RHAttribution'))
#

fig, ax = plt.subplots(1, 1, figsize=(18, 8))
rh_win.plot(ax=ax, kind='bar', grid=True, zorder=2, title="RH Win lose")
yticks = ax.get_yticks()
#ax.set_yticklabels(range(int(math.floor(yticks.min())), int(math.ceil(yticks.max()))))
ax.axhline(0, color='k', linestyle='-', linewidth=1)
ax.yaxis.set_major_locator(MaxNLocator(integer=True))
xticks = rh_win.index
ax.set_xticklabels([dtz.strftime('%Y-%m-%d') for dtz in xticks], rotation=45)
plt.tight_layout()


# In[34]:

# debug BBG data changed
print(raw_df.set_index('datetime')
      .loc['2017-05-12'][['quick', 'RHAttribution', 'YAAttribution', 'LRAttribution']]
      .sort_values(by='quick')
     # .applymap(lambda x: '{:.2f%}'.format(x) if x < 1 else x)
     )
print(raw_df.set_index('datetime').loc['2017-05-12'][['quick']].count())
# t = raw_df.set_index('datetime').loc['2017-05-11']
# t[t['RHAttribution'] > 0][['quick', 'RHAttribution']].sort_values(by='quick')


# In[27]:

ya_win = grouped.apply(win_lose, ('YAAttribution'))

fig, ax = plt.subplots(1, 1, figsize=(18, 8))
ya_win.plot(ax=ax, kind='bar', grid=True, zorder=2, title="YA Win Lose")

ax.axhline(0, color='k', linestyle='-', linewidth=1)
ax.yaxis.set_major_locator(MaxNLocator(integer=True))
xticks = ya_win.index
ax.set_xticklabels([dtz.strftime('%Y-%m-%d') for dtz in xticks], rotation=45)
plt.tight_layout()


# In[21]:

fig, ax = plt.subplots(1, 1, figsize=(20, 8))
ya_pl = raw_df.groupby('datetime').sum()['YAAttribution']
ya_pl.plot(ax=ax, kind='bar', title="YA Earning season PL")
yticks = ax.get_yticks()
ax.set_yticklabels(['{:3.2f}%'.format(x*100) for x in yticks])
ax.axhline(0, color='k', linestyle='-', linewidth=1)
xticks = rh_pl.index
ax.set_xticklabels([dtz.strftime('%Y-%m-%d') for dtz in xticks], rotation=45)
plt.tight_layout()


# In[22]:

raw_df.groupby('side').sum()[['RHAttribution', 'RHMktAlpha']]*100


# In[23]:

raw_df.groupby('advisor').sum()[['RHAttribution', 'RHMktAlpha']]*100


# In[24]:

raw_df.groupby('TPX').sum()[['RHAttribution', 'RHMktAlpha']]*100


# In[25]:

(raw_df.groupby(['name', 'side']).sum()[['RHAttribution', 'RHMktAlpha']]*100).head()

