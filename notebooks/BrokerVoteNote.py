
# coding: utf-8

# In[1]:

# %matplotlib inline
# %load_ext watermark
import pandas as pd
from pandas import DataFrame, Series
from pandas.io import sql
import japandas as jpd
from pandas.tseries.offsets import CustomBusinessDay, CustomBusinessMonthEnd

import numpy as np
import pymysql
from datetime import datetime, timedelta
from decimal import *
from plotly import __version__
from plotly.offline import download_plotlyjs, init_notebook_mode, iplot
init_notebook_mode()
from decimal import *
import re
from dateutil.parser import parse
from IPython.display import display, HTML
import json
print(pd.__version__)

figsize=(20, 8)


# In[2]:

with open('config.json') as f:
    conf = json.load(f)

con = pymysql.connect(host=conf['host'], user=conf['user'], passwd=conf['password'], port=conf['port']) #, database=conf['database'])


# In[3]:

broker_votes = pd.read_sql("""
SELECT d.objValue AS region, IF(lastName='Nakamura','Trading',IF(lastName='Wan','Borrow',lastName)) AS analyst,c.objValue AS broker,analystVote/100 AS vote
        FROM
        hkg02p.t08BrokerVote a,hkg02p.t01Person b, hkg02p.t10ObjectText c, hkg02p.t10ObjectText d
        WHERE
        brokerVoteDate='2016-12-01'
        AND a.personID=b.personID
        AND a.brokerVoteBrokerID=c.objectTextID
        AND a.regionID = d.objectTextID
        AND analystVote>0
        ORDER BY lastName,regionID,broker
""", con)


# ### Vote budget

# In[4]:

vote_budget_data = {
    'analyst': ['Arigami', 'Borrow', 'Forday', 'Isozaki', 'Meguro', 'Phillips', 
                'Yamashita', 'Qi', 'Nishimi', 'Shima', 'Togo', 'Trading', 'Lu', 
                'Wakahara', 'Jo', 'Ohno', 'Esparza', 'Lian', 
               ],
    'jp': [100, 50, 100, 100, 100, 100, 50, 0, 100, 100, 100, 60, 0, 100, 100, 100, 40, 0],
    'asia': [0, 25, 0, 0, 0, 0, 25, 50, 50, 50, 0, 25, 50, 0, 0, 0, 0, 50]
}

vote_budget = pd.DataFrame(vote_budget_data)


# In[5]:

vote_budget


# ### Credit for Type of trades

# In[6]:

trade_credit = pd.DataFrame(
    {
        'type': ['Placement', 'IPO', 'Trade', 'CB'],
        'percent': [0.05, 0.1, 1.0, 0.02]
    }
)
trade_credit


# ### PL target jpy
# 
# Avg assets (in USD) * 0.26 / 4 quarters

# ### Quarter PL

# In[10]:

quarter_pl = pd.read_sql("""
SELECT a.name, a.quick, a.side, SUM(a.attribution) AS PL
FROM hkg02p.t05PortfolioResponsibilities a
WHERE
  a.processDate >= '2016-12-01' AND a.processDate <= '2017-02-28'
GROUP BY a.instrumentID, a.side
ORDER BY  SUM(a.attribution) DESC
""", con)
quarter_pl.head()


#  ### Broker evaluation from Adv

# In[11]:

pl_evaluation = pd.DataFrame({
    'year': [2017]*5,
    'quarter': [1]*5,
    'quick': ['9984', '9984', '9142', '5423', '5423'],
    'broker_id': [
        18,  # MUFJ
        22,  # CITI
        10,  # Mizuho
        6,  # Daiwa
        22
    ],
    'percent': [0.1, 0.1, 0.05, 0.05, 0.05]
})
pl_evaluation


# ### Percent used

# In[8]:

broker_votes.groupby(['region', 'analyst']).sum()


# In[ ]:



