
# coding: utf-8

# In[21]:

# %matplotlib inline
import pandas as pd
from pandas import DataFrame
from pandas.io import sql
from pandas.tseries.offsets import *
import numpy as np
# import pylab as plt
import pymysql
# import seaborn
# from matplotlib import pyplot
from datetime import datetime,timedelta
import csv
import math
pd.options.display.float_format = '{:,.2f}'.format


# In[22]:

con = pymysql.connect(host='localhost', user='root', passwd='root', db='hkg02p')


# In[23]:

ytd_trades = sql.read_sql("""
SELECT a.code, a.fundCode, a.orderType, a.side, a.swap, a.tradeDate,
  # a.commission,
  (@commInUSD := a.commission*f.rate) AS commInUSD,
  (@commrate := IF (g.commrate IS NOT NULL, ROUND(g.commrate,4), ROUND(d.brokerCommissionRate,4))),
  (@jpresearch := IF(b.currencyCode="JPY" AND c.instrumentType="EQ" AND  (@commrate=0.0015), 
      @commInUSD*11/15,0)*1.0) AS JPResearch,
      IF(b.currencyCode="JPY" AND c.instrumentType="EQ" AND (@commrate=0.0004), 
          @commInUSD, 
          0)*1.0 AS JPDis,
  (@clearing:=
  IF (c.instrumentType IN ("FU", "OP"), 
      IF(SUBSTRING(a.code, 1, 2) IN ("TP", "NK"), 500, 
        IF(SUBSTRING(a.code, 1, 2)="JP",50, 
		IF(SUBSTRING(a.code, 1, 2) IN ("HC", "HI"), 30, 0) 
	)
   ) * a.quantity *f.rate, 0  )) AS Clearing,
  IF(b.currencyCode="JPY" AND c.instrumentType="EQ" AND (@commrate=0.0004 OR @commrate=0),
      0,
      IF(b.currencyCode="JPY" AND c.instrumentType="EQ",
      @commInUSD - @jpresearch,
      IF(b.currencyCode="JPY" AND c.instrumentType IN ("FU", "OP"), 
      @commInUSD - @clearing,0) )) AS JPExec,
  d.brokerCode, c.instrumentType, b.currencyCode,
  IF(d.brokerCode="BXS", "Soft", IF(d.brokerCode="INSH", "Nomura",e.name)) AS brokerName,
  (@tax := CASE CONCAT(c.instrumentType, a.orderType, b.currencyCode)
            WHEN "EQBCNY" THEN 0.000098
            WHEN "EQSCNY" THEN 0.001098
            WHEN "EQBHKD" THEN 0.00108
            WHEN "EQSHKD" THEN 0.00108
            WHEN "EQSTWD" THEN 0.003
            WHEN "EQSKRW" THEN 0.003
            WHEN "EQBSGD" THEN 0.0004
            WHEN "EQSSGD" THEN 0.0004
            ELSE 0
            END
  ) AS tax,
  (@asiadeal := IF (b.currencyCode <> "JPY" AND c.instrumentType="EQ" AND d.brokerCommissionRate > 0.01,
                    a.gross * f.rate * (d.brokerCommissionRate - @tax ), 0)) AS asiaDeal,
  (@asiaResearch := IF(b.currencyCode <> "JPY" AND c.instrumentType="EQ" AND @asiadeal=0, 
    IF(d.brokerCommissionRate-@tax-0.0005>= 0, d.brokerCommissionRate-@tax-0.0005, 0) *f.rate*a.gross, 0)) AS asiaResearch,
  IF (b.currencyCode <> "JPY" AND c.instrumentType="EQ" AND @asiadeal=0,
    0.0005 * f.rate * a.gross,
    IF (b.currencyCode <> "JPY" AND c.instrumentType IN ("FU", "OP"), @commInUSD-@clearing, 0)
  ) AS asiaExecution,
  IF (a.swap="SWAP", @asiaResearch, 0)*1.0 AS HCSwaps
FROM t08Reconcile a
  INNER JOIN t01Instrument c ON a.code = c.quick
  INNER JOIN t02Currency b ON b.currencyID = c.currencyID
  INNER JOIN t08Reconcile d ON a.matchDoric = d.primaryID # a.primaryID = d.matchBrokers AND d.srcFlag ="D"
  INNER JOIN t06DailyCrossRate f ON f.priceDate=a.tradeDate AND f.base=b.currencyCode AND f.quote="USD"
  LEFT JOIN t02Broker e ON e.brokerCode = a.brokerCode
  left join (select a.code, a.orderType, a.side, a.swap, a.tradeDate, a.settleDate, a.brokerCode, MAX(a.brokerCommissionRate) as commrate
from t08Reconcile a
where a.status="A" and a.srcFlag="D" and a.tradeDate > '2015-12-31'
group by a.code, a.orderType, a.side, a.swap, a.tradeDate, a.settleDate, a.brokerCode
  ) g ON a.code=g.code and a.orderType=g.orderType and a.side=g.side and a.swap=g.swap and a.tradeDate=g.tradeDate
          and a.brokerCode=g.brokerCode
WHERE a.tradeDate>'2015-12-31' AND a.srcFlag="K"
ORDER BY a.tradeDate, a.code;
""", con, parse_dates=['tradeDate'],index_col='tradeDate' )

q2 = ytd_trades['2016-04-01':'2016-06-30']


# In[24]:

# Asia commission budget is 2mm  USD per year
# Japan has gone through a few iterations but it's basically approximately 7.5 mm USD per year
jp_annual_commission_budget = 7500000  # usd
asia_annual_commission_budget = 2000000 # usd
jp_quarter_commission_budget = jp_annual_commission_budget / 4.0
asia_quarter_commission_budget = asia_annual_commission_budget / 4.0

jp_quarter_commission_budget = 1880911  # to make it same with old system


# In[25]:

# broker rank for each quarter
data = {
    'brokers': ['BAML', 'Mizuho Securities', 'AdvancedResearch', 'Nomura', 'Japan Equity Research', 
                'Citi', 'Mitsubishi UFJ', 'Tokai', 'Ichiyoshi', 'SMBC Nikko', 
                'BNP', 'Deutsche', 'CLSA', 'Daiwa', 'Jefferies', 
                'CS', 'UBS', 'JP Morgan', 'Goldman Sachs', 'Okasan', 
                'MS', 'Macquarie'
               ],
    'rank': [6, 3, 21, 2, 17, 8, 7, 16, 15, 4, 20, 19, 22, 1, 14, 9, 12, 5, 9, 17, 9, 13],
    'research': map(lambda x: x*100, [0.0716, 0.0951, 0, 0.1014, 0,
                 0.0643, 0.0691, 0.0135, 0.0143, 0.0801,
                 0.008, 0.0096, 0.0006, 0.1104, 0.0193,
                 0.0565, 0.0463, 0.0724, 0.0564, 0.0116,
                 0.0563, 0.0298
                ])
}

jp_ranks_df = pd.DataFrame(data).set_index('brokers')
jp_ranks_df
# jp_ranks = pd.Series(jpranks)
# jp_ranks


# In[26]:

(q2 #.assign(JPExec = lambda x: x.commInUSD-x.JPResearch if x.currencyCode=='JPY' else x.Clearing)
   .groupby(['brokerName', 'currencyCode'])
   .sum()
   .loc[(slice(None), 'JPY'),['commInUSD', 'JPResearch', 'JPExec', 'JPDis',  'Clearing']]
   .reset_index()
   .drop('currencyCode', axis=1)
   .set_index('brokerName')
   .merge(jp_ranks_df, how='right', left_index=True, right_index=True)
   .fillna(0)
   .sort_values(by='rank', axis=0)
  .assign( res_target = lambda df: df['research'] * jp_quarter_commission_budget / 100)
  .assign( balance_usd = lambda df: df['res_target']/3.0 - df['JPResearch'])
)


# In[27]:

(q2[q2['currencyCode'] != "JPY"]
 .groupby('brokerName')
 .sum()[['asiaResearch', 'asiaExecution', 'HCSwaps']]
# .reset_index()
 )


# In[28]:

currency_mask = ytd_trades['currencyCode'] == 'JPY'
(ytd_trades.groupby(['brokerName', currency_mask])
           .sum()
           .assign(AsiaYTD = lambda x : x.asiaResearch + x.asiaExecution)
           .unstack()
           .fillna(0) [['JPResearch', 'JPExec', 'Clearing', 'commInUSD'
                        , 'AsiaYTD'
                       ]]
           .rename(columns={True: 'JP', False: 'Asia'})
         #.loc[(slice(None), 'JPY'), ['JPResearch', 'JPExec', 'commInUSD']]
)


# In[29]:

# (ytd_trades.groupby(['brokerName', 'currencyCode'])
#            .sum()
#            #.assign(AsiaYTD = lambda x : x.asiaResearch + x.asiaExecution)
#            .unstack()
#            .fillna(0) [['commInUSD', 'JPResearch', 'JPExec', 'Clearing'
#             #            , 'AsiaYTD'
#                        ]]
#            .rename(columns={True: 'JP', False: 'Asia'})
#          #.loc[(slice(None), 'JPY'), ['JPResearch', 'JPExec', 'commInUSD']]
# )


# In[30]:

(q2[q2['currencyCode'] != "JPY"]
   .groupby(['brokerName', 'currencyCode'])
   .sum()[['asiaResearch']]
   .unstack()
   .fillna(0)
)


# In[31]:

# broker rank for each quarter
data = {
    'brokers': ['BAML', 'Mizuho Securities',  'Nomura', 'Japan Equity Research', 
                'Citi', 'Mitsubishi UFJ', 'Tokai', 'Ichiyoshi', 'SMBC Nikko', 
                'BNP', 'Deutsche', 'CLSA', 'Daiwa', 'Jefferies', 
                'CS', 'UBS', 'JP Morgan', 'Goldman Sachs', 'Okasan', 
                'MS', 'Macquarie'
               ],
    'rank': [6, 1, 2, 20, 9, 4, 18, 16, 3, 14, 19, 22, 5, 15, 11, 10, 12, 7, 17, 8, 21],
    'research': map(lambda x: x, [7.55, 10.26, 8.69, 0.81,
                 6.47, 7.95, 1.16, 1.36, 8.1,
                 2.19, 0.99, 0.13, 7.94, 1.46,
                 5.72, 8.18, 4.98, 6.99, 1.2,
                 6.86, 0.63
                ])
}

jp_ranks_df = pd.DataFrame(data).set_index('brokers')
jp_ranks_df


# In[32]:

jp_quarter_commission_budget =10031527/4.0
jp_quarter_commission_budget


# For monthly balance need to divided res_target by 3 to find balance, but for quarter calculation, do not need to divide by 3.

# In[33]:

# q1[q1['brokerCode']=='MSUS']


# In[34]:

# q1 = ytd_trades['2016-02-05':'2016-02-08']
# q1 = ytd_trades['2016-02-22':'2016-02-22']
q1 = ytd_trades['2016-01-01':'2016-03-31']

(q1 #.assign(JPExec = lambda x: x.commInUSD-x.JPResearch if x.currencyCode=='JPY' else x.Clearing)
   .groupby(['brokerName', 'currencyCode'])
   .sum()
   .loc[(slice(None), 'JPY'),['commInUSD', 'JPResearch', 'JPExec', 'JPDis', 'Clearing']]
   .reset_index()
   .drop('currencyCode', axis=1)
   .set_index('brokerName')
   .merge(jp_ranks_df, how='right', left_index=True, right_index=True)  # some names removed like Barclays and Softs
   .fillna(0)
   .sort_values(by='rank', axis=0)
  .assign( res_target = lambda df: df['research'] * jp_quarter_commission_budget / 100)
  .assign( balance_usd = lambda df: df['res_target'] - df['JPResearch'])
)


# In[35]:

(q1[q1['currencyCode'] != "JPY"]
   .groupby(['brokerName'])
   .sum()[['asiaResearch', 'asiaExecution', 'HCSwaps']]
   .assign(asiaYTD = lambda x: x.asiaResearch + x.asiaExecution)
   # ['commission', 'commInUSD', 'JPResearch', 'JPExec', 'Clearing']
)


# In[36]:

(q1[q1['currencyCode'] != "JPY"]
 .groupby(['brokerName', 'currencyCode'])
 .sum()[['asiaResearch']]
 .unstack()
 .fillna(0)
 )


# In[37]:

# q2jpm = ytd_trades #.loc['2017-04-03':]  #,ytd_trades['brokerCode'] == 'JPMF']
# q2jpm[(q2jpm['brokerCode'] == 'RHBO') & (q2jpm['currencyCode'] != 'JPY')]


# In[38]:

# from IPython.display import display, HTML
#HTML(q1[(q1['currencyCode'] == 'HKD') & (q1['brokerCode'] == 'MERT')].to_html())
#HTML(q1[(q1['currencyCode'] != 'JPY') & (q1['brokerName'] == 'Nomura')].to_html())
# q1[(q1['currencyCode'] != 'JPY') & (q1['brokerName'] == 'UBS')].count()

