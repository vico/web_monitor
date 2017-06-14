
# coding: utf-8

# In[1]:

#%matplotlib inline
import pandas as pd
from pandas import DataFrame
from pandas.io import sql
from pandas.tseries.offsets import *
import numpy as np
from pprint import pprint
# import pylab as plt
import pymysql
# import seaborn
# from matplotlib import pyplot
from datetime import datetime,timedelta
import csv
import math
pd.options.display.float_format = '{:,.2f}'.format
import json
from IPython.display import display, HTML
from sqlalchemy import create_engine


# In[2]:

ubs_include_list = ['Tokai', 'Japan Equity Research']


# In[3]:

with open('config.json') as f:
    conf = json.load(f)


# In[4]:

con = pymysql.connect(host=conf['host'], user=conf['user'], passwd=conf['password'], db=conf['database'])
engine = create_engine('mysql+pymysql://root:root@localhost:3306/hkg02p', echo=False)


# In[5]:

def get_trades(year):
    """
    return all trades from start of year
    :param year: the year to get trades
    :return: DataFrame of all trades of year
    """

    start_date = '{}-01-01'.format(year)
    
    query = """
        SELECT a.code, a.fundCode, a.orderType, a.side, a.swap, a.tradeDate, a.PB, 
              g.commrate as gcomrate, d.brokerCommissionRate,
              # a.commission,
              (@commInUSD := a.commission*f.rate) AS commInUSD,
              (@commrate := IF (g.commrate IS NOT NULL, ROUND(g.commrate,4), ROUND(d.brokerCommissionRate,4))) AS CommRate,
              (@jpresearch := IF(b.currencyCode IN ("JPY", "USD") AND c.instrumentType="EQ" AND  (@commrate<> 0.0004), 
                  @commInUSD*11/15,0)*1.0) AS JPResearch,
                  IF(b.currencyCode IN ("JPY", "USD") AND c.instrumentType="EQ" AND (@commrate=0.0004), 
                      @commInUSD, 
                      0)*1.0 AS JPDis,
              (@clearing:=
              IF (c.instrumentType IN ("FU", "OP"), 
                  IF(SUBSTRING(a.code, 1, 2) IN ("TP", "NK"), 500, 
                    IF(SUBSTRING(a.code, 1, 2)="JP",50, 
                    IF(SUBSTRING(a.code, 1, 2) IN ("HC", "HI"), 30, 0) 
                )
               ) * a.quantity *f.rate, 0  )) AS Clearing,
              IF(b.currencyCode IN ("JPY", "USD")  AND c.instrumentType="EQ" AND (@commrate=0.0004 OR @commrate=0),
                  0,
                  IF(b.currencyCode IN ("JPY", "USD")  AND c.instrumentType="EQ",
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
              (@asiadeal := IF (b.currencyCode  NOT IN ("JPY", "USD")  AND c.instrumentType="EQ" AND d.brokerCommissionRate > 0.01,
                                a.gross * f.rate * (d.brokerCommissionRate - @tax ), 0)) AS asiaDeal,
              (@asiaResearch := IF(b.currencyCode  NOT IN ("JPY", "USD")  AND c.instrumentType="EQ" AND @asiadeal=0, 
                IF(d.brokerCommissionRate-@tax-0.0005>= 0, d.brokerCommissionRate-@tax-0.0005, 0) *f.rate*a.gross, 0)) AS asiaResearch,
              IF (b.currencyCode NOT  IN ("JPY", "USD")  AND c.instrumentType="EQ" AND @asiadeal=0,
                0.0005 * f.rate * a.gross,
                IF (b.currencyCode NOT IN ("JPY", "USD")  AND c.instrumentType IN ("FU", "OP"), @commInUSD-@clearing, 0)
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
            where a.status="A" and a.srcFlag="D" and a.tradeDate >= {}
            group by a.code, a.orderType, a.side, a.swap, a.tradeDate, a.settleDate, a.brokerCode
              ) g ON a.code=g.code and a.orderType=g.orderType and a.side=g.side and a.swap=g.swap and a.tradeDate=g.tradeDate
                     and a.settleDate=g.settleDate  and d.brokerCode=g.brokerCode
            WHERE a.tradeDate>='{}' AND a.srcFlag="K"
            ORDER BY a.tradeDate, a.code;
        """.format(start_date, start_date)
    
#     print(query)

    ytd_trades = sql.read_sql(query, con, parse_dates=['tradeDate'], index_col='tradeDate')
    return ytd_trades


# In[6]:

def get_quarter_trades(year, quarter, ytd_trades):
    if quarter == 1:
        start = '{}-01-01'.format(year)
        end = '{}-03-31'.format(year)
    elif quarter == 2:
        start = '{}-04-01'.format(year)
        end = '{}-06-30'.format(year)
    elif quarter == 3:
        start = '{}-07-01'.format(year)
        end = '{}-09-30'.format(year)
    elif quarter == 4:
        start = '{}-10-01'.format(year)
        end = '{}-12-31'.format(year)
    else:
        return None
    return ytd_trades[start:end]


# In[7]:

def format_2f(df):
    t = df.copy()
    columns = ['JPResearch', 'JPExec', 'JPDis', 'res_target', 'balance_usd']
    t[columns] = t[columns].applymap(lambda x: '$ {:12,.0f}'.format(x) if x != 0 else '')
    t['balance_jpy'] = t['balance_jpy'].apply(lambda x: '¥ {:12,.0f}'.format(x) if x != 0 else '')
    t['rank'] = t['rank'].apply(lambda x: '{:.0f}'.format(x) if not np.isnan(x) else '')
    t['research'] = t['research'].apply(lambda x: '{:5.2f}%'.format(x) if x > 0 else '')
    t['accrued'] = t['accrued'].apply(lambda x: '{:5.0f}%'.format(x) if not np.isnan(x) else '')
    t['exec_target'] = t['exec_target'].apply(lambda x: '{:5.0f}%'.format(x) if x > 0 else '')
    return t


# In[8]:

def calculate_columns(quarter_trades, jp_ranks_df, jp_quarter_commission_budget, usd_jpy, 
                      ubs_include_list=[], clsa_asia_target=0):
    
    def zero_out_ubs_include(df, ubs_include_list):
        """ assume index is brokerName, and balance_usd column is already created
        """
        t = df.copy()
        for broker in ubs_include_list:
            t.loc[broker, 'balance_usd'] = 0
            
        return t
    
    rank_df = jp_ranks_df.copy()
    balance = rank_df['balance']
    for broker in ubs_include_list:
        rank_df.loc['UBS', 'research'] += rank_df.loc[broker, 'research']
    
    rank_df.loc['UBS', 'research'] += clsa_asia_target
        
    ranked_df = (quarter_trades
             .groupby(['brokerName', 'currencyCode'])
             .sum()
             .loc[(slice(None), ['JPY', 'USD']), ['JPResearch', 'JPExec', 'JPDis']]
             .groupby(level=0).sum()
             .reset_index()
             .set_index('brokerName')
             .merge(rank_df, how='right', left_index=True,
                    right_index=True)  # some names removed like Barclays and Softs
             .fillna(0)
             .sort_values(by='rank', axis=0)
                )
    bal = balance.reindex(ranked_df.index)
    table = (ranked_df
             .assign(res_target=lambda df: df['research'] * jp_quarter_commission_budget / 100 + (bal if bal is not None else 0) )
             .assign(balance_usd=lambda df: df['res_target'] - df['JPResearch'] )
             .pipe(zero_out_ubs_include, ubs_include_list)
             .assign(balance_jpy=lambda df: df['balance_usd'] * usd_jpy)
             .assign(accrued=lambda df: (df['JPExec'] + df['JPDis']) * 100 / (df['JPExec'].sum() + df['JPDis'].sum()))
             .reset_index()
             .set_index('rank')
             )
    
    return table


# In[9]:

def calculate_commission(quarter_trades, jp_ranks_df, jp_quarter_commission_budget, usd_jpy,
                        ubs_include_list=[], clsa_asia_target=0):
    
    table = calculate_columns(quarter_trades, jp_ranks_df, jp_quarter_commission_budget, usd_jpy, ubs_include_list, clsa_asia_target)

    exec_target = [11, 11, 10, 10, 10, 7, 7, 7, 7, 7, 3, 3, 3, 2, 1]
    if len(exec_target) < len(table.index):
        exec_target = exec_target + [0] * (len(table.index) - len(exec_target))
    table['exec_target'] = pd.Series(exec_target, index=table.index)
    
    return (table
            .reset_index()
            .append(table[['research', 'res_target', 'JPResearch', 'JPExec',
                           'JPDis', 'balance_usd', 'balance_jpy', 'exec_target']].sum(), ignore_index=True)
            .pipe(format_2f)
            [['rank', 'brokers', 'research', 'res_target', 'JPResearch', 'JPExec',
              'JPDis', 'balance_usd', 'balance_jpy', 'exec_target', 'accrued']]
            )


# In[10]:

def calculate_soft(quarter_trades, year, month):
    mon_trades = quarter_trades['{}-{:02d}'.format(year, month)]
    soft = "{:,.0f}".format(mon_trades[(mon_trades['brokerName'] == 'Soft')]['JPResearch'].sum())
    soft_ms = "{:,.0f}".format(mon_trades[mon_trades['brokerName'] == 'Soft']['JPExec'].sum())
    return soft, soft_ms


# In[11]:

def get_fx_rate(price_date):
    """ return tuple of fx rate for USDJPY and USDHKD
    """
    query = """SELECT a.quote, a.rate
                    FROM t06DailyCrossRate a
                    WHERE a.priceDate="{}" AND a.quote IN ("JPY", "HKD") AND a.base="USD"
    """

    t = pd.read_sql(query.format(price_date), con, index_col="quote")
    return t.loc['JPY', 'rate'], t.loc['HKD', 'rate']

get_fx_rate('2016-06-30')
# query = """SELECT a.quote, a.rate
#                     FROM t06DailyCrossRate a
#                     WHERE a.priceDate="{}" AND a.quote IN ("JPY", "HKD") AND a.base="USD"
#     """

# t = pd.read_sql(query.format('2016-06-30'), con, index_col="quote")
# type(t['rate']['JPY'])
# type(t.loc['JPY', 'rate'])


# ### new requirement
# need to restart balance_usd for specific broker from some month. for ex: MS from 2017 Jun

# In[12]:

date = '2016-06-30'

if date is None:
    date = datetime.date.today()
else:
    try:
        date = datetime.strptime(date, '%Y-%m-%d')
    except ValueError:
        date = datetime.date.today()

quarter = (date.month-1) // 3 + 1



# In[13]:

get_ipython().magic(u'time ytd_trades = get_trades(date.year)')


# In[14]:

date.year, quarter


# In[15]:

ytd_trades.head()


# In[16]:

# broker rank for each quarter - 2016Q1
# data = {
#     'brokers': ['BAML', 'Mizuho Securities',  'Nomura', 'Japan Equity Research', 
#                 'Citi', 'Mitsubishi UFJ', 'Tokai', 'Ichiyoshi', 'SMBC Nikko', 
#                 'BNP', 'Deutsche', 'CLSA', 'Daiwa', 'Jefferies', 
#                 'CS', 'UBS', 'JP Morgan', 'Goldman Sachs', 'Okasan', 
#                 'MS', 'Macquarie', 'Barclays'
#                ],
#     'rank': [6, 1, 2, 20, 9, 4, 18, 16, 3, 14, 19, 22, 5, 15, 11, 10, 12, 7, 17, 8, 21, 13],
#     'balance': [2240, -664, -808, 0,
#                 58, 881, 0, 594, 691,
#                 1815, 1128, 1836, 1179, 2450,
#                 -8419, -417, 3744, 2854, 618,
#                 -2577, 928, 2348
#                ],
#     'research': map(lambda x: x, [7.547019, 10.255114, 8.691184, 0.809168,
#                  6.471412, 7.949641, 1.158507, 1.362588, 8.103913,
#                  2.192719, 0.989449, 0.125777, 7.936205, 1.459018,
#                  5.722873, 6.2115491, 4.984979, 6.987916, 1.203270,
#                  6.860959, 0.628887, 2.347845
#                 ])
# }

# jp_ranks_df = pd.DataFrame(data).set_index('brokers')

jp_ranks_df = pd.read_sql('''
SELECT b.name AS brokers, a.rank, a.balance_usd AS balance, a.budget_target AS research
FROM broker_ranks a
INNER JOIN brokers b ON a.broker_id=b.id
WHERE a.year=2015 AND a.quarter='Q4'
''', con, index_col='brokers')

# usd_jpy = 112.27  # 2016Q1
# hkd_jpy = 7.754
usd_jpy, hkd_jpy = get_fx_rate('2016-03-31')
print(usd_jpy, hkd_jpy)
quarter1_trades = get_quarter_trades(date.year, 1, ytd_trades)


# In[17]:

# broker_id = pd.read_sql('''
# SELECT id, name AS name
# FROM brokers
# ''', con)


# In[18]:

# jp_ranks_df.reset_index()[['brokers']].to_sql(name='brokers', con=engine, if_exists = 'append', index=False)


# In[19]:

# output_df = jp_ranks_df.merge(broker_id, how='inner', left_index=True, right_on='name')


# In[20]:

# output_df.to_sql(name='sample_table2', con=engine, if_exists = 'append', index=False)


# In[21]:

# Asia commission budget is 2mm  USD per year
# Japan has gone through a few iterations but it's basically approximately 7.5 mm USD per year
jp_annual_commission_budget = 7500000  # usd
asia_annual_commission_budget = 2000000  # usd
jp_quarter_commission_budget = jp_annual_commission_budget / 4.0
asia_quarter_commission_budget = asia_annual_commission_budget / 4.0

jp_quarter_commission_budget = 10031527/4.0  # to make it same with old system 2016Q1


# In[23]:

calculate_commission(quarter1_trades, jp_ranks_df, jp_quarter_commission_budget, 
                     usd_jpy, ubs_include_list=['Tokai', 'Japan Equity Research'], clsa_asia_target=0)


# In[24]:

# broker rank for 2016Q2
# data = {
#     'brokers': ['BAML', 'Mizuho Securities', 'Nomura', 'Japan Equity Research',
#                 'Citi', 'Mitsubishi UFJ', 'Tokai', 'Ichiyoshi', 'SMBC Nikko',
#                 'BNP', 'Deutsche', 'CLSA', 'Daiwa', 'Jefferies',
#                 'CS', 'UBS', 'JP Morgan', 'Goldman Sachs', 'Okasan',
#                 'MS', 'Macquarie'
#                 ],
#     'rank': [6, 2, 1, 17,
#              4, 10, 15, 14, 5,
#              13, 18, 21, 8, 16,
#              9, 12, 7, 3, 19,
#              11, 20],
#     'research': map(lambda x: x, [6.762165, 10.039098, 12.522392, 0.965961,
#                                   8.526189, 5.841530, 1.182470, 2.287421, 7.562959,
#                                   2.414071, 0.878525, 0.249817, 6.466847, 1.094201,
#                                   5.895336, 4.8135476, 6.608858, 8.940196, 0.846049,
#                                   5.319596, 0.741125
#                                   ])
# }

# jp_ranks_df = pd.DataFrame(data).set_index('brokers')



jp_ranks_df = pd.read_sql('''
SELECT b.name AS brokers, a.rank, a.balance_usd AS balance, a.budget_target AS research
FROM broker_ranks a
INNER JOIN brokers b ON a.broker_id=b.id
WHERE a.year=2016 AND a.quarter='Q1'
''', con, index_col='brokers')

# usd_jpy = 102.66  # 2016Q2
# hkd_jpy = 7.759
usd_jpy, hkd_jpy = get_fx_rate(date)
print(usd_jpy, hkd_jpy)
quarter_trades = get_quarter_trades(date.year, quarter, ytd_trades)


# In[25]:

# jp_ranks_df


# In[26]:

# table_q1 = calculate_columns(quarter1_trades, jp_ranks_df, jp_quarter_commission_budget, 
#                              usd_jpy, ubs_include_list = ['Tokai', 'Japan Equity Research'])
# table_q1[['brokers', 'balance_usd']].reset_index().drop('rank', axis=1).set_index('brokers')
# table_q1[['brokers', 'balance_usd']]#.reset_index().drop('rank', axis=1)
# t = table_q1[['brokers', 'balance_usd']].merge(broker_id, left_on='brokers', right_on='name')
# t


# In[27]:

# jp_ranks_df['balance'] - test_df['balance']
# (jp_ranks_df.merge(table_q1[['brokers', 'balance_usd']], left_index=True, right_on='brokers')
#    .assign(dff= lambda df: df['balance'] - df['balance_usd'])
# )


# In[28]:

# jp_ranks_df


# In[29]:

# out_df = (jp_ranks_df.merge(t, how='inner', left_index=True, right_on='brokers')
#           .rename(columns={'research': 'budget_target', 'id': 'broker_id'})
#           .assign(year=2016)
#           .assign(quarter='Q1')
#            .drop(['name', 'brokers'], axis=1)
# )
# out_df.to_sql(name='broker_ranks', con=engine, if_exists = 'append', index=False)
# #out_df


# In[30]:

table = calculate_commission(quarter_trades, jp_ranks_df, jp_quarter_commission_budget, 
                             usd_jpy, ubs_include_list = ['Tokai', 'Japan Equity Research'], clsa_asia_target = 1.950146)
qtd_clearing = "{:,.0f}".format(quarter_trades['Clearing'].sum())
ytd_clearing = "{:,.0f}".format(ytd_trades['Clearing'].sum())

months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
mon1, mon2, mon3 = [months[(quarter - 1) * 3 + i] for i in range(3)]  # get month names for quarter
m1, m2, m3 = [(quarter - 1) * 3 + i for i in range(1, 4)]  # get the month numbers for quarter

soft1, soft_ms1 = calculate_soft(quarter_trades, date.year, m1)
soft2, soft_ms2 = calculate_soft(quarter_trades, date.year, m2)
soft3, soft_ms3 = calculate_soft(quarter_trades, date.year, m3)

soft_qtd = '{:,.0f}'.format(quarter_trades[(quarter_trades['brokerName'] == 'Soft') &
                          (quarter_trades['currencyCode'] == 'JPY')]['JPResearch'].sum())
soft_msqtd = '{:,.0f}'.format(quarter_trades[(quarter_trades['brokerName'] == 'Soft') &
                          (quarter_trades['currencyCode'] == 'JPY')]['JPExec'].sum())


# In[31]:

table


# In[32]:

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


# In[33]:

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


# In[34]:

jp_quarter_commission_budget =10031527/4.0
jp_quarter_commission_budget


# In[35]:

q1 = get_quarter_trades(2016, 1, ytd_trades)


# For monthly balance need to divided res_target by 3 to find balance, but for quarter calculation, do not need to divide by 3.

# In[36]:

# # debug duplicated trades for 2016Q1
# ubsw = q1[q1['brokerCode'] == 'UBSW']
# jan14 = ubsw.loc['2016-01-14']
# jan14[jan14['code'] == '6305']


# In[37]:

## debug duplicated trades for 2016Q2
smbc = quarter_trades[quarter_trades['brokerCode'] == 'NIKK']
print(smbc['JPResearch'].sum())
smbc.loc['2016-05-17':'2016-05-17']['JPResearch'].sum() #[['code', 'fundCode', 'orderType', 'side', 'swap', 'JPResearch']]
smbc.loc['2016-05-17':'2016-05-17']#[['code', 'fundCode', 'orderType', 'side', 'swap', 'JPResearch']]


# In[38]:

# q1[q1['brokerCode']=='MSUS']
print("QTD Clearing : {:,.0f}".format(q1['Clearing'].sum()))


# In[39]:

print("YTD Clearing : {:,.0f}".format(ytd_trades['Clearing'].sum()))


# In[40]:

print("Soft YTD: {:,.0f}".format(q1[(q1['brokerName'] == 'Soft') & (q1['currencyCode'] == 'JPY')]['JPResearch'].sum()))


# In[41]:

jan = q1['2016-01']
print("Jan Soft= {:,.0f}".format(jan[(jan['brokerName'] == 'Soft') ]['JPResearch'].sum()))


# In[42]:

jan_ms = jan[(jan['brokerName'] == 'Soft') ]['JPExec'].sum()
jan_ms
print('Soft MS: {:,.0f}'.format(jan_ms))


# In[43]:

(q1[q1['currencyCode'] != "JPY"]
   .groupby(['brokerName'])
   .sum()[['asiaResearch', 'asiaExecution', 'HCSwaps']]
   .assign(asiaYTD = lambda x: x.asiaResearch + x.asiaExecution)
   # ['commission', 'commInUSD', 'JPResearch', 'JPExec', 'Clearing']
)


# In[44]:

(q1[q1['currencyCode'] != "JPY"]
 .groupby(['brokerName', 'currencyCode'])
 .sum()[['asiaResearch']]
 .unstack()
 .fillna(0)
 )


# In[45]:

# q2jpm = ytd_trades #.loc['2017-04-03':]  #,ytd_trades['brokerCode'] == 'JPMF']
# q2jpm[(q2jpm['brokerCode'] == 'RHBO') & (q2jpm['currencyCode'] != 'JPY')]


# In[46]:

# from IPython.display import display, HTML
#HTML(q1[(q1['currencyCode'] == 'HKD') & (q1['brokerCode'] == 'MERT')].to_html())
#HTML(q1[(q1['currencyCode'] != 'JPY') & (q1['brokerName'] == 'Nomura')].to_html())
# q1[(q1['currencyCode'] != 'JPY') & (q1['brokerName'] == 'UBS')].count()


# In[47]:

con.close()

