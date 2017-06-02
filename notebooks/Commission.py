
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


# In[2]:

with open('config.json') as f:
    conf = json.load(f)


# In[3]:

con = pymysql.connect(host=conf['host'], user=conf['user'], passwd=conf['password'], db=conf['database'])


# In[4]:

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


# In[5]:

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


# In[6]:

def format_2f(df):
    t = df.copy()
    columns = ['JPResearch', 'JPExec', 'JPDis', 'res_target', 'balance_usd']
    t[columns] = t[columns].applymap(lambda x: '$ {:12,.0f}'.format(x) if x > 0 else '')
    t['balance_jpy'] = t['balance_jpy'].apply(lambda x: '¥ {:12,.0f}'.format(x) if x > 0 else '')
    t['rank'] = t['rank'].apply(lambda x: '{:.0f}'.format(x) if not np.isnan(x) else '')
    t['research'] = t['research'].apply(lambda x: '{:5.2f}%'.format(x) if x > 0 else '')
    t['accrued'] = t['accrued'].apply(lambda x: '{:5.0f}%'.format(x) if not np.isnan(x) else '')
    t['exec_target'] = t['exec_target'].apply(lambda x: '{:5.0f}%'.format(x) if x > 0 else '')
    return t


# In[7]:

def calculate_commission(quarter_trades, jp_ranks_df, jp_quarter_commission_budget, usd_jpy):
    table = (quarter_trades
             .groupby(['brokerName', 'currencyCode'])
             .sum()
             .loc[(slice(None), ['JPY', 'USD']), ['JPResearch', 'JPExec', 'JPDis']]
             .groupby(level=0).sum()
             .reset_index()
             .set_index('brokerName')
             .merge(jp_ranks_df, how='right', left_index=True,
                    right_index=True)  # some names removed like Barclays and Softs
             .fillna(0)
             .sort_values(by='rank', axis=0)
             .assign(res_target=lambda df: df['research'] * jp_quarter_commission_budget / 100)
             .assign(balance_usd=lambda df: df['res_target'] - df['JPResearch'])
             .assign(balance_jpy=lambda df: df['balance_usd'] * usd_jpy)
             .assign(accrued=lambda df: (df['JPExec'] + df['JPDis']) * 100 / (df['JPExec'].sum() + df['JPDis'].sum()))
             .reset_index()
             .set_index('rank')
             )

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


# In[8]:

date = '2016-06-30'

if date is None:
    date = datetime.date.today()
else:
    try:
        date = datetime.strptime(date, '%Y-%m-%d')
    except ValueError:
        date = datetime.date.today()

quarter = (date.month-1) // 3 + 1

ytd_trades = get_trades(date.year)


# In[9]:

date.year, quarter


# In[10]:

ytd_trades.head()


# In[11]:

# Asia commission budget is 2mm  USD per year
# Japan has gone through a few iterations but it's basically approximately 7.5 mm USD per year
jp_annual_commission_budget = 7500000  # usd
asia_annual_commission_budget = 2000000  # usd
jp_quarter_commission_budget = jp_annual_commission_budget / 4.0
asia_quarter_commission_budget = asia_annual_commission_budget / 4.0

jp_quarter_commission_budget = 10031527/4.0  # to make it same with old system 2016Q1


# In[12]:

# broker rank for 2016Q2
data = {
    'brokers': ['BAML', 'Mizuho Securities', 'Nomura', 'Japan Equity Research',
                'Citi', 'Mitsubishi UFJ', 'Tokai', 'Ichiyoshi', 'SMBC Nikko',
                'BNP', 'Deutsche', 'CLSA', 'Daiwa', 'Jefferies',
                'CS', 'UBS', 'JP Morgan', 'Goldman Sachs', 'Okasan',
                'MS', 'Macquarie'
                ],
    'rank': [6, 2, 1, 17,
             4, 10, 15, 14, 5,
             13, 18, 21, 8, 16,
             9, 12, 7, 3, 19,
             11, 20],
    'research': map(lambda x: x, [6.76, 10.04, 12.52, 0.97,
                                  8.53, 5.84, 1.18, 2.29, 7.56,
                                  2.41, 0.88, 0.25, 6.47, 1.09,
                                  5.90, 8.91, 6.61, 8.94, 0.85,
                                  5.32, 0.25
                                  ])
}

jp_ranks_df = pd.DataFrame(data).set_index('brokers')
usd_jpy = 112.27  # 2016Q1
hkd_jpy = 7.754
usd_jpy = 102.66  # 2016Q2
hkd_jpy = 7.759
quarter_trades = get_quarter_trades(date.year, quarter, ytd_trades)


# In[13]:

table = calculate_commission(quarter_trades, jp_ranks_df, jp_quarter_commission_budget, usd_jpy)
qtd_clearing = "{:,.0f}".format(quarter_trades['Clearing'].sum())
ytd_clearing = "{:,.0f}".format(ytd_trades['Clearing'].sum())

months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
mon1, mon2, mon3 = {
    1: months[0:3],
    2: months[3:6],
    3: months[6:9],
    4: months[9:]
}[quarter]

if quarter == 1:
    m1, m2, m3 = [1, 2, 3]
elif quarter == 2:
    m1, m2, m3 = [4, 5, 6]

mon1_trades = quarter_trades['{}-{:02d}'.format(date.year, m1)]
soft1 = "{:,.0f}".format(mon1_trades[(mon1_trades['brokerName'] == 'Soft')]['JPResearch'].sum())
soft_ms1 = "{:,.0f}".format(mon1_trades[mon1_trades['brokerName'] == 'Soft']['JPExec'].sum())

mon2_trades = quarter_trades['{}-{:02d}'.format(date.year, m2)]
soft2 = "{:,.0f}".format(mon2_trades[(mon2_trades['brokerName'] == 'Soft')]['JPResearch'].sum())
soft_ms2 = "{:,.0f}".format(mon2_trades[mon2_trades['brokerName'] == 'Soft']['JPExec'].sum())

mon3_trades = quarter_trades['{}-{:02d}'.format(date.year, m3)]
soft3 = "{:,.0f}".format(mon3_trades[(mon3_trades['brokerName'] == 'Soft')]['JPResearch'].sum())
soft_ms3 = "{:,.0f}".format(mon3_trades[mon3_trades['brokerName'] == 'Soft']['JPExec'].sum())

soft_qtd = '{:,.0f}'.format(quarter_trades[(quarter_trades['brokerName'] == 'Soft') &
                          (quarter_trades['currencyCode'] == 'JPY')]['JPResearch'].sum())
soft_msqtd = '{:,.0f}'.format(quarter_trades[(quarter_trades['brokerName'] == 'Soft') &
                          (quarter_trades['currencyCode'] == 'JPY')]['JPExec'].sum())


# In[14]:

table


# In[15]:

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


# In[16]:

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


# In[17]:

# broker rank for each quarter - 2016Q1
data = {
    'brokers': ['BAML', 'Mizuho Securities',  'Nomura', 'Japan Equity Research', 
                'Citi', 'Mitsubishi UFJ', 'Tokai', 'Ichiyoshi', 'SMBC Nikko', 
                'BNP', 'Deutsche', 'CLSA', 'Daiwa', 'Jefferies', 
                'CS', 'UBS', 'JP Morgan', 'Goldman Sachs', 'Okasan', 
                'MS', 'Macquarie', 'Barclays'
               ],
    'rank': [6, 1, 2, 20, 9, 4, 18, 16, 3, 14, 19, 22, 5, 15, 11, 10, 12, 7, 17, 8, 21, 13],
    'balance': [2240, -664, -808, 0,
                58, 881, 0, 594, 691,
                1815, 1128, 1836, 1719, 2450,
                -8419, -417, 3744, 2854, 618,
                -2577, 928, 2348
               ],
    'research': map(lambda x: x, [7.55, 10.255114, 8.69, 0.81,
                 6.47, 7.95, 1.16, 1.36, 8.1,
                 2.19, 0.99, 0.13, 7.94, 1.46,
                 5.72, 8.18, 4.98, 6.99, 1.2,
                 6.86, 0.63, 2.35
                ])
}

jp_ranks_df = pd.DataFrame(data).set_index('brokers')
jp_ranks_df


# In[18]:

jp_quarter_commission_budget =10031527/4.0
jp_quarter_commission_budget


# In[19]:

q1 = get_quarter_trades(2016, 1, ytd_trades)


# For monthly balance need to divided res_target by 3 to find balance, but for quarter calculation, do not need to divide by 3.

# In[20]:

# # debug duplicated trades for 2016Q1
# ubsw = q1[q1['brokerCode'] == 'UBSW']
# jan14 = ubsw.loc['2016-01-14']
# jan14[jan14['code'] == '6305']


# In[21]:

## debug duplicated trades for 2016Q2
smbc = quarter_trades[quarter_trades['brokerCode'] == 'NIKK']
print(smbc['JPResearch'].sum())
smbc.loc['2016-05-17':'2016-05-17']['JPResearch'].sum() #[['code', 'fundCode', 'orderType', 'side', 'swap', 'JPResearch']]
smbc.loc['2016-05-17':'2016-05-17']#[['code', 'fundCode', 'orderType', 'side', 'swap', 'JPResearch']]


# In[22]:

# q1[q1['brokerCode']=='MSUS']
print("QTD Clearing : {:,.0f}".format(q1['Clearing'].sum()))


# In[23]:

print("YTD Clearing : {:,.0f}".format(ytd_trades['Clearing'].sum()))


# In[24]:

print("Soft YTD: {:,.0f}".format(q1[(q1['brokerName'] == 'Soft') & (q1['currencyCode'] == 'JPY')]['JPResearch'].sum()))


# In[25]:

jan = q1['2016-01']
print("Jan Soft= {:,.0f}".format(jan[(jan['brokerName'] == 'Soft') ]['JPResearch'].sum()))


# In[26]:

jan_ms = jan[(jan['brokerName'] == 'Soft') ]['JPExec'].sum()
jan_ms
print('Soft MS: {:,.0f}'.format(jan_ms))


# In[27]:

(q1[q1['currencyCode'] != "JPY"]
   .groupby(['brokerName'])
   .sum()[['asiaResearch', 'asiaExecution', 'HCSwaps']]
   .assign(asiaYTD = lambda x: x.asiaResearch + x.asiaExecution)
   # ['commission', 'commInUSD', 'JPResearch', 'JPExec', 'Clearing']
)


# In[28]:

(q1[q1['currencyCode'] != "JPY"]
 .groupby(['brokerName', 'currencyCode'])
 .sum()[['asiaResearch']]
 .unstack()
 .fillna(0)
 )


# In[29]:

# q2jpm = ytd_trades #.loc['2017-04-03':]  #,ytd_trades['brokerCode'] == 'JPMF']
# q2jpm[(q2jpm['brokerCode'] == 'RHBO') & (q2jpm['currencyCode'] != 'JPY')]


# In[30]:

# from IPython.display import display, HTML
#HTML(q1[(q1['currencyCode'] == 'HKD') & (q1['brokerCode'] == 'MERT')].to_html())
#HTML(q1[(q1['currencyCode'] != 'JPY') & (q1['brokerName'] == 'Nomura')].to_html())
# q1[(q1['currencyCode'] != 'JPY') & (q1['brokerName'] == 'UBS')].count()

