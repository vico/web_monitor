
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
import japandas as jpd


# In[2]:

ubs_include_list = ['Tokai', 'Japan Equity Research']


# In[3]:

date = '2016-06-30'
date = '2016-09-30'
date = '2016-12-30'
# date = '2016-03-31'
# date = '2016-04-01'

if date is None:
    date = datetime.date.today()
else:
    try:
        date = datetime.strptime(date, '%Y-%m-%d')
    except ValueError:
        date = datetime.date.today()

quarter = (date.month-1) // 3 + 1

prev_quarter = 4 if quarter == 1 else quarter - 1
prev_year = date.year - 1 if quarter == 1 else date.year


# In[4]:

with open('config.json') as f:
    conf = json.load(f)


# In[5]:

con = pymysql.connect(host=conf['host'], user=conf['user'], passwd=conf['password'], db=conf['database'])
engine = create_engine('mysql+pymysql://root:root@localhost:3306/hkg02p', echo=False)


# In[6]:

def calculate_quarter(from_month):
    return (from_month - 1) // 3 + 1


# In[7]:

def get_ytd_trades(date):
    """
    return all trades from start of year upto date
    :param date: the year to get trades
    :return: DataFrame of all trades of year
    """

    start_date = '{}-01-01'.format(date.year)
    end_date = datetime.strftime(date, '%Y-%m-%d')

    ytd_trades = sql.read_sql("""
        SELECT a.code, a.fundCode, a.orderType, a.side, a.swap, a.tradeDate, a.PB, 
              g.commrate as gcomrate, d.brokerCommissionRate,
              # a.commission,
              (@commInUSD := a.commission*f.rate) AS commInUSD,
              (@commrate := IF (g.commrate IS NOT NULL, ROUND(g.commrate,4), ROUND(d.brokerCommissionRate,4))) AS CommRate,
              (@jpresearch := IF(b.currencyCode IN ("JPY", "USD") AND c.instrumentType IN ("EQ", "CB") AND  (@commrate<> 0.0004), 
                  @commInUSD*11/15,0)*1.0) AS JPResearch,
                  IF(b.currencyCode IN ("JPY", "USD") AND c.instrumentType IN ("EQ", "CB") AND (@commrate=0.0004), 
                      @commInUSD, 
                      0)*1.0 AS JPDis,
              (@clearing:=
              IF (c.instrumentType IN ("FU", "OP"), 
                  IF(SUBSTRING(a.code, 1, 2) IN ("TP", "NK"), 500, 
                    IF(SUBSTRING(a.code, 1, 2)="JP",50, 
                    IF(SUBSTRING(a.code, 1, 2) IN ("HC", "HI"), 30, 0) 
                )
               ) * a.quantity *f.rate, 0  )) AS Clearing,
              IF(b.currencyCode IN ("JPY", "USD")  AND c.instrumentType IN ("EQ", "CB") AND (@commrate=0.0004 OR @commrate=0),
                  0,
                  IF(b.currencyCode IN ("JPY", "USD")  AND c.instrumentType IN ("EQ", "CB"),
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
              (@asiadeal := IF (b.currencyCode  NOT IN ("JPY", "USD")  AND c.instrumentType IN ("EQ", "CB") AND d.brokerCommissionRate > 0.01,
                                a.gross * f.rate * (d.brokerCommissionRate - @tax ), 0)) AS asiaDeal,
              (@asiaResearch := IF(b.currencyCode  NOT IN ("JPY", "USD")  AND c.instrumentType IN ("EQ", "CB") AND @asiadeal=0, 
                IF(a.commissionRate-@tax-0.0005 >= 0, a.commissionRate-@tax-0.0005, 0) *f.rate*a.gross, 0)) AS asiaResearch,
              IF (b.currencyCode NOT  IN ("JPY", "USD")  AND c.instrumentType IN ("EQ", "CB") AND @asiadeal=0,
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
            where a.status="A" and a.srcFlag="D" and a.tradeDate >= '{}' and a.tradeDate <= '{}'
            group by a.code, a.orderType, a.side, a.swap, a.tradeDate, a.settleDate, a.brokerCode
              ) g ON a.code=g.code and a.orderType=g.orderType and a.side=g.side and a.swap=g.swap and a.tradeDate=g.tradeDate
                     and a.settleDate=g.settleDate  and d.brokerCode=g.brokerCode
            WHERE a.tradeDate>='{}' AND a.tradeDate <= '{}' AND a.srcFlag="K"
            ORDER BY a.tradeDate, a.code;
        """.format(start_date, end_date, start_date, end_date), con, parse_dates=['tradeDate'], index_col='tradeDate')
    return ytd_trades


# In[8]:

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


# In[9]:

def format_2f(df):
    t = df.copy()
    columns = ['JPResearch', 'JPExec', 'JPDis', 'res_target', 'balance_usd']
    t[columns] = t[columns].applymap(lambda x: '$ {:12,.0f}'.format(x) if x != 0 else '')
    t['balance_jpy'] = t['balance_jpy'].apply(lambda x: '¥ {:12,.0f}'.format(x) if x != 0 else '')
    t['rank'] = t['rank'].apply(lambda x: '{:.0f}'.format(x) if not np.isnan(x) and x != 0  else '')
    t['research'] = t['research'].apply(lambda x: '{:5.2f}%'.format(x) if x > 0 else '')
    t['accrued'] = t['accrued'].apply(lambda x: '{:5.0f}%'.format(x) if not np.isnan(x) else '')
    t['exec_target'] = t['exec_target'].apply(lambda x: '{:5.0f}%'.format(x) if x > 0 else '')
    return t


# In[10]:

def format_asia_2f(df):
    t = df.copy()
    columns = ['asiaResearch', 'asiaExecution', 'asiaDeal', 'res_target', 'balance_usd']
    t[columns] = t[columns].applymap(lambda x: '$ {:12,.0f}'.format(x) if x != 0 else '')
    t['balance_hkd'] = t['balance_hkd'].apply(lambda x: '$ {:12,.0f}'.format(x) if x != 0 else '')
    t['HCSwaps'] = t['HCSwaps'].apply(lambda x: '$ {:12,.0f}'.format(x) if x != 0 else '')
    t['rank'] = t['rank'].apply(lambda x: '{:.0f}'.format(x) if not np.isnan(x) else '')
    t['research'] = t['research'].apply(lambda x: '{:5.2f}%'.format(x) if x > 0 else '')
    t['accrued'] = t['accrued'].apply(lambda x: '{:5.0f}%'.format(x) if not np.isnan(x) else '')
    t['execution'] = t['execution'].apply(lambda x: '{:5.0f}%'.format(x) if x > 0 else '')
    return t


# In[11]:

def get_csa_brokers(date, region='Japan'):
    quarter = calculate_quarter(date.month)
    prev_quarter = 4 if quarter == 1 else quarter - 1
    date_str = date.strftime('%Y-%m-%d')
    
    return pd.read_sql("""
    SELECT a.name AS master_brokers, d.name AS brokers, 
    #c.balance_usd AS balance,c.budget_target AS research,
    d.region
    FROM brokers a
    INNER JOIN broker_csa b ON a.id=b.master_broker_id
      INNER JOIN broker_ranks c ON b.sub_broker_id=c.broker_id
      INNER JOIN brokers d ON b.sub_broker_id=d.id
    WHERE a.region="{}" AND b.start_date <= "{}"
      AND (b.end_date IS NULL OR b.end_date >= "{}")
      AND c.year={}
      AND c.quarter={}
    """.format(region, date_str, date_str, date.year, prev_quarter), con)
    


# In[12]:

def calculate_soft(quarter_trades, year, month):
    month_key = '{}-{:02d}'.format(year, month)
    if month_key in quarter_trades.index:
        mon_trades = quarter_trades[month_key]
        soft = "{:,.0f}".format(mon_trades[(mon_trades['brokerName'] == 'Soft')]['JPResearch'].sum())
        soft_ms = "{:,.0f}".format(mon_trades[mon_trades['brokerName'] == 'Soft']['JPExec'].sum())
        return soft, soft_ms
    else:
        return 0, 0


# In[13]:

def get_fx_rate(price_date):
    """ return tuple of fx rate for USDJPY and USDHKD
    """
    query = """SELECT a.quote, a.rate
                    FROM t06DailyCrossRate a
                    WHERE a.priceDate="{}" AND a.quote IN ("JPY", "HKD") AND a.base="USD"
    """

    t = pd.read_sql(query.format(price_date.strftime('%Y-%m-%d')), con, index_col="quote")
    return t.loc['JPY', 'rate'], t.loc['HKD', 'rate']


# In[14]:

def get_ranks(year, quarter, region='Japan'):
    return pd.read_sql('''
                SELECT b.name AS brokers, a.rank, a.balance_usd AS balance, 
                        a.budget_target AS research
                FROM broker_ranks a
                INNER JOIN brokers b ON a.broker_id=b.id
                WHERE a.year={} AND a.quarter='Q{}' AND b.region='{}'
        '''.format(year, quarter, region), con, index_col='brokers')


# In[15]:

def get_annual_budget(for_year):
    query = """SELECT a.region, a.amount
                        FROM commission_budget a
                        WHERE a.year={}
    """.format(for_year)

    budget_df = pd.read_sql(query, con, index_col='region')
    return budget_df.loc['Japan', 'amount'], budget_df.loc['NonJapan', 'amount']


# In[16]:

def calculate_columns_asia(quarter_trades, ranks_df, asia_quarter_commission_budget, usd_hkd,
                      request_date, ubs_include_list=[]):

    def zero_out_ubs_include(df, ubs_list):
        """ assume index is brokerName, and balance_usd column is already created
        """
        t = df.copy()
        for broker in ubs_list:
            t.loc[broker, 'balance_usd'] = 0

        return t

    # quarter = calculate_quarter(request_date.month)
    # budget_ratio = (request_date.month - (quarter - 1) * 3) / 3.0

    rank_df = ranks_df.copy()
    balance = rank_df['balance']
    for broker in ubs_include_list:
        rank_df.loc['UBS', 'research'] += rank_df.loc[broker, 'research']
        rank_df.loc[broker, 'research'] = 0

    ranked_df = (quarter_trades[(quarter_trades['currencyCode'] != "JPY") & (quarter_trades['currencyCode'] != "USD")]
                 .groupby(['brokerName'])
                 .sum()[['asiaResearch', 'asiaExecution', 'asiaDeal', 'HCSwaps']]
                 .assign(asiaYTD=lambda x: x.asiaResearch + x.asiaExecution)
                 .merge(rank_df, how='right', left_index=True, right_index=True)
                 .fillna(0)
                 .sort_values(by='rank')
                 )
    bal = balance.reindex(ranked_df.index)
    table = (ranked_df.assign(res_target=lambda df: df['research'] * asia_quarter_commission_budget / 100 +
                                                    (bal if bal is not None else 0))
             .assign(balance_usd=lambda df: df['res_target'] - df['asiaResearch'])
             .pipe(zero_out_ubs_include, ubs_include_list)
             .assign(balance_hkd=lambda df: df['balance_usd'] * usd_hkd)
             .assign(accrued=lambda df: df['asiaExecution'] * 100 / df['asiaExecution'].sum())
             .reset_index()
             .set_index('rank')
             )

    return table


# In[17]:

def calculate_commission_asia_table(table_param, nonjp_paid_on_jp):
    table = table_param.copy()
    exec_target = [15, 15, 15, 15, 15, 5, 5, 5, 5, 5]
    if len(exec_target) < len(table.index):
        exec_target = exec_target + [0] * (len(table.index) - len(exec_target))

    table['execution'] = pd.Series(exec_target, index=table.index)
    
    table = table[~table['brokers'].isin(nonjp_paid_on_jp['brokers'].values.tolist())]

    return (table
            .reset_index()
            .append(table[['research', 'res_target', 'asiaResearch', 'asiaExecution',
                           'balance_usd', 'balance_hkd', 'execution', 'accrued', 'asiaDeal', 'HCSwaps']].sum(), 
                    ignore_index=True)
            # .fillna(0)
            .pipe(format_asia_2f)
            [['rank', 'brokers', 'research', 'res_target', 'asiaResearch', 'balance_usd', 'balance_hkd',
              'execution', 'accrued', 'asiaExecution', 'asiaDeal', 'HCSwaps']]
            )


# In[18]:

get_ipython().magic(u'time ytd_trades = get_ytd_trades(date)')


# In[19]:

calendar = jpd.JapaneseHolidayCalendar()
cday = pd.offsets.CDay(calendar=calendar)


# In[20]:

indexer = pd.date_range('{}-{}-{}'.format(date.year, 1, 1), '{}-{}-{}'.format(date.year, 12, 31), freq=cday)


# In[21]:

# Asia commission budget is 2mm  USD per year
# Japan has gone through a few iterations but it's basically approximately 7.5 mm USD per year
# jp_annual_commission_budget = 7500000  # usd
# asia_annual_commission_budget = 2000000  # usd

jp_annual_commission_budget, asia_annual_commission_budget = get_annual_budget(date.year)

jp_quarter_commission_budget = jp_annual_commission_budget / 4.0
asia_quarter_commission_budget = asia_annual_commission_budget / 4.0

# jp_quarter_commission_budget = 10031527/4.0  # to make it same with old system 2016Q1



# In[22]:

csa_brokers = get_csa_brokers(date)
# nonjapan_indexer = csa_brokers[csa_brokers['region'] == 'NonJapan']['research'].index
# csa_brokers.loc[nonjapan_indexer, 'research'] = csa_brokers.loc[nonjapan_indexer, 'research'] * asia_quarter_commission_budget/jp_quarter_commission_budget
csa_brokers


# In[23]:

csa_brokers['master_brokers'].unique().tolist()


# In[24]:

nonjp_csa_brokers = get_csa_brokers(date, 'NonJapan')
nonjp_csa_brokers


# In[25]:

csa_brokers[csa_brokers['region'] == 'NonJapan']['brokers'].tolist()


# In[26]:

nonjp_csa_brokers['brokers'].tolist()


# In[27]:

jp_ranks_df = get_ranks(prev_year,prev_quarter)

# usd_jpy = 102.66  # 2016Q2
# hkd_jpy = 7.759
usd_jpy, usd_hkd = get_fx_rate(date)
print(usd_jpy, usd_hkd)
quarter_trades = get_quarter_trades(date.year, quarter, ytd_trades)


# In[28]:

nonjp_ranks_df = get_ranks(prev_year,prev_quarter, 'NonJapan')


# In[29]:

def adjust_research(df, jp_quarter_commission_budget):
    t = df.copy()
    t['research'] = df['balance_usd'] *100/jp_quarter_commission_budget
    return t

pair_list = csa_brokers[csa_brokers['region'] == 'NonJapan'][['master_brokers', 'brokers']].values.tolist()
nonjp_paid_on_jp_broker_list = [ b[1] for b in pair_list]
asia_columns = calculate_columns_asia(quarter_trades, nonjp_ranks_df, asia_quarter_commission_budget, usd_hkd, 
                                      date, nonjp_csa_brokers['brokers'].tolist())


# need to get asia columns calculated to get balance to append to JP table.
nonjp_paid_on_jp = (asia_columns
 .merge(csa_brokers, how='inner', left_on='brokers', right_on='brokers')
 .pipe(adjust_research, jp_quarter_commission_budget)[['master_brokers', 'brokers', 'balance_usd', 'research']]
)

# nonjp_paid_on_jp[['master_brokers', 'brokers']].values.tolist()
# nonjp_paid_on_jp.groupby('master_brokers').sum().index.tolist()
# nonjp_paid_on_jp.drop('master_brokers', axis=1).set_index('brokers').rename(index={'CLSA': 'CLSA Asia'})
# asia_columns.drop(asia_columns['brokers'] == pd.Series(nonjp_paid_on_jp['brokers'].values.tolist()))


# In[30]:

# calculate_columns(quarter_trades, jp_ranks_df, jp_quarter_commission_budget, usd_jpy, ubs_include_list, clsa_asia_target = 1.950146)
nonjp_paid_on_jp['brokers'].tolist()


# In[31]:

#csa_brokers[csa_brokers['region'] == 'Japan']['brokers'].tolist()
# csa_brokers['brokers'].tolist()
# csa_brokers[csa_brokers['region'] == 'Japan'][['master_brokers', 'brokers']].values.tolist()


# In[32]:

def zero_out_ubs_include(df, ubs_list):
        """ assume index is brokerName, and balance_usd column is already created
        """
        t = df.copy()
        for broker in ubs_list:
            t.loc[broker, 'balance_usd'] = 0

        return t
    
rank_df = jp_ranks_df.copy()
balance = rank_df['balance']

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
         .sort_values(by='research', ascending=False)
            )

for (master, broker) in csa_brokers[csa_brokers['region'] == 'Japan'][['master_brokers', 'brokers']].values.tolist():
    ranked_df.loc[master, 'research'] += rank_df.loc[broker, 'research']
    ranked_df.loc[broker, 'research'] = 0

temp = nonjp_paid_on_jp.groupby('master_brokers').sum()
for master in temp.index.tolist():
    ranked_df.loc[master, 'research'] += temp.loc[master]['research']

bal = balance.reindex(ranked_df.index)
table = (ranked_df
         .assign(res_target=lambda df: df['research'] * jp_quarter_commission_budget / 100 + (bal if bal is not None else 0) )
         .assign(balance_usd=lambda df: df['res_target'] - df['JPResearch'] )
         .assign(balance_jpy=lambda df: df['balance_usd'] * usd_jpy)
         .assign(accrued=lambda df: (df['JPExec'] + df['JPDis']) * 100 / (df['JPExec'].sum() + df['JPDis'].sum()))
         .reset_index()
         .set_index('rank')
         )
# table



# In[33]:

exec_target = [11, 11, 10, 10, 10, 7, 7, 7, 7, 7, 3, 3, 3, 2, 1]
if len(exec_target) < len(table.index):
    exec_target = exec_target + [0] * (len(table.index) - len(exec_target))
table['exec_target'] = pd.Series(exec_target, index=table.index)

final_table = (table
        .reset_index()
         .set_index('brokers')
         .append(nonjp_paid_on_jp
                 .drop('master_brokers', axis=1)
                 .set_index('brokers')
                 .rename(index={'CLSA': 'CLSA Asia'}))
             .fillna(0)
         .reset_index()
        .append(table[['research', 'res_target', 'JPResearch', 'JPExec',
                       'JPDis', 'balance_usd', 'balance_jpy', 'exec_target']].sum(), ignore_index=True)
        .pipe(format_2f)
        [['rank', 'brokers', 'research', 'res_target', 'JPResearch', 'JPExec',
          'JPDis', 'balance_usd', 'balance_jpy', 'exec_target', 'accrued']]
        )


# ### new requirement
# need to restart balance_usd for specific broker from some month. for ex: MS from 2017 Jun

# In[34]:

# calculate_commission(quarter1_trades, jp_ranks_df, jp_quarter_commission_budget, 
#                      usd_jpy, ubs_include_list=['Tokai', 'Japan Equity Research'], clsa_asia_target=0)


# In[35]:

def check(df):
    t = df.copy()
    columns = [ 'balance_usd']
    t[columns] = t[columns].applymap(lambda x: '{:12.7f}'.format(x) if x != 0 else '')
    
    return t
table.pipe(check)


# In[36]:

calculate_commission_asia_table(asia_columns, nonjp_paid_on_jp)


# In[37]:

# out_df = (jp_ranks_df.merge(t, how='inner', left_index=True, right_on='brokers')
#           .rename(columns={'research': 'budget_target', 'id': 'broker_id'})
#           .assign(year=2016)
#           .assign(quarter='Q1')
#            .drop(['name', 'brokers'], axis=1)
# )
# out_df.to_sql(name='broker_ranks', con=engine, if_exists = 'append', index=False)
## #out_df


# In[38]:

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


# In[39]:

final_table


# In[40]:

# q1 = get_quarter_trades(2016, 1, ytd_trades)
q3 = get_quarter_trades(2016, 3, ytd_trades)


# For monthly balance need to divided res_target by 3 to find balance, but for quarter calculation, do not need to divide by 3.

# In[41]:

# # debug duplicated trades for 2016Q1
# ubsw = q1[q1['brokerCode'] == 'UBSW']
# jan14 = ubsw.loc['2016-01-14']
# jan14[jan14['code'] == '6305']
# baml = q3[q3['brokerCode'] == 'MERT']
# sept = baml.loc['2016-09-30':'2016-09-30']
# sept[['code', 'fundCode', 'orderType', 'side', 'swap', 'JPDis']]


# In[42]:

# citi = q3[q3['brokerCode'] == 'SALH']
# citi.loc['2016-09-15':'2016-09-15'][['code', 'fundCode', 'orderType', 'side', 'swap', 'JPResearch', 'JPDis', 'JPExec']]


# In[43]:

# baml = q3[q3['brokerCode'] == 'MERT']
# sept = baml.loc['2016-09-06':'2016-09-07']
# sept[['code', 'fundCode', 'orderType', 'side', 'swap', 'asiaResearch']]


# In[44]:

# cs = q3[q3['brokerCode'] == 'CSFH']
# cs.loc['2016-09-27':'2016-09-30'][['code', 'fundCode', 'orderType', 'side', 'swap', 'asiaResearch']]

mzh = q3[q3['brokerCode'] == 'IBJA']
mzh.loc['2016-09-01':'2016-09-30'][['code', 'fundCode', 'orderType', 'side', 'swap', 'asiaResearch']].sum()


# In[45]:

## debug duplicated trades for 2016Q2
# smbc = quarter_trades[quarter_trades['brokerCode'] == 'NIKK']
# print(smbc['JPResearch'].sum())
# smbc.loc['2016-05-17':'2016-05-17']['JPResearch'].sum() #[['code', 'fundCode', 'orderType', 'side', 'swap', 'JPResearch']]
# smbc.loc['2016-05-17':'2016-05-17']#[['code', 'fundCode', 'orderType', 'side', 'swap', 'JPResearch']]


# In[46]:

# calculate_columns_asia(quarter_trades, nonjp_ranks_df, asia_quarter_commission_budget, usd_hkd, date, ['Kim Eng', 'Samsung', 'CIMB'])#.pipe(check)
calculate_columns_asia(quarter_trades, nonjp_ranks_df, asia_quarter_commission_budget, usd_hkd, 
                                      date, nonjp_csa_brokers['brokers'].tolist()).pipe(check)


# In[47]:

def format_all_2f(df):
    t = df.copy()
    t = t.applymap(lambda x: '$ {:12,.0f}'.format(x) if x != 0 and type(x) != str else (x if type(x) == str else ''))
    return t


# In[48]:

def get_csa_payment(year, quarter):
    csa_cum_payment = pd.read_sql("""
        SELECT a.name AS brokerName, e.name AS master_broker,
            IF(a.region="Japan", IF(b.currency_code="JPY", SUM(b.amount) / c.finalFXRate, SUM(b.amount)), 0) AS JPResearch,
            IF(a.region="NonJapan", IF(b.currency_code="JPY", SUM(b.amount) / c.finalFXRate, SUM(b.amount)), 0) AS asiaResearch
        FROM brokers a
        INNER JOIN csa_payment b ON a.id=b.broker_id
          LEFT JOIN broker_csa d ON a.id=d.sub_broker_id
          LEFT JOIN brokers e ON d.master_broker_id=e.id
          LEFT JOIN (
            SELECT DISTINCT (TRUNCATE((MONTH(a.processDate) - 1) / 3, 0) + 1) AS quarter, a.finalFXRate
        FROM t05PortfolioReportEvent a
        WHERE YEAR(a.processDate)={} AND a.dataType="SUB_RED"
          AND MONTH(a.processDate) IN (3,6,9,12)
        ORDER BY a.processDate DESC
            ) c ON b.quarter = CONCAT("Q", c.quarter)
        WHERE b.year={} AND b.quarter <= "Q{}"
        GROUP BY a.name;
        """.format(year, year, quarter), con, index_col="brokerName")
    return csa_cum_payment


# In[49]:

if indexer[-1].to_pydatetime() == date:
    csa_cum_payment = get_csa_payment(date.year, quarter)
else:
    csa_cum_payment = get_csa_payment(date.year, prev_quarter)


# In[50]:

date.year, prev_quarter


# In[51]:

# the amount of money paid as CSA to brokers. 
# Need to add this to YTD amount, and minus it from broker which pay other brokers for reaseach service
csa_cum_payment


# In[52]:

csa_cum_payment.loc['CLSA', 'master_broker']


# In[53]:

qtd_sum = (quarter_trades
 .groupby(['brokerName'])
 .sum()[['JPResearch', 'JPDis', 'JPExec', 'asiaDeal', 'asiaResearch', 'asiaExecution', 'HCSwaps']]
 .assign(japan_qtd=lambda df: df['JPResearch'] + df['JPExec'] + df['JPDis'])
 .assign(asia_qtd = lambda df: df['asiaDeal'] + df['asiaResearch'] + df['asiaExecution'])
           [['japan_qtd', 'asia_qtd']]
)
qtd_sum


# In[54]:

def apply_csa_sum(df, csa_cum_payment):
    # for each broker belong to a master broker which pay CSA 
    for broker in csa_cum_payment.index:
        if broker in df.index:
            if csa_cum_payment.loc[broker, 'asiaResearch'] > 0:
                df.loc[broker, 'asiaResearch'] += csa_cum_payment.loc[broker, 'asiaResearch']
            if csa_cum_payment.loc[broker, 'JPResearch'] > 0:
                df.loc[broker, 'JPResearch'] += csa_cum_payment.loc[broker, 'JPResearch']
        else:
            df = df.append(csa_cum_payment.loc[broker])
    return df


# In[55]:

def remove_csa_sum(df, csa_cum_payment, nonjp_paid_on_jp):
    # for each broker belong to a master broker which pay CSA 
    for broker in csa_cum_payment.index:
        master_broker = csa_cum_payment.loc[broker, 'master_broker']
        if csa_cum_payment.loc[broker, 'asiaResearch'] > 0 and broker not in nonjp_paid_on_jp['brokers'].tolist():
            df.loc[master_broker, 'asia_ytd'] -= csa_cum_payment.loc[broker, 'asiaResearch']
        elif csa_cum_payment.loc[broker, 'asiaResearch'] > 0 and broker in nonjp_paid_on_jp['brokers'].tolist():
            df.loc[master_broker, 'japan_ytd'] -= csa_cum_payment.loc[broker, 'asiaResearch']
        if csa_cum_payment.loc[broker, 'JPResearch'] > 0:
            df.loc[master_broker, 'japan_ytd'] -= csa_cum_payment.loc[broker, 'JPResearch']
    return df


# In[56]:

(ytd_trades
 .groupby('brokerName')
 .sum()[['JPResearch', 'JPDis', 'JPExec', 'asiaDeal', 'asiaResearch', 'asiaExecution', 'HCSwaps']]
 .assign(japan_exe_ytd = lambda df: df['JPExec'] + df['JPDis'])
 .pipe(apply_csa_sum, (csa_cum_payment))
 .fillna(0)
 .assign(japan_ytd = lambda df: df['JPResearch'] + df['JPExec'] + df['JPDis'])
 .assign(asia_ytd = lambda df: df['asiaResearch'] + df['asiaExecution'] + df['asiaDeal'])  # + csa difference
 .pipe(remove_csa_sum, csa_cum_payment, nonjp_paid_on_jp)
 .assign(total_ytd = lambda df: df['japan_ytd'] + df['asia_ytd'])
 .merge(qtd_sum, how='left', left_index=True, right_index=True)
 .assign(total_qtd = lambda df: df['japan_qtd'] + df['asia_qtd'])
 .drop('Soft', axis=0)
 .fillna(0)
 .sort_values('total_ytd', ascending=False)
.reset_index()
.pipe(format_all_2f)
[['brokerName', 'japan_qtd', 'JPResearch', 'japan_exe_ytd', 'japan_ytd', 'asia_qtd', 'asia_ytd', 'total_qtd', 'total_ytd']]
)


# In[57]:

# q2jpm = ytd_trades #.loc['2017-04-03':]  #,ytd_trades['brokerCode'] == 'JPMF']
# q2jpm[(q2jpm['brokerCode'] == 'RHBO') & (q2jpm['currencyCode'] != 'JPY')]


# In[58]:

# from IPython.display import display, HTML
#HTML(q1[(q1['currencyCode'] == 'HKD') & (q1['brokerCode'] == 'MERT')].to_html())
#HTML(q1[(q1['currencyCode'] != 'JPY') & (q1['brokerName'] == 'Nomura')].to_html())
# q1[(q1['currencyCode'] != 'JPY') & (q1['brokerName'] == 'UBS')].count()


# In[59]:

# con.close()

