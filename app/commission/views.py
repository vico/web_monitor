# -*- encoding: utf8 -*-
from flask import request, g, render_template
from flask.ext.login import login_required

import pandas as pd
import numpy as np
from pandas.io import sql
# import pymysql
from datetime import datetime, date
import japandas as jpd
import pymysql
from . import commissions


def get_ytd_trades(request_date):
    """
    return all trades from start of year upto date
    :param request_date: the year to get trades
    :return: DataFrame of all trades of year
    """

    start_date = '{}-01-01'.format(request_date.year)
    end_date = datetime.strftime(request_date, '%Y-%m-%d')

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
        """.format(start_date, end_date, start_date, end_date), g.con, parse_dates=['tradeDate'], index_col='tradeDate')
    return ytd_trades


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


def format_2f(df):
    t = df.copy()
    columns = ['JPResearch', 'JPExec', 'JPDis', 'res_target', 'balance_usd']
    t[columns] = t[columns].applymap(lambda x: '$ {:12,.0f}'.format(x) if x != 0 else '')
    t['balance_jpy'] = t['balance_jpy'].apply(lambda x: '¥ {:12,.0f}'.format(x) if x != 0 else '')
    t['rank'] = t['rank'].apply(lambda x: '{:.0f}'.format(x) if not np.isnan(x) and x != 0 else '')
    t['research'] = t['research'].apply(lambda x: '{:5.2f}%'.format(x) if x > 0 else '')
    t['accrued'] = t['accrued'].apply(lambda x: '{:5.0f}%'.format(x) if not np.isnan(x) else '')
    t['exec_target'] = t['exec_target'].apply(lambda x: '{:5.0f}%'.format(x) if x > 0 else '')
    return t


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


def format_all_2f(df):
    t = df.copy()
    t = t.applymap(lambda x: '$ {:12,.0f}'.format(x) if x != 0 and type(x) != str else (x if type(x) == str else ''))
    return t


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


def calculate_quarter(from_month):
    return (from_month - 1) // 3 + 1


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
        """.format(year, year, quarter), g.con, index_col="brokerName")
    return csa_cum_payment


def calculate_columns_asia(quarter_trades, ranks_df, asia_quarter_commission_budget, usd_hkd,
                      request_date, ubs_include_list=[]):

    def zero_out_ubs_include(df, ubs_list):
        """ assume index is brokerName, and balance_usd column is already created
        """
        t = df.copy()
        for broker in ubs_list:
            t.loc[broker, 'balance_usd'] = 0

        return t

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


def calculate_soft(quarter_trades, year, month):
    month_key = '{}-{:02d}'.format(year, month)
    if month_key in quarter_trades.index:
        mon_trades = quarter_trades[month_key]
        soft = "{:,.0f}".format(mon_trades[(mon_trades['brokerName'] == 'Soft')]['JPResearch'].sum())
        soft_ms = "{:,.0f}".format(mon_trades[mon_trades['brokerName'] == 'Soft']['JPExec'].sum())
        return soft, soft_ms
    else:
        return 0, 0


def get_fx_rate(price_date):
    """ return tuple of fx rate for USDJPY and USDHKD
    """
    query = """SELECT a.quote, a.rate
                FROM t06DailyCrossRate a
                WHERE a.priceDate="{}" AND a.quote IN ("JPY", "HKD") AND a.base="USD"
    """

    t = pd.read_sql(query.format(price_date.strftime('%Y-%m-%d')), g.con, index_col="quote")
    if t.empty:
        return None, None
    return t.loc['JPY', 'rate'], t.loc['HKD', 'rate']


def get_annual_budget(for_year):
    query = """SELECT a.region, a.amount
                        FROM commission_budget a
                        WHERE a.year={}
    """.format(for_year)

    budget_df = pd.read_sql(query, g.con, index_col='region')
    if budget_df.empty:
        return None, None
    return budget_df.loc['Japan', 'amount'], budget_df.loc['NonJapan', 'amount']


def get_ranks(year, quarter, region='Japan'):
    return pd.read_sql('''
                SELECT b.name AS brokers, a.rank, a.balance_usd AS balance, 
                        a.budget_target AS research
                FROM broker_ranks a
                INNER JOIN brokers b ON a.broker_id=b.id
                WHERE a.year={} AND a.quarter='Q{}' AND b.region='{}'
        '''.format(year, quarter, region), g.con, index_col='brokers')


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
    """.format(region, date_str, date_str, date.year, prev_quarter), g.con)


def adjust_research(df, jp_quarter_commission_budget):
    t = df.copy()
    t['research'] = df['balance_usd'] * 100 / jp_quarter_commission_budget
    return t


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


@commissions.before_request
def before_request():
    # TODO: get username and password from environment file
    g.con = pymysql.connect(host='localhost', user='root', passwd='root', db='hkg02p')
    g.startDate = datetime(datetime.now().year-1, 12, 31).strftime('%Y-%m-%d')
    g.endDate = datetime.now().strftime('%Y-%m-%d')


@commissions.route('/', methods=['GET'])
# @login_required
def index():
    request_date = request.args.get('date')

    if request_date is None:
        request_date = date.today()
    else:
        try:
            request_date = datetime.strptime(request_date, '%Y-%m-%d')
        except ValueError:
            request_date = date.today()

    quarter = calculate_quarter(request_date.month)

    prev_quarter = 4 if quarter == 1 else quarter - 1
    prev_year = request_date.year - 1 if quarter == 1 else request_date.year

    ytd_trades = get_ytd_trades(request_date)

    error_message = []

    if ytd_trades.empty:
        error_message.append("There is no trade data for year {}!".format(request_date.year))

    # get business calendar for JP
    cal = jpd.JapaneseHolidayCalendar()
    cday = pd.offsets.CDay(calendar=cal)
    indexer = pd.date_range('{}-01-01'.format(request_date.year), request_date, freq=cday)

    if request_date in indexer and request_date not in ytd_trades.index:
        error_message.append('There is no trades for requested date!')

    # Asia commission budget is 2mm  USD per year
    # Japan has gone through a few iterations but it's basically approximately 7.5 mm USD per year
    # jp_annual_commission_budget = 7500000  # usd
    # asia_annual_commission_budget = 2000000  # usd
    jp_annual_commission_budget, asia_annual_commission_budget = get_annual_budget(request_date.year)
    if jp_annual_commission_budget is None:
        error_message.append('No budget input for {}'.format(request_date.year))

    jp_ranks_df = get_ranks(prev_year, prev_quarter)  # ranking of last quarter
    nonjp_ranks_df = get_ranks(prev_year, prev_quarter, 'NonJapan')

    if jp_ranks_df.empty:
        error_message.append('No ranking data for {}Q{}'.format(prev_year, prev_quarter))

    # jp_quarter_commission_budget = 10031527/4.0  # to make it same with old system 2016Q1

    usd_jpy, usd_hkd = get_fx_rate(request_date)
    if usd_jpy is None or usd_hkd is None:
        error_message.append('FX rate are not available for {}'.format(request_date.strftime('%Y-%m-%d')))

    if len(error_message) > 0:
        return render_template('commission/index.html',
                               error_message=error_message, date=request_date
                               )

    jp_quarter_commission_budget = jp_annual_commission_budget / 4.0
    asia_quarter_commission_budget = asia_annual_commission_budget / 4.0

    quarter_trades = get_quarter_trades(request_date.year, quarter, ytd_trades)

    csa_brokers = get_csa_brokers(request_date)
    nonjp_csa_brokers = get_csa_brokers(request_date, 'NonJapan')

    asia_columns = calculate_columns_asia(quarter_trades, nonjp_ranks_df, asia_quarter_commission_budget, usd_hkd,
                                          date, nonjp_csa_brokers['brokers'].tolist())

    nonjp_paid_on_jp = (asia_columns
                        .merge(csa_brokers, how='inner', left_on='brokers', right_on='brokers')
                        .pipe(adjust_research, jp_quarter_commission_budget)
                        [['master_brokers', 'brokers', 'balance_usd', 'research']]
                        )

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

    japan_csa_broker_list = csa_brokers[csa_brokers['region'] == 'Japan'][['master_brokers', 'brokers']].values.tolist()
    for (master, broker) in japan_csa_broker_list:
        ranked_df.loc[master, 'research'] += rank_df.loc[broker, 'research']
        ranked_df.loc[broker, 'research'] = 0

    temp = nonjp_paid_on_jp.groupby('master_brokers').sum()
    for master in temp.index.tolist():
        ranked_df.loc[master, 'research'] += temp.loc[master]['research']

    bal = balance.reindex(ranked_df.index)
    table = (ranked_df
             .assign(
        res_target=lambda df: df['research'] * jp_quarter_commission_budget / 100 + (bal if bal is not None else 0))
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

    asia_table = calculate_commission_asia_table(asia_columns, nonjp_paid_on_jp)

    qtd_clearing = "{:,.0f}".format(quarter_trades['Clearing'].sum())
    ytd_clearing = "{:,.0f}".format(ytd_trades['Clearing'].sum())

    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    mon1, mon2, mon3 = [months[(quarter - 1) * 3 + i] for i in range(3)]  # get month names for quarter
    m1, m2, m3 = [(quarter - 1) * 3 + i for i in range(1, 4)]  # get the month numbers for quarter

    soft1, soft_ms1 = calculate_soft(quarter_trades, request_date.year, m1)
    soft2, soft_ms2 = calculate_soft(quarter_trades, request_date.year, m2)
    soft3, soft_ms3 = calculate_soft(quarter_trades, request_date.year, m3)

    soft_qtd = '{:,.0f}'.format(quarter_trades[(quarter_trades['brokerName'] == 'Soft') &
                              (quarter_trades['currencyCode'] == 'JPY')]['JPResearch'].sum())
    soft_msqtd = '{:,.0f}'.format(quarter_trades[(quarter_trades['brokerName'] == 'Soft') &
                              (quarter_trades['currencyCode'] == 'JPY')]['JPExec'].sum())

    qtd_sum = (quarter_trades
               .groupby(['brokerName'])
               .sum()[['JPResearch', 'JPDis', 'JPExec', 'asiaDeal', 'asiaResearch', 'asiaExecution', 'HCSwaps']]
               .assign(japan_qtd=lambda df: df['JPResearch'] + df['JPExec'] + df['JPDis'])
               .assign(asia_qtd=lambda df: df['asiaDeal'] + df['asiaResearch'] + df['asiaExecution'])
               [['japan_qtd', 'asia_qtd']]
               )

    indexer = pd.date_range('{}-{}-{}'.format(request_date.year, 1, 1),
                            '{}-{}-{}'.format(request_date.year, 12, 31),
                            freq=cday)

    if indexer[-1].to_pydatetime() == request_date:
        csa_cum_payment = get_csa_payment(request_date.year, quarter)
    else:
        csa_cum_payment = get_csa_payment(request_date.year, prev_quarter)

    all_commission = (ytd_trades
                      .groupby('brokerName')
                      .sum()[['JPResearch', 'JPDis', 'JPExec', 'asiaDeal', 'asiaResearch', 'asiaExecution', 'HCSwaps']]
                      .assign(japan_exe_ytd=lambda df: df['JPExec'] + df['JPDis'])
                      .pipe(apply_csa_sum, csa_cum_payment)
                      .fillna(0)
                      .assign(japan_ytd=lambda df: df['JPResearch'] + df['JPExec'] + df['JPDis'])
                      .assign(asia_ytd=lambda df: df['asiaResearch'] + df['asiaExecution'] + df['asiaDeal'])
                      .pipe(remove_csa_sum, csa_cum_payment, nonjp_paid_on_jp)
                      .assign(total_ytd=lambda df: df['japan_ytd'] + df['asia_ytd'])
                      .merge(qtd_sum, how='left', left_index=True, right_index=True)
                      .assign(total_qtd=lambda df: df['japan_qtd'] + df['asia_qtd'])
                      .drop('Soft', axis=0)
                      .fillna(0)
                      .sort_values('total_ytd', ascending=False)
                      .reset_index()
                      .pipe(format_all_2f)
                      [['brokerName', 'japan_qtd', 'JPResearch', 'japan_exe_ytd', 'japan_ytd', 'asia_qtd', 'asia_ytd',
                        'total_qtd', 'total_ytd']]
                      )

    return render_template('commission/index.html',
                           main_table=final_table.to_html(index=False, classes=['borderTable', 'center'], na_rep=''),
                           asia_table=asia_table.to_html(index=False, classes=['borderTable', 'center'], na_rep=''),
                           all_table=all_commission.to_html(index=False, classes=['borderTable', 'center'], na_rep=''),
                           qtd_clearing=qtd_clearing, ytd_clearing=ytd_clearing,
                           usdjpy=usd_jpy, usdhkd=usd_hkd,
                           mon1=mon1, mon2=mon2, mon3=mon3,
                           soft1=soft1, soft2=soft2, soft3=soft3,
                           soft_ms1=soft_ms1, soft_ms2=soft_ms2, soft_ms3=soft_ms3,
                           soft_qtd=soft_qtd, soft_msqtd=soft_msqtd,
                           jp_quarter_commission_budget=jp_quarter_commission_budget,
                           asia_quarter_commission_budget=asia_quarter_commission_budget,
                           date=request_date
                           )


@commissions.route('/rank', methods=['GET'])
def rank():
    today = date.today()
    year = request.args.get('year', today.year)
    quarter = request.args.get('quarter', calculate_quarter(today.month))

    ranks = get_ranks(year, quarter)
    if not ranks.empty:
        table = (ranks.reset_index()
                 .sort_values(by='rank')
                 .to_html(classes='table table-stripe', index=False)
                 .replace('border="1"', '')
                 )
    else:
        table = 'No ranking for selected quarter'

    # rank_form = RankSearchForm()
    return render_template('commission/broker_rank.html',
                           table=table,
                           year=year,
                           quarter=int(quarter),
                           )


