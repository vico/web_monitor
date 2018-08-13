from flask import request, g, render_template

import pandas as pd
from pandas import DataFrame
from pandas.io import sql
import numpy as np
import pymysql
from datetime import datetime, timedelta
from decimal import *
from cache import cache
from flask.ext.login import login_required
import japandas as jpd
from collections import defaultdict

from . import main

TIMEOUT = 15 * 60
NUMBER_OF_ROW_PER_PAGE = 40
NUMBER_OF_TOP_POSITIONS = 8


def break_into_page(df, start_new_page=True, finalize_last_page=True,
                    number_of_row_to_break_first_page=NUMBER_OF_ROW_PER_PAGE, table_type='summary',
                    table_caption=''):
    '''
    break a dataframe into many page, each page has NUMBER_OF_ROW_PER_PAGE
    return HTML format of pages and total number of rows
    :rtype: str, int
    '''
    count = 0
    table_html = ''
    style = 'border="1" class="dataframe borderTable"'
    is_first_page = True
    field_separator = '!'
    df.index = df.index.map(lambda x: str(x)[:12])
    rows = df.to_csv(sep=field_separator).split('\n')
    if start_new_page:
        table_html = '</section><section class="sheet padding-10mm">'
    table_header = ('<table %s><thead><tr>' % style) + ''.join(
            ['<th>' + h + '</th>' for h in rows[0].split(field_separator)]) + '</tr></thead>'
    if table_caption != '':
        table_header += '<caption>' + table_caption + '</caption><tbody>'
    else:
        table_header += '<tbody>'

    table_html += table_header

    total_rows = 1
    for r in rows[1:]:
        if is_first_page and count > number_of_row_to_break_first_page and r != '':
            table_html += (
                          '</tbody></table></section><section class="sheet padding-10mm"><table %s>' % style) + table_header
            count = 0
            total_rows += 1
            is_first_page = False
        elif count > NUMBER_OF_ROW_PER_PAGE and r != '':
            table_html += (
                          '</tbody></table></section><section class="sheet padding-10mm"><table %s>' % style) + table_header
            count = 0
            total_rows += 1
        if r != '':
            elements = r.split(field_separator)
            if table_type == 'summary':
                company_name = elements[0].title()
                if finalize_last_page:
                    if elements[0] != 'Total':
                        company_name = ' '.join([part[:4].title() for part in elements[0].split(' ')[:2]])

                table_html += ('<tr>' +
                               ''.join(['<th>' + company_name + '</th>'] +
                                       ['<td>' + '{:.2f}%'.format(float(h) * 100) + '</td>' for h in elements[1:4]] +
                                       ['<td>' +
                                        ('0' if h == '' else '{:,.0f}'.format(float(h))) +
                                        '</td>' for h in elements[4:-2]] +
                                       ['<td>' +
                                        ('0' if h == '' else '{:.1f}%'.format(float(h) * 100)) +
                                        '</td>' for h in elements[-2:]]
                                       ) +
                               '</tr>')
            elif table_type == 'ranking':
                table_html += ('<tr>' +
                               ''.join(['<th>' + elements[0] + '</th>'] +
                                       ['<td>' + h + '</td>' for h in elements[1:-2]] +
                                       ['<td>' +
                                        ('0' if h == '' else '{:,.0f}'.format(float(h))) +
                                        '</td>' for h in elements[-2:-1]] +
                                       ['<td>' + h + '</td>' for h in elements[-1:]]) +
                               '</tr>')

            total_rows += 1
        count += 1
    table_html += '</tbody></table>'
    if finalize_last_page:
        table_html += '</section>'
    return table_html, count


def add_to_page(df, current_page, remaining_row_number, table_type='summary', table_caption='', last_page=False):
    return_html, r_count = break_into_page(df, start_new_page=False, finalize_last_page=last_page,
                                           number_of_row_to_break_first_page=remaining_row_number,
                                           table_type=table_type, table_caption=table_caption)
    if r_count < remaining_row_number:
        remaining_row_number -= r_count
    else:
        return_html, r_count = break_into_page(df, start_new_page=True, finalize_last_page=last_page,
                                               number_of_row_to_break_first_page=NUMBER_OF_ROW_PER_PAGE,
                                               table_type=table_type, table_caption=table_caption)
        remaining_row_number = NUMBER_OF_ROW_PER_PAGE - r_count

    return current_page + return_html, remaining_row_number


@main.before_request
def before_request():
    g.con = pymysql.connect(host='192.168.1.147', user='uploader', passwd='fA6ZxopGrbdb', db='hkg02p')
    start = datetime(datetime.now().year-1, 12, 31)
    if start.weekday() == 6:  # Sunday
        start = start - timedelta(2)
    elif start.weekday() == 5:  # Saturday
        start = start - timedelta(1)
    g.startDate = start.strftime('%Y-%m-%d')
    g.endDate = datetime.now().strftime('%Y-%m-%d')  # not include
    start_year = datetime(datetime.now().year, 1, 1)
    g.startYear = start_year.strftime('%Y-%m-%d')
    g.reportAdvisor = 'AP'
    g.lineWidth = 3
    g.thinLineWidth = 2
    g.indexMapping = {
        'AP': 'TPX',
        'AO': 'TPX',
        'CS': 'TPX',
        'SM': 'TPX',
        'HA': 'TPX',
        'RW': 'TPX',
        'SJ': 'TPX',
        'TI': 'TPX',
        'TT': 'TPX',
        'AQ': 'HSCEI',
        'DL': 'AS51',
        'EL': 'TWSE',
        #'PK': 'TPX',
        #'PK-A': 'KOSPI',
        'Adv': 'TPX',
        'Bal': 'TPX',
        'AP-A': 'HSCEI',
    }
    g.dropList = ['NJD', 'NJA', 'DH', 'TNi', 'PK', 'PK-A']  # , 'Adv', 'Bal']


@main.teardown_request
def teardown_request(exception):
    db = getattr(g, 'db', None)
    if db is not None:
        db.close()
    con = getattr(g, 'con', None)
    if con is not None:
        con.close()


@cache.memoize(TIMEOUT)
def get_turnover_df(from_date, end_date):
    # there is some trades on 2014/12/31 both long and short sides, which is not in database table
    sql_turnover_df = sql.read_sql('''
        -- First date Turnover: the total amount we have invested for open positions at the start of period
        SELECT processDate as tradeDate,
        a.quick AS code,
        a.ccy AS currencyCode,
        a.side,
        a.name,
        ABS(a.quantity)*a.MktPrice*b.rate AS Turnover,
        a.advisor,
        a.strategy,
        UPPER(a.sector) AS sector,
        IF (f.value IS NOT NULL, f.value, 'N.A.') AS GICS,
        IF (i.value = 'REIT', 'Real Estate',  IF(i.value='ETP', 'Index', IF(g.value IS NOT NULL, g.value, 'Non-Japan'))) AS TOPIX,
        IF(a.side='L', a.firstTradeDateLong, firstTradeDateShort) AS firstTradeDate,
        IF(c.instrumentType <> 'EQ', 'Index',
            IF(h.value*d.rate < 250000000,'Micro',
            IF(h.value*d.rate <1000000000, 'Small',
            IF(h.value*d.rate <5000000000, 'Mid',
            IF(h.value*d.rate <20000000000, 'Large',
            IF(h.value IS NULL, 'Micro','Mega'))) ))) AS MktCap
        FROM t05PortfolioResponsibilities a
        INNER JOIN t06DailyCrossRate b ON a.processDate = b.priceDate AND a.ccy = b.base AND b.quote='JPY'
        INNER JOIN t01Instrument c ON c.instrumentID = a.instrumentID
        INNER JOIN t06DailyCrossRate d ON a.processDate = d.priceDate AND a.ccy = d.base AND d.quote='USD'
        LEFT JOIN t06DailyBBStaticSnapshot h ON c.instrumentID = h.instrumentID AND h.dataType = 'CUR_MKT_CAP'
        LEFT JOIN t06DailyBBStaticSnapshot f ON c.instrumentID = f.instrumentID AND f.dataType = 'GICS_SECTOR_NAME'
        LEFT JOIN t06DailyBBStaticSnapshot g ON c.instrumentID = g.instrumentID AND g.dataType = 'JAPANESE_INDUSTRY_GROUP_NAME_ENG'
        LEFT JOIN t06DailyBBStaticSnapshot i ON c.instrumentID = i.instrumentID AND i.dataType = 'SECURITY_TYP'
        WHERE processDate = '%s'
    UNION ALL
        SELECT aa.tradeDate,
        aa.code,
        aa.currencyCode,
        aa.side,
        aa.name,
        ABS(Notl) AS Turnover,
        e.advisor,
        e.strategy,
        UPPER(e.sector) AS sector,
        IF (f.value IS NOT NULL, f.value, 'N.A.') AS GICS,
        IF (i.value = 'REIT', 'Real Estate',  IF(i.value='ETP', 'Index', IF(g.value IS NOT NULL, g.value, 'Non-Japan'))) AS TOPIX,
        aa.firstTradeDate,
        aa.MktCap
        FROM (
        -- Get list of trades for each instrument, trade date, order type (B/S), side (L/S), quantity, notation, code
        -- which has max trade date
        SELECT b.code,
        d.currencyCode,
        b.side,
        IF(orderType="B",1,-1)*b.quantity AS Qty,
        IF(orderType="B",-1,1)*b.net*i.rate AS Notl,
        MAX(a.adviseDate) AS `MaxOfDate`,
        b.reconcileID,
        b.tradeDate,
        b.equityType,
        c.instrumentType,
        c.name,
        c.instrumentID,
        b.orderType,
        a.strategy,
        IF (z.side='L', firstTradeDateLong, firstTradeDateShort) AS firstTradeDate,
        IF(c.instrumentType <> 'EQ', 'Index', IF(h.value*j.rate < 250000000,'Micro',
                        IF(h.value*j.rate <1000000000, 'Small', IF(h.value*j.rate <5000000000, 'Mid', IF(h.value*j.rate <20000000000, 'Large', IF(h.value IS NULL, 'Micro','Mega'))) ))) AS MktCap
        FROM t08AdvisorTag a
        INNER JOIN t08Reconcile b ON a.code = b.code
        INNER JOIN t01Instrument c ON (b.equityType = c.instrumentType) AND (b.code = c.quick)
        INNER JOIN t02Currency d ON c.currencyID = d.currencyID
        INNER JOIN `t06DailyCrossRate` j ON j.priceDate = b.processDate AND j.base=d.currencyCode AND j.quote='USD'
        INNER JOIN `t06DailyCrossRate` i ON i.priceDate = b.processDate AND i.base=d.currencyCode AND i.quote='JPY'
        LEFT JOIN t05PortfolioResponsibilities z ON z.instrumentID = c.instrumentID AND z.processDate = b.processDate
        LEFT JOIN t06DailyBBStaticSnapshot h ON c.instrumentID = h.instrumentID AND h.dataType = 'CUR_MKT_CAP'
        WHERE a.adviseDate<= b.processDate
            AND b.processDate > '%s' AND b.processDate < '%s' # Grab Analyst Trades start date
            AND b.equityType<>"OP"
            AND b.srcFlag="K"
        GROUP BY c.instrumentID, b.tradeDate, b.orderType, b.reconcileID, b.side, Qty, Notl, b.code
        ORDER BY b.code
        ) aa
        LEFT JOIN t08AdvisorTag e ON (aa.MaxOfDate = e.adviseDate) AND (aa.code = e.code)
        LEFT JOIN t06DailyBBStaticSnapshot f ON aa.instrumentID = f.instrumentID AND f.dataType = 'GICS_SECTOR_NAME'
        LEFT JOIN t06DailyBBStaticSnapshot g ON aa.instrumentID = g.instrumentID AND g.dataType = 'JAPANESE_INDUSTRY_GROUP_NAME_ENG'
        LEFT JOIN t06DailyBBStaticSnapshot i ON aa.instrumentID = i.instrumentID AND i.dataType = 'SECURITY_TYP'
        WHERE (aa.side="L" AND aa.orderType="B") OR (aa.side="S" AND aa.orderType="S")
        ;
         ''' % (from_date, from_date, end_date), g.con, parse_dates=['tradeDate'], coerce_float=True, index_col='tradeDate')

    return sql_turnover_df


@cache.memoize(TIMEOUT)
def get_hit_rate_df(from_date, end_date):
    """
        Return a hit rate dataframe of all advisors between from_date to end_date

        Hit rate: hit rate of a side is ratio of number of win trades/positions over all number of trades/positions.
        A trade/position is called a win if it has positive attribution, attribution is calculated as sum of all
        attributions from the time the position is opened (first trade) until it is closed.
    """

    hit_rate_df = sql.read_sql('''select advisor, SUM(IF(RHAttr > 0 AND side='L',1,0)) as LongsWin,
        SUM(IF(side='L' and RHAttr <> 0,1,0)) as Longs,
        SUM(if(RHAttr > 0 and side='L',1,0))*100/sum(if(side='L' AND RHAttr <> 0,1,0)) as LongsHR,
    SUM(IF(RHAttr > 0 AND side='S',1,0)) AS ShortsWin, SUM(IF(side='S' AND RHAttr <> 0,1,0)) AS Shorts,
    SUM(if(RHAttr > 0 and side='S',1,0))*100/SUM(if(side='S' AND RHAttr <> 0,1,0)) as ShortsHR
    from (
    SELECT quick,firstTradeDateLong, side, SUM(RHAttribution) as RHAttr, advisor, SUM(attribution)
    FROM t05PortfolioResponsibilities
    where processDate > '%s' and processDate < '%s'
    and quick not like '%%DIV%%'
    and advisor <> ''
    group by quick,firstTradeDateLong
    ) a
    group by a.advisor
    ;''' % (from_date, end_date), g.con, coerce_float=True, index_col='advisor')
    return hit_rate_df


@cache.memoize(TIMEOUT)
def get_pl_df(from_date, end_date):
    pl_df = sql.read_sql('''SELECT a.processDate,advisor, side, a.quick, attribution,
                            RHAttribution AS RHAttr,
                            YAAttribution AS YAAttr,
                            LRAttribution AS LRAttr,
                            RHNAV, YANAV, LRNAV,
                            a.quantity AS position,
                            t.RHPos + t.YAPos + t.LRPos AS ShortPos,
                            t.RHPos, t.YAPos, t.LRPos, h.PB,
                            h.avg_rate, h.borrow_quantity,
                            h.settleDate, h.borrowCCY,
                            IF (f.value IS NOT NULL, f.value, 'N.A.') AS GICS,
                            IF (i.value = 'REIT', 'Real Estate',  IF(i.value='ETP', 'Index',
                                IF(g.value IS NOT NULL, g.value, 'Non-Japan'))) AS TPX,
                            strategy, firstTradeDateLong, firstTradeDateShort,
                            IF(c.instrumentType <> 'EQ', 'Index', IF(d.value*b.rate < 250000000,'Micro',
                                                IF(d.value*b.rate <1000000000, 'Small',
                                                IF(d.value*b.rate <5000000000, 'Mid',
                                                IF(d.value*b.rate <20000000000, 'Large',
                                                IF(d.value IS NULL, 'Micro','Mega'))) ))) AS MktCap
                FROM `t05PortfolioResponsibilities` a
                INNER JOIN `t06DailyCrossRate` b ON a.processDate=b.priceDate AND a.CCY=b.base AND b.quote='USD'
                INNER JOIN t01Instrument c ON c.instrumentID = a.instrumentID
                LEFT JOIN t06DailyBBStaticSnapshot d ON d.instrumentID = a.instrumentID AND d.dataType = 'CUR_MKT_CAP'
                LEFT JOIN t06DailyBBStaticSnapshot f ON d.instrumentID = f.instrumentID AND f.dataType = 'GICS_SECTOR_NAME'
                LEFT JOIN t06DailyBBStaticSnapshot g ON d.instrumentID = g.instrumentID AND g.dataType = 'JAPANESE_INDUSTRY_GROUP_NAME_ENG'
                LEFT JOIN t06DailyBBStaticSnapshot i ON d.instrumentID = i.instrumentID AND i.dataType = 'SECURITY_TYP'
                LEFT JOIN (
                    SELECT processDate, instrumentID,
                      SUM(IF(portfolioID = 1, quantity, 0)) AS RHPos,
                        SUM(IF(portfolioID = 2, quantity, 0)) AS YAPos,
                          SUM(IF(portfolioID = 3, quantity, 0)) AS LRPos
                    FROM t05PortfolioPosition 
                    WHERE processDate > '%s' AND processDate < '%s' AND side="S"
                    GROUP BY instrumentID, processDate
                  ) t ON t.instrumentID=a.instrumentID AND t.processDate=a.processDate
                LEFT JOIN
                  (SELECT a.tradeDate, a.code, SUM(a.borrow * a.rate)/SUM(a.borrow) AS avg_rate, b.quantity AS short_quantity,
                     b.settleDate, SUM(borrow) AS borrow_quantity, b.PB, b.currencyCode AS borrowCCY
                    FROM t08UploadSSBorrow a
                      LEFT JOIN (SELECT a.code, a.tradeDate, SUM(a.quantity) AS quantity, a.settleDate, a.PB,
                       d.currencyCode
                      FROM t08Reconcile a
                       INNER JOIN t01Instrument c ON a.code=c.quick
                        INNER JOIN t02Currency d ON c.currencyID=d.currencyID
                      WHERE a.srcFlag="K" AND a.side="S"
                      GROUP BY a.tradeDate, a.code) b ON a.code=b.code
                                                   AND a.tradeDate=b.tradeDate
                    GROUP BY a.tradeDate, a.code) h ON h.code=c.quick AND a.processDate=h.tradeDate
                WHERE a.processDate > '%s' AND a.processDate < '%s'
                AND advisor <> ''
                AND a.quick NOT LIKE "DIV%%"
                AND a.quick NOT LIKE "FX%%"
                          ;''' % (from_date, end_date, from_date, end_date), g.con,
                         coerce_float=True, parse_dates=['processDate', 'settleDate'])
    return pl_df


@cache.memoize(TIMEOUT)
def get_aum_df(from_date, end_date):
    aum_df = sql.read_sql('''SELECT processDate, MAX(RHAUM) AS RHAUM, MAX(YAAUM) AS YAAUM, MAX(LRAUM) AS LRAUM
      FROM (
      SELECT processDate,portfolioID,
        AVG(CASE portfolioID WHEN 1 THEN `value` END) AS RHAUM,
          AVG(CASE portfolioID WHEN 2 THEN `value` END) AS YAAUM,
            AVG(CASE portfolioID WHEN 3 THEN `value` END) AS LRAUM
            FROM t05PortfolioReport
            WHERE processDate>='%s' AND processDate < '%s' AND portfolioID>0 AND dataType="PSTJNAV"
            GROUP BY processDate, portfolioID
            ) a
            GROUP BY processDate
            ;''' % (from_date, end_date), g.con, coerce_float=True, parse_dates=['processDate'], index_col='processDate')

    aum_df['Total'] = aum_df['RHAUM'] + aum_df['YAAUM'] + aum_df['LRAUM']
    return aum_df


@cache.memoize(TIMEOUT)
def get_code_beta():
    code_beta_df = sql.read_sql('''SELECT TRIM(a.code) AS code, a.beta, UPPER(a.sector) AS sector
          FROM t08AdvisorTag a,
            (SELECT advisorTagID, code, MAX(t08AdvisorTag.adviseDate) AS MaxOfadviseDate
            FROM t08AdvisorTag
            GROUP BY t08AdvisorTag.code) b
            WHERE #a.advisorTagID = b.advisorTagID
            a.code = b.code
            AND b.MaxOfadviseDate = a.adviseDate
            ;''', g.con, coerce_float=True)

    return code_beta_df


@cache.memoize(TIMEOUT)
def get_exposure_df(from_date, end_date):
    exposure_df = sql.read_sql('''SELECT processDate, advisor, quick,
         side, RHExposure, YAExposure, LRExposure
         FROM `t05PortfolioResponsibilities`
         WHERE processDate >= '%s' AND processDate < '%s'
         AND advisor <> ''
         ;''' % (from_date, end_date), g.con, coerce_float=True, parse_dates=['processDate'])

    return exposure_df


@cache.memoize(TIMEOUT)
def get_borrow_fee_df(sql_pl_df):
    first_trade_date = np.where(sql_pl_df['side'] == 'L',
                                sql_pl_df['firstTradeDateLong'],
                                sql_pl_df['firstTradeDateShort']
                                )

    from functools import reduce
    name_list = reduce(lambda x, y: '{}, {}'.format(x, y),
                       map(lambda x: '"{}"'.format(x),
                           np.unique(sql_pl_df['quick']).tolist()
                           )
                       )

    prices_df = pd.read_sql("""
        SELECT a.quick, b.priceDate, b.close
        FROM t01Instrument a 
        INNER JOIN t06DailyPrice b ON a.instrumentID=b.instrumentID
        WHERE b.priceDate >= '{}' AND b.priceDate <= '{}'
         AND a.quick IN {}
         ;
        """.format(g.startYear, g.endDate, '(' + name_list + ')'), g.con, index_col=['quick', 'priceDate'])

    def get_settle_date(quick, trade_date):
        sql = """
        SELECT settleDate FROM t08Reconcile a
        WHERE a.tradeDate="{}" AND a.srcFlag="K" AND a.code="{}"
        LIMIT 1
        """
        ret = pd.read_sql(sql.format(trade_date.strftime('%Y-%m-%d'), quick), g.con)
        if ret.empty:
            return None
        return ret['settleDate'].values[0]

    def drop_price_tail(df):
        if not df.empty:
            while pd.isnull(df.iloc[-1].close) and len(df.index) > 1:
                df = df.iloc[:-1]
        return df

    def fill_avg_rate(df):
        if not df.empty:
            t = df.copy()
            u = t[['borrow_quantity', 'avg_rate']].dropna()
            avg_rate = (u['avg_rate'] * u['borrow_quantity']).sum() / u['borrow_quantity'].sum()
            t['avg_rate'] = avg_rate
            return t
        else:
            return df

    new_df = pd.DataFrame()

    for key, group in sql_pl_df.groupby(['quick', first_trade_date]):
        # if key[1] < datetime.strptime(startDate, '%Y-%m-%d') and np.all(np.isnan(group['avg_rate'])):
        if group.iloc[0].side == 'S' and np.all(pd.isnull(group['avg_rate'])):
            # such case like borrow started yester-year and there is no borrow this year
            # find borrow rate and borrow quantity from open date until startDate,
            # average out the rate and assign it to first row of group
            rate_df = pd.read_sql("""
                    SELECT a.tradeDate, a.code, SUM(a.borrow * a.rate)/SUM(a.borrow) AS avg_rate, b.settleDate,
           SUM(borrow) AS borrow_quantity,  b.PB, b.currencyCode
                            FROM t08UploadSSBorrow a
                              INNER JOIN t01Instrument c ON a.code=c.quick
                              INNER JOIN t06DailyPrice d ON c.instrumentID=d.instrumentID AND d.priceDate=a.tradeDate
                              LEFT JOIN (SELECT a.code, a.tradeDate, SUM(a.quantity) AS quantity, a.settleDate, a.PB
                                , d.currencyCode
                              FROM t08Reconcile a
                                INNER JOIN t01Instrument c ON a.code=c.quick
                                INNER JOIN t02Currency d ON c.currencyID=d.currencyID
                              WHERE a.srcFlag="K" AND a.side="S"
                              GROUP BY a.tradeDate, a.code) b ON a.code=b.code
                                                           AND a.tradeDate=b.tradeDate
                            WHERE a.code="{}" AND a.processDate >= '{}' AND a.processDate < '{}'
                            GROUP BY a.tradeDate, a.code
                    """.format(key[0], key[1], g.startYear), g.con, parse_dates=['tradeDate'])

            if not rate_df.empty:
                avg_rate = (rate_df['avg_rate'] * rate_df['borrow_quantity']).sum() / rate_df['borrow_quantity'].sum()
                borrow_quantity = rate_df['borrow_quantity'].sum()
                interval = datetime.combine(rate_df.iloc[0].settleDate, datetime.min.time()) - datetime.combine(
                    rate_df.iloc[0].tradeDate, datetime.min.time())
                group.iloc[0, 16] = rate_df.iloc[0].PB
                group.iloc[0, 17] = avg_rate
                group.iloc[0, 18] = borrow_quantity
                group.iloc[0, 19] = group.iloc[0].processDate + interval
                group.iloc[0, 20] = rate_df.iloc[0].currencyCode

        if not np.all(pd.isnull(group['avg_rate'])):
            settle_date = group.iloc[0]['settleDate']
            trade_date = group.iloc[0]['processDate']

            if pd.isnull(settle_date):
                settle_date = datetime.combine(get_settle_date(key[0], key[1]), datetime.min.time())
                trade_date = datetime.combine(key[1], datetime.min.time())

            interval = settle_date - trade_date

            loan_range = pd.date_range(group.iloc[0]['processDate'], group.iloc[-1]['processDate'])

            if group.iloc[-1]['processDate'] + interval >= datetime.strptime(g.endDate, '%Y-%m-%d'):
                price_range = pd.date_range(group.iloc[0]['processDate'],
                                            datetime.strptime(g.endDate, '%Y-%m-%d') - timedelta(1))
            else:
                price_range = pd.date_range(group.iloc[0]['processDate'], group.iloc[-1]['processDate']
                                            + interval  # getting prices for already closed position
                                            )

            if group.iloc[0]['borrow_quantity'] < group.iloc[0]['position']:  # we covered on same day
                group = group.dropna(subset=['borrow_quantity'])

            # be careful of trades with more than 1 advisor. It causes index below duplicated
            # TODO: this quirk is just to remove duplicated entries, not adjust fairnessly the
            # borrow cost for each analysts share same code.
            duplicated_index = group.set_index('processDate').index.duplicated(keep='first')
            cleaned_group = group[~duplicated_index]

            fee_group = (cleaned_group.set_index('processDate')
                         .reindex(loan_range)
                         .pipe(fill_avg_rate)
                         .merge(prices_df
                                .loc[key[0]]
                                .reindex(price_range)
                                .fillna(method='ffill')  # fill out weekends for 6756
                                .shift(-interval.days)
                                .pipe(drop_price_tail)
                                #                          .fillna(method='ffill')
                                .dropna(subset=['close'])
                                ,
                                left_index=True, right_index=True, how='inner'
                                )
                         .fillna(method='ffill')
                         .assign(
                fee=lambda df: df['ShortPos'] * df['close'] * df['avg_rate'] / (100 * 360)
            )
                         .assign(rhfee=lambda df: df['RHPos'] * df['close'] * df['avg_rate'] / (100 * 360))
                         .assign(yafee=lambda df: df['YAPos'] * df['close'] * df['avg_rate'] / (100 * 360))
                         .assign(lrfee=lambda df: df['LRPos'] * df['close'] * df['avg_rate'] / (100 * 360))
                         #                 .pipe(change_index, interval)
                         #                                 [['avg_rate', 'borrow_quantity', 'position', 'fee']]
                         )
            if not fee_group.empty:
                t = fee_group[['fee', 'rhfee', 'yafee', 'lrfee']].resample('B').sum()
                t['quick'] = key[0]
                new_df = new_df.append(t).dropna()

    return new_df


@cache.memoize(TIMEOUT)
def get_index_return(from_date, end_date):
    index_df = sql.read_sql('''SELECT b.priceDate, a.indexCode, b.close
      FROM `t07Index` a, `t06DailyIndex` b
      WHERE a.indexID = b.indexID
      AND b.priceDate >= '%s' AND b.priceDate < '%s'
      AND a.indexCode IN ('TPX','KOSPI','TWSE','HSCEI', 'AS51')
      ;''' % (from_date, end_date), g.con, coerce_float=True, parse_dates=['priceDate'])
    p_index_df = index_df.pivot('priceDate', 'indexCode', 'close')
    p_index_df.fillna(method='ffill', inplace=True)  # fill forward for same value of previous day for holidays
    index_return = p_index_df.pct_change()  # index_return = p_index_df / p_index_df.shift(1) - 1
    # index_return = index_return.fillna(method='ffill', inplace=True)  # for index like TWSE has data for Sat
    return index_return, p_index_df


@cache.memoize(TIMEOUT)
def get_code_name_map():
    code_name_map = sql.read_sql('''SELECT quick, name FROM t01Instrument;''', g.con)
    return code_name_map


@main.route('/', methods=['GET', 'POST'])
#@login_required
def index():
    # TODO: change all double quotes to single quote for consistence
    # TODO: verify cache invalidate
    # TODO: test Redis as cache backend
    # TODO: find a good way to reduce first access turn around time
    # TODO: add send PDFs to specified email
    # TODO: add checker view (should have pie graph to see what is missing)
    # TODO: add 1 year summary attribution page

    calendar = jpd.JapaneseHolidayCalendar()
    cday = pd.offsets.CDay(calendar=calendar)

    param_adviser = request.args.get('analyst', g.reportAdvisor)
    start_date = request.args.get('startDate', g.startDate)
    end_date = request.args.get('endDate', g.endDate)
    borrow_fee = request.args.get('borrowfee', True)

    indexer = pd.date_range(start_date, end_date, freq=cday)
    error_message = []
    if start_date not in indexer:
        error_message.append('The start-date is not a working date.')

    nticks = len(pd.date_range(start_date, end_date, freq='BM'))

    code_name_map = get_code_name_map()

    hit_rate_df = get_hit_rate_df(start_date, end_date)

    turnover_df = get_turnover_df(start_date, end_date)
    turnover_df.sort_index(inplace=True)

    # create new column which contain turnover in JPY
    turnover_df['JPYPL'] = turnover_df['Turnover']

    # process code that has over 1 advs code
    # divided turnover by 2, assuming only have 2 advs share code
    turnover_df.loc[turnover_df['advisor'].str.contains('/'), 'JPYPL'] /= 2
    # replace share code with selected adv code
    turnover_df['advisor'] = turnover_df['advisor'].str.replace(r"%s/.*|.*/%s" % (param_adviser, param_adviser), param_adviser)

    # calculate total turnover for each side
    total_turnover = turnover_df.truncate(after=end_date).groupby(['side']).sum()['JPYPL']

    # calculate turnover for each advisor
    sum_turnover_per_adv = (turnover_df.truncate(after=end_date)
                            .groupby(['advisor', 'side'])
                            .sum()['JPYPL']
                            .unstack())

    # print(sum_turnover_per_adv)
    #
    # t1 = turnover_df.truncate(after=end_date).groupby(["advisor", "side"]).sum()['JPYPL'].unstack()
    # t2 = t1.loc[t1.index.str.contains('/')].copy()
    #
    # new_data = defaultdict(float)
    # for ind in t2.index:
    #     for new_ind in ind.split('/'):
    #         if new_ind in new_data:
    #             new_data[new_ind] += t2.loc[ind]
    #         else:
    #             new_data[new_ind] = t2.loc[ind]
    # sum_shared_turnover = (pd.DataFrame.from_dict(new_data, orient='index')
    #                        .div(2)
    #                        .fillna(0)
    #                        .reindex(sum_turnover_per_adv.dropna().index, fill_value=0)
    #                        )
    # sum_turnover_per_adv = sum_turnover_per_adv.dropna() + sum_shared_turnover

    sum_turnover_per_adv = sum_turnover_per_adv.reindex(g.indexMapping.keys())

    total_ratio = (sum_turnover_per_adv * 100 / total_turnover).fillna(0)  # % TOTAL

    aum_df = get_aum_df(start_date, end_date)

    code_beta_df = get_code_beta()

    f_exposure_df = get_exposure_df(start_date, end_date)

    names_df = f_exposure_df.groupby(by=['processDate', 'advisor']).count()['quick']

    mf_exposure_df = f_exposure_df.merge(code_beta_df, how='left', left_on='quick', right_on='code')
    sum_exposure_per_adv_side = (mf_exposure_df.groupby(['processDate', 'advisor', 'side'])
                                 .sum()[['RHExposure', 'YAExposure', 'LRExposure']])

    temp2 = mf_exposure_df.set_index(['processDate', 'advisor', 'side'])

    t2 = (temp2['RHExposure'].mul(aum_df['RHAUM'], axis=0).mul(temp2['beta'].fillna(0), axis=0) +
          temp2['YAExposure'].mul(aum_df['YAAUM'], axis=0).mul(temp2['beta'].fillna(0), axis=0) +
          temp2['LRExposure'].mul(aum_df['LRAUM'], axis=0).mul(temp2['beta'].fillna(0), axis=0))

    t3 = t2.reset_index()  # .drop('quick',1)
    t4 = t3.groupby(['processDate', 'advisor', 'side']).sum()
    t4.columns = ['exposure']

    beta_exposure_df = t4['exposure']
    all_fund_exposure_in_money = (sum_exposure_per_adv_side['RHExposure'].mul(aum_df['RHAUM'], axis=0) +
                                  sum_exposure_per_adv_side['YAExposure'].mul(aum_df['YAAUM'], axis=0) +
                                  sum_exposure_per_adv_side['LRExposure'].mul(aum_df['LRAUM'], axis=0))

    all_fund_exposure_in_money.columns = ['Exposure']

    sql_pl_df = get_pl_df(start_date, end_date)
    sql_pl_df = sql_pl_df.merge(code_name_map, left_on='quick', right_on='quick')

    if (f_exposure_df[f_exposure_df['advisor'] == param_adviser].empty and
            turnover_df[turnover_df['advisor'] == param_adviser].empty and
            sql_pl_df[sql_pl_df['advisor'] == param_adviser].empty):
        error_message.append('Currently, there is no data for {}'.format(param_adviser))

    if sql_pl_df.empty:
        error_message.append('There is no PL information for specificed date range.')

    if len(error_message) > 0:
        return render_template('main/error_message.html', error_message=error_message)

    first_trade_date = np.where(sql_pl_df['side'] == 'L',
                                sql_pl_df['firstTradeDateLong'],
                                sql_pl_df['firstTradeDateShort']
                                )
    if borrow_fee:
        new_df = get_borrow_fee_df(sql_pl_df)

        sql_pl_df = (sql_pl_df
                     .merge(new_df.reset_index(), left_on=['processDate', 'quick'],
                            right_on=['index', 'quick'], how='left')
                     .fillna(0)
                     )
        sql_pl_df['attribution'] -= sql_pl_df['fee']
        sql_pl_df['RHAttr'] -= sql_pl_df['rhfee'] / sql_pl_df['RHNAV']
        sql_pl_df['YAAttr'] -= sql_pl_df['yafee'] / sql_pl_df['YANAV']
        sql_pl_df['LRAttr'] -= sql_pl_df['lrfee'] / sql_pl_df['LRNAV']

    t = (sql_pl_df.groupby(['processDate', 'advisor', 'side'])
         .sum()
         .drop(['RHAttr', 'YAAttr', 'LRAttr'], axis=1)
         .unstack()
         .reset_index()
         .set_index('processDate'))

    attr_df = t[t['advisor'] == param_adviser]['attribution']
    attr_df.fillna(0, inplace=True)  # fixed pl graph when there is na values, which caused Total become 0
    attr_df['Total'] = attr_df['L'] + attr_df['S']
    cs_attr_df = attr_df
    cs_attr_df = cs_attr_df.cumsum().fillna(method='ffill').fillna(0)

    long_short_return = (sql_pl_df.groupby(["advisor", "side"])
                         .sum()
                         .drop(['RHAttr', 'YAAttr', 'LRAttr'], axis=1)
                         .unstack()
                         .div(sum_turnover_per_adv, axis=0)) * 100

    index_net_return, index_df = get_index_return(start_date, end_date)

    advisor_exposure = all_fund_exposure_in_money[:, param_adviser].unstack().shift(1)

    exposure_avg = DataFrame(all_fund_exposure_in_money).reset_index()

    t = DataFrame(all_fund_exposure_in_money).reset_index()

    gross_exposure = t.groupby(by=['processDate', 'advisor'])[0].sum().div(aum_df['Total'], axis=0)

    t2 = t[t['side'] == 'S'].set_index(['processDate', 'advisor'])[0].div(aum_df['Total'], axis=0)

    t3 = DataFrame(t[t['side'] == 'S']
                   .set_index(['processDate', 'advisor'])[0]
                   .div(aum_df['Total'], axis=0)).reset_index()

    t4 = t3.groupby(by='processDate')[0].sum().truncate(before=start_date)

    short_exposure = t2.div(t4, axis=0)

    rank_long_df = (exposure_avg[(exposure_avg['side'] == 'L')]
                    .groupby(by='advisor')
                    .mean() * 100 / aum_df['Total'].mean())

    ranke_short_df = (exposure_avg[(exposure_avg['side'] == 'S')]
                      .groupby(by='advisor')
                      .mean() * 100 / aum_df['Total'].mean())

    rank_long_df = rank_long_df.drop(g.dropList, errors='ignore').rank(ascending=False)
    ranke_short_df = ranke_short_df.drop(g.dropList, errors='ignore').rank(ascending=False)

    net_op = DataFrame()

    net_op['L'] = (attr_df['L'].sub(advisor_exposure['L']
                                    .mul(index_net_return[g.indexMapping[param_adviser]], axis=0), axis=0)
                   .div(aum_df.shift(1)['Total'], axis=0))

    if 'S' in advisor_exposure:
        net_op['S'] = (attr_df['S'].sub((advisor_exposure['S'] * -1)
                                    .mul(index_net_return[g.indexMapping[param_adviser]], axis=0), axis=0)
                   .div(aum_df.shift(1)['Total'], axis=0))
    else:
        net_op['S'] = pd.Series(0, index=attr_df['S'].index)

    net_op = net_op.cumsum().fillna(method='ffill').fillna(0)  # fill na forward and then fill 0 at beginning
    net_op['Total'] = net_op['L'] + net_op['S']

    beta_exposure = beta_exposure_df[:, param_adviser].unstack().shift(1)
    alpha = DataFrame()
    alpha['L'] = (attr_df['L'].sub(beta_exposure['L']
                                     .mul(index_net_return[g.indexMapping[param_adviser]], axis=0), axis=0)
                    .div(aum_df.shift(1)['Total'], axis=0))

    if 'S' in beta_exposure:
        alpha['S'] = (attr_df['S'].sub((beta_exposure['S'] * -1)
                                     .mul(index_net_return[g.indexMapping[param_adviser]], axis=0), axis=0)
                    .div(aum_df.shift(1)['Total'], axis=0))
    else:
        alpha['S'] = pd.Series(0, attr_df['S'].index)

    alpha = alpha.cumsum().fillna(method='ffill').fillna(0)  # fill na forward and then fill 0 at beginning

    alpha['Total'] = alpha['L'] + alpha['S']

    total_fund = (sql_pl_df.groupby(['processDate', 'advisor', 'side'])
                  .sum()
                  .drop(['attribution'], axis=1)
                  .unstack()
                  .reset_index()
                  .set_index('processDate'))

    cs_index_return = index_df/index_df.iloc[1]-1

    # calculate range for two graph so that we can make them have same 0 of y axis
    positive_pl_bound = 1.1*abs(max([max(cs_attr_df.max().values), min(cs_attr_df.min().values), 0]))
    negative_pl_bound = 1.1*abs(min([min(cs_attr_df.min().values), max(cs_attr_df.max().values), 0]))
    positive_index_bound = 1.1*abs(max([cs_index_return[g.indexMapping[param_adviser]].max(), 0]))
    negative_index_bound = 1.1*abs(min([cs_index_return[g.indexMapping[param_adviser]].min(), 0]))

    range1 = [0, 0]
    range2 = [0, 0]

    if positive_pl_bound == 0 and positive_index_bound == 0:
        range1 = [-negative_pl_bound, 0]
        range2 = [-negative_index_bound, 0]
    elif positive_pl_bound == 0 and positive_index_bound > 0 and negative_index_bound != 0:
        range1 = [-negative_pl_bound*positive_index_bound/negative_index_bound, negative_pl_bound]
        range2 = [-negative_index_bound, positive_index_bound]
    elif negative_pl_bound == 0 and negative_index_bound == 0:
        range1 = [0, positive_pl_bound]
        range2 = [0, positive_index_bound]
    elif negative_pl_bound == 0 and negative_index_bound > 0 and positive_index_bound != 0:
        range1 = [-positive_pl_bound * negative_index_bound / positive_index_bound, positive_pl_bound]
        range2 = [-negative_index_bound, positive_index_bound]
    elif positive_pl_bound == 0 and positive_index_bound > 0 and negative_index_bound == 0:
        range1 = [-negative_pl_bound, negative_pl_bound]
        range2 = [-positive_index_bound, positive_index_bound]
    elif negative_pl_bound == 0 and negative_index_bound > 0 and positive_index_bound == 0:
        range1 = [-positive_pl_bound, positive_pl_bound]
        range2 = [-negative_index_bound, negative_index_bound]
    elif positive_pl_bound > 0 and negative_pl_bound > 0 and positive_index_bound == 0 and negative_index_bound > 0:
        range1 = [-negative_pl_bound, positive_pl_bound]
        range2 = [-negative_index_bound, positive_pl_bound*negative_index_bound/negative_pl_bound]
    elif positive_pl_bound > 0 and negative_pl_bound > 0 and positive_index_bound > 0 and negative_index_bound > 0:
        range1 = [-negative_pl_bound, positive_pl_bound]
        if positive_index_bound > negative_index_bound:
            range2 = [-negative_pl_bound*positive_index_bound/positive_pl_bound, positive_index_bound]
        else:
            range2 = [-negative_index_bound, positive_pl_bound*negative_index_bound/negative_pl_bound]

    range2 = list(map(lambda x: x*100, range2))  # map is iterable and cannot be serialized to JSON -> list

    render_obj = dict()
    render_obj['graph_width'] = 750
    render_obj['graph_height'] = 240
    render_obj['graph_line_width'] = g.lineWidth
    render_obj['margin_left'] = 40
    render_obj['margin_top'] = 40
    render_obj['margin_bottom'] = 34
    render_obj['margin_right'] = 5
    render_obj['graph_font'] = 'Calibri'
    render_obj['tick_font_size'] = 10
    render_obj['nticks'] = nticks
    render_obj['analyst'] = param_adviser
    render_obj['num_adv'] = len(g.indexMapping.keys())

    pl_graph = {'data': [{
                    'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in cs_attr_df.index],
                    'y': cs_attr_df[col].values.tolist(),
                    'name': ('Long' if col == 'L' else ('Short' if col == 'S' else col)) + ' PL',
                    'line': {'width': g.lineWidth,
                             'color': "rgb(27, 93, 225)" if col == 'L' else ("rgb(214,39,40)" if col == 'S'
                                                                             else "rgb(0,0,0)")
                             }
                } for col in cs_attr_df.columns
                ] + [{
                        'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in index_df.index],
                        'y': (cs_index_return[g.indexMapping[param_adviser]].dropna()*100).values.tolist(),
                        'name': g.indexMapping[param_adviser],
                        'fill': 'tozeroy',
                        'line': {'width': 0},
                        'yaxis': 'y2'
                }],
                'layout': {
                    'margin': {'t': 0, 'b': 15, 'l': render_obj['margin_left'], 'r': 40},
                    'width': render_obj['graph_width'],
                    'height': render_obj['graph_height'],
                    'xaxis': {'tickformat': '%d %b', 'tickfont': {'size': render_obj['tick_font_size']}},
                    'yaxis': {'tickfont': {'size': render_obj['tick_font_size']}, 'range': range1},
                    'yaxis2': {
                        'overlaying': 'y',
                        'side': 'right',
                        'title': 'Index',
                        'ticksuffix': '%',
                        'tickfont': {'size': render_obj['tick_font_size']},
                        'range': range2
                    },
                    'legend': {'font': {'size': render_obj['tick_font_size']}, 'x': 1.05}
                }
    }

    netop_graph = [{
                       'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in net_op.index],
                       'y': (net_op[col] * 100).dropna().values.tolist(),
                       'name': ('Long Net' if col == 'L' else ('Short Net' if col == 'S' else col)) + ' O/P',
                       'line': {'width': g.lineWidth,
                                'color': "rgb(27, 93, 225)" if col == 'L' else ("rgb(214,39,40)" if col == 'S'
                                                                                else "rgb(0,0,0)")
                                }
                   } for col in net_op.columns
                   ]

    beta_graph = [{
                      'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in alpha.index],
                      'y': (alpha[col] * 100).dropna().values.tolist(),
                      'name': ('Long Beta' if col == 'L' else ('Short Beta' if col == 'S' else col)) + ' O/P',
                      'line': {'width': g.lineWidth,
                               'color': "rgb(27, 93, 225)" if col == 'L' else ("rgb(214,39,40)" if col == 'S'
                                                                               else "rgb(0,0,0)")
                               }
                  } for col in alpha.columns
                  ]

    exposure_graph_df = all_fund_exposure_in_money[:, param_adviser].unstack().reindex(all_fund_exposure_in_money.index.levels[0]).dropna()
    exposure_graph_range = [0, exposure_graph_df.stack().max()]
    exposure_graph = {'data': [{
                          'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in exposure_graph_df.index],
                          'y': exposure_graph_df[col].dropna().values.tolist(),
                          'name': 'Long Exposure' if col == 'L' else ('Short Exposure' if col == 'S' else col),
                          'line': {'width': g.lineWidth,
                                   'color': "rgb(27, 93, 225)" if col == 'L' else ("rgb(214,39,40)" if col == 'S'
                                                                                   else "rgb(0,0,0)")
                                   }
                      } for col in exposure_graph_df.columns.tolist()
                      ],
                      'layout': {
                            'margin': {'t': render_obj['margin_top'], 'b': render_obj['margin_bottom'],
                                       'l': render_obj['margin_left'], 'r': render_obj['margin_right']},
                            'width': render_obj['graph_width'],
                            'height': render_obj['graph_height'],
                            'xaxis': {'tickformat': '%d %b', 'tickfont': {'size': render_obj['tick_font_size']}},
                            'yaxis': {'tickfont': {'size': render_obj['tick_font_size']},
                                      'range': exposure_graph_range},
                            'legend': {'font': {'size': render_obj['tick_font_size']}}
                        }
                      }

    month_end = datetime(net_op.index[-1].year, net_op.index[-1].month, net_op.index[-1].daysinmonth)
    bm_index = pd.date_range(start=start_date, end=month_end, freq='BM')

    long_index_op = net_op['L'].iloc[-1] * 100
    short_index_op = net_op['S'].iloc[-1] * 100
    long_alpha = alpha['L'].iloc[-1] * 100
    short_alpha = alpha['S'].iloc[-1] * 100

    bm_net_op = net_op
    bm_alpha = alpha
    if net_op.index[-1] < bm_index[-1]:
        bm_net_op.ix[bm_index[-1]] = np.nan
        bm_alpha.ix[bm_index[-1]] = np.nan

    bm_net_op = net_op.fillna(method='ffill').reindex(bm_index)
    bm_alpha = alpha.fillna(method='ffill').reindex(bm_index)
    bm_net_op = bm_net_op - bm_net_op.shift(1).fillna(0)
    bm_alpha = bm_alpha - bm_alpha.shift(1).fillna(0)
    graph_op = DataFrame()
    graph_op['Long OP'] = bm_net_op['L'].fillna(0)
    graph_op['Long Beta OP'] = bm_alpha['L'].fillna(0)
    graph_op['Short OP'] = bm_net_op['S'].fillna(0)
    graph_op['Short Beta OP'] = bm_alpha['S'].fillna(0)
    graph_op = graph_op.truncate(before=datetime.strptime(start_date, '%Y-%m-%d')+timedelta(1))

    op_graph = dict()
    op_graph['index'] = [x.strftime('%b') for x in graph_op.index]
    op_graph['columns'] = {col: (graph_op[col] * 100).values.tolist() for col in graph_op.columns}

    gross_exposure_graph = [{
                                'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in
                                      gross_exposure[:, col].index],
                                'y': (gross_exposure[:, col] * 100).dropna().values.tolist(),
                                'name': col,
                                'line': {
                                    'color': "rgb(214, 39, 40)" if (col == param_adviser) else "rgb(190, 190, 190)",
                                    'width': g.lineWidth if (col == param_adviser) else g.thinLineWidth
                                }
                            } for col in gross_exposure.index.levels[1] if col not in g.dropList]

    short_exposure_range = [0, short_exposure.loc[(slice(None), g.indexMapping.keys())].max()*100]
    short_exposure_graph = {'data': [{
                                'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in
                                      short_exposure[:, col].index],
                                'y': (short_exposure[:, col] * 100).values.tolist(),
                                'name': col,
                                'line': {
                                    'color': "rgb(214, 39, 40)" if (col == param_adviser) else "rgb(190, 190, 190)",
                                    'width': g.lineWidth if (col == param_adviser) else g.thinLineWidth

                                }
                            } for col in short_exposure.index.levels[1] if not col in g.dropList],
                            'layout': {
                                'margin': {'t': render_obj['margin_top'], 'b': render_obj['margin_bottom'],
                                           'l': render_obj['margin_left'], 'r': render_obj['margin_right']+20},
                                'width': render_obj['graph_width'],
                                'height': render_obj['graph_height'],
                                'title': 'Short Exposure (As a Percent of all non-index short exposure)',
                                'xaxis': {'tickformat': '%d %b', 'tickfont': {'size': render_obj['tick_font_size']}},
                                'yaxis': {
                                    'ticksuffix': '%',
                                    'tickfont': {'size': render_obj['tick_font_size']},
                                    'range': short_exposure_range
                                },
                                'legend': {'font': {'size': render_obj['tick_font_size']}},
                                'showlegend': False
                            }
                        }

    names_graph = [{
                       'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in names_df[:, col].index],
                       'y': (names_df[:, col]).values.tolist(),
                       'name': col,
                       'line': {
                           'color': "rgb(214, 39, 40)" if (col == param_adviser) else "rgb(190, 190, 190)",
                           'width': g.lineWidth if (col == param_adviser) else g.thinLineWidth

                       }
                   } for col in names_df.index.levels[1] if col not in g.dropList]

    # attribution for each fund: number is correct
    fund_scale = (sql_pl_df.groupby(['advisor', 'MktCap'])
                  .sum()[['RHAttr', 'YAAttr', 'LRAttr']]
                  .loc[param_adviser]
                  )

    # pl for each side
    scale_pl = (sql_pl_df.groupby(['advisor', 'MktCap', 'side'])
                .sum()[['attribution']]
                .loc[param_adviser]
                .unstack()['attribution']
                .fillna(0))

    # try to assign cap to each trade turnover as Micro,.., Large
    scale_table = turnover_df.truncate(after=end_date).reset_index()
    scale_table.groupby(['advisor', 'MktCap']).sum()

    # TODO: truncate before?

    # TODO: analyst=DH&startDate=2014-12-31&endDate=2015-10-01, has incorrect value? for Long PL

    # TODO: analyst=TT&startDate=2014-12-31&endDate=2015-11-01  turnover and gics

    # TODO: ranking table not fit page well

    size_turnover = (scale_table.groupby(["advisor", "MktCap"])
                         .sum()[['JPYPL']]
                         .loc[param_adviser])

    size_turnover = size_turnover.merge(fund_scale, left_index=True, right_index=True, how='outer').fillna(0).merge(
        scale_pl, left_index=True, right_index=True, how='outer')
    total_turnover = size_turnover['JPYPL'].sum()
    size_turnover['TO'] = (size_turnover['JPYPL'] / total_turnover).replace([np.nan, np.inf, -np.inf], 0)

    size_turnover = size_turnover.reindex(['Micro', 'Small', 'Mid', 'Large', 'Mega', 'Index'], fill_value=0)
    total_series = size_turnover.sum()
    total_series.name = 'Total'
    scale_total = pd.DataFrame(total_series).T
    scale_table = pd.concat([size_turnover, scale_total])
    scale_table['Return'] = (scale_table['L'] + (scale_table['S'] if 'S' in scale_table else 0)) / scale_table['JPYPL']
    scale_table['Return'].replace([np.nan,np.inf,-np.inf],0, inplace=True)
    scale_table = scale_table[['RHAttr', 'YAAttr', 'LRAttr'] + scale_pl.columns.tolist() + ['JPYPL', 'TO', 'Return']]

    scale_table = scale_table.rename(
        columns={'JPYPL': 'Turnover', 'RHAttr': 'RH', 'YAAttr': 'YA', 'LRAttr': 'LR', 'L': 'LongPL',
                 'S': 'ShortPL', 'TO': 'TO %'})

    gics_table = (turnover_df.truncate(after=end_date)
                      .groupby(["advisor", "GICS"])
                      .sum()[['JPYPL']]
                      .loc[param_adviser])

    fund_gics = (sql_pl_df.groupby(['advisor', 'GICS'])
                     .sum()[['RHAttr', 'YAAttr', 'LRAttr']]
                     .loc[param_adviser])

    gics_pl = (sql_pl_df.groupby(['advisor', 'GICS', 'side'])
               .sum()[['attribution']]
               .loc[param_adviser]
               .unstack()['attribution']
               .fillna(0))

    gics_table = (gics_table.merge(fund_gics, left_index=True, right_index=True, how='outer')
        .merge(gics_pl, left_index=True, right_index=True, how='outer')
        .fillna(0))

    total_turnover = gics_table['JPYPL'].sum()
    gics_table['TO'] = gics_table['JPYPL'] / total_turnover

    total_series = gics_table.sum()
    total_series.name = 'Total'
    gics_total = pd.DataFrame(total_series).T
    gics_table = pd.concat([gics_table, gics_total])
    gics_table['Return'] = ((gics_table['L'] + (gics_table['S'] if 'S' in gics_table else 0)) / gics_table['JPYPL']
                            ).replace(['', np.nan, np.inf, -np.inf], 0)
    gics_table = gics_table[['RHAttr', 'YAAttr', 'LRAttr'] + gics_pl.columns.tolist() + ['JPYPL', 'TO', 'Return']]

    gics_table = gics_table.rename(
            columns={'JPYPL': 'Turnover', 'RHAttr': 'RH', 'YAAttr': 'YA', 'LRAttr': 'LR',
                     'L': 'LongPL',
                     'S': 'ShortPL', 'TO': 'TO %'})

    # some code has inconsistent format like xxxx Hk instead of HK
    code_beta_df['code'] = code_beta_df[['code']].applymap(str.upper)['code']

    t = sql_pl_df.merge(code_beta_df, left_on='quick', right_on='code', how='left')

    sector_table = (turnover_df.truncate(after=end_date)
                        .groupby(["advisor", "sector"])
                        .sum()[['JPYPL']]
                        .loc[param_adviser])

    fund_sector = (t.groupby(['advisor', 'sector'])
                       .sum()[['RHAttr', 'YAAttr', 'LRAttr']]
                       .loc[param_adviser])

    sector_pl = (t.groupby(['advisor', 'sector', 'side'])
                 .sum()[['attribution']]
                 .loc[param_adviser]
                 .unstack()['attribution']
                 .fillna(0))

    sector_table = (sector_table.merge(fund_sector, left_index=True, right_index=True, how='outer')
                    .fillna(0)
                    .merge(sector_pl, left_index=True, right_index=True, how='outer')
                    .fillna(0))

    sector_total_turnover = sector_table['JPYPL'].sum()

    sector_table['TO'] = sector_table['JPYPL'] / sector_total_turnover

    sector_series = sector_table.sum()
    sector_series.name = 'Total'
    if 'TailSens' in sector_table.index and 'Tail' in sector_table.index:
        sector_table.loc['Tail'] = sector_table.loc['Tail'] + sector_table.loc['TailSens']
        sector_table.drop('TailSens', inplace=True)
    if 'Tail' in sector_table.index and 'TailRR' in sector_table.index:
        sector_table.loc['Tail'] = sector_table.loc['Tail'] + sector_table.loc['TailRR']
        sector_table.drop('TailRR', inplace=True)

    sector_total = pd.DataFrame(sector_series).T
    sector_table = pd.concat([sector_table, sector_total])
    sector_table['Return'] = (
        (sector_table['L'] + (sector_table['S'] if 'S' in sector_table else 0)) / sector_table['JPYPL']).replace(
        [np.inf, -np.inf], 0)
    sector_table = sector_table[['RHAttr', 'YAAttr', 'LRAttr'] + sector_pl.columns.tolist() + ['JPYPL', 'TO', 'Return']]
    sector_table = sector_table.rename(
            columns={'JPYPL': 'Turnover', 'RHAttr': 'RH', 'YAAttr': 'YA', 'LRAttr': 'LR',
                     'L': 'LongPL',
                     'S': 'ShortPL', 'TO': 'TO %'})

    top_positions = (sql_pl_df[['quick', 'advisor', 'attribution', 'name',
                                'side', 'processDate', 'firstTradeDateLong',
                               'firstTradeDateShort']]
                     .groupby(['advisor', 'quick', 'name', 'side', first_trade_date])
                     .sum()
                     .sort_values(by='attribution', ascending=False)
                     .loc[param_adviser]
                     .head(NUMBER_OF_TOP_POSITIONS)
                     .reset_index()
                     .drop('quick', axis=1))

    top_positions.index += 1

    top_positions = top_positions.rename(columns={'name': 'Name', 'side': 'Side',
                                                  'attribution': 'Attribution', 'level_3': 'First Trade Date'})

    top_positions = top_positions[['Name', 'Side', 'Attribution', 'First Trade Date']]

    bottom_positions = (sql_pl_df[['quick', 'advisor', 'attribution', 'name', 'side',
                                   'firstTradeDateLong', 'firstTradeDateShort']]
                        .groupby(['advisor', 'quick', 'name', 'side', first_trade_date])
                        .sum()
                        .sort_values(by='attribution')
                        .loc[param_adviser]
                        .head(NUMBER_OF_TOP_POSITIONS)
                        .reset_index()
                        .drop('quick', axis=1))

    bottom_positions.index += 1

    bottom_positions = bottom_positions.rename(
        columns={'name': 'Name', 'side': 'Side', 'attribution': 'Attribution', 'level_3': 'First Trade Date'})

    bottom_positions = bottom_positions[['Name', 'Side', 'Attribution', 'First Trade Date']]

    topix_table = (turnover_df.truncate(after=end_date)
                   .groupby(["advisor", "TOPIX"])
                   .sum()[['JPYPL']]
                   .loc[param_adviser])

    fund_topix = (sql_pl_df.groupby(['advisor', 'TPX'])
                  .sum()[['RHAttr', 'YAAttr', 'LRAttr']]
                  .loc[param_adviser])

    fund_topix = fund_topix.rename(index={'Warehousing  and  Harbor Transpo': 'Warehousing  and  Harbor Transport'})

    topix_pl = (sql_pl_df.groupby(['advisor', 'TPX', 'side'])
                .sum()[['attribution']]
                .loc[param_adviser]
                .unstack()['attribution']
                .reset_index()
                .set_index('TPX'))

    topix_pl = topix_pl.rename(index={'Warehousing  and  Harbor Transpo': 'Warehousing  and  Harbor Transport'})

    topix_table = (topix_table.merge(fund_topix, left_index=True, right_index=True, how='outer')
                   .fillna(0)
                   .merge(topix_pl.fillna(0), left_index=True, right_index=True, how='outer'))

    total_turnover = topix_table['JPYPL'].sum()
    topix_table['TO'] = topix_table['JPYPL'] / total_turnover

    topix_series = topix_table.sum()
    topix_series.name = 'Total'
    topix_total = pd.DataFrame(topix_series).T
    topix_table = pd.concat([topix_table, topix_total])
    topix_table['Return'] = (
        (topix_table['L'] + (topix_table['S'].fillna(0) if 'S' in topix_table else 0)) / topix_table['JPYPL']).replace(
        [np.nan, np.inf, -np.inf], 0)
    topix_table = topix_table[['RHAttr', 'YAAttr', 'LRAttr'] + topix_pl.columns.tolist() + ['JPYPL', 'TO', 'Return']]
    topix_table = topix_table.rename(
            columns={'JPYPL': 'Turnover', 'RHAttr': 'RH',
                     'YAAttr': 'YA', 'LRAttr': 'LR',
                     'L': 'LongPL', 'S': 'ShortPL', 'TO': 'TO %'})

    strategy_table = (turnover_df.truncate(after=end_date)
                      .groupby(["advisor", "strategy"])
                      .sum()[['JPYPL']]
                      .loc[param_adviser])

    fund_strategy = (sql_pl_df.groupby(['advisor', 'strategy'])
                     .sum()[['RHAttr', 'YAAttr', 'LRAttr']]
                     .loc[param_adviser])

    fund_strategy = fund_strategy.fillna(0)

    strategy_pl = (sql_pl_df.groupby(['advisor', 'strategy', 'side'])
                   .sum()[['attribution']]
                   .loc[param_adviser]
                   .unstack()['attribution']
                   .reset_index()
                   .set_index('strategy')
                   .fillna(0))

    strategy_table = (strategy_table.merge(fund_strategy, left_index=True, right_index=True, how='outer')
                      .fillna(0)
                      .merge(strategy_pl, left_index=True, right_index=True, how='left')
                      .fillna(0))

    total_strategy_turnover = strategy_table['JPYPL'].sum()

    strategy_table['TO'] = strategy_table['JPYPL'] / total_strategy_turnover

    strategy_series = strategy_table.sum()
    strategy_series.name = 'Total'
    strategy_total = pd.DataFrame(strategy_series).T
    strategy_table = pd.concat([strategy_table, strategy_total])
    strategy_table['Return'] = (
        (strategy_table['L'] + (strategy_table['S'].fillna(0) if 'S' in strategy_table else 0)) / strategy_table[
            'JPYPL']).replace(
        [np.inf, -np.inf], 0)
    strategy_table = strategy_table[
        ['RHAttr', 'YAAttr', 'LRAttr'] + strategy_pl.columns.tolist() + ['JPYPL', 'TO', 'Return']]
    strategy_table = strategy_table.rename(
            columns={'JPYPL': 'Turnover', 'RHAttr': 'RH', 'YAAttr': 'YA', 'LRAttr': 'LR',
                     'L': 'LongPL',
                     'S': 'ShortPL', 'TO': 'TO %'})

    def group_f(arr):
        if arr.dtype == np.float64:
            return arr.sum()
        else:
            return arr[-1] # return newest name

    position_table = (turnover_df.truncate(after=end_date)
                      .groupby(["advisor", "code"])
                      .agg(group_f)[['name','JPYPL']]
                      .loc[param_adviser])

    position_pl = (sql_pl_df.groupby(['advisor', 'quick', 'name'])
                   .sum()[['RHAttr', 'YAAttr', 'LRAttr']]
                   .loc[param_adviser]
                   .reset_index()
                   .set_index('quick'))

    tt = (sql_pl_df.groupby(['advisor', 'quick', 'side'])
          .sum()[['attribution', 'fee']]
          .loc[(slice(param_adviser, param_adviser), slice(None)), :]
          .unstack()
          )

    tt.columns = [' '.join(col) for col in tt.columns.values]
    side_pl = tt.reset_index().drop(['advisor', 'fee L'], 1).set_index('quick').fillna(0)

    position_table = (position_table.merge(position_pl, left_index=True, right_index=True, how='outer')
                      .merge(side_pl, left_index=True, right_index=True, how='left')
                      .fillna(0))

    total_position_turnover = position_table['JPYPL'].sum()
    position_table['TO'] = position_table['JPYPL'] / total_position_turnover
    position_table['name'] = np.where(position_table['name_x'] != 0, position_table['name_x'], position_table['name_y'])
    position_table = position_table.reset_index().set_index(['name']).sort_index()
    position_series = position_table.sum()
    position_series.name = 'Total'
    position_total = pd.DataFrame(position_series).T

    position_table = pd.concat([position_table, position_total])
    position_table['Return'] = ((position_table['attribution L'] +
                                 (position_table['attribution S'].fillna(0) if 'attribution S' in position_table else 0)
                                 ) / position_table['JPYPL'].replace(0, np.nan)
                                ).replace([np.nan, np.inf, -np.inf], 0)

    position_table = position_table[['RHAttr', 'YAAttr', 'LRAttr', 'attribution L'] +
                                    (['attribution S'] if 'attribution_S' in position_table else []) +
                                    (['fee S'] if 'fee S' in position_table else []) +
                                    ['JPYPL', 'TO', 'Return']
                                    ]
    position_table = position_table.rename(
        columns={'JPYPL': 'Turnover', 'RHAttr': 'RH', 'YAAttr': 'YA', 'LRAttr': 'LR',
                 'attribution L': 'LongPL',
                 'attribution S': 'ShortPL', 'fee S': 'BorrowFee', 'TO': 'TO %'})

    tables_html = ''
    remaining_row_number = 28
    for df in [scale_table, gics_table, sector_table]:
        tables_html, remaining_row_number = add_to_page(df, tables_html, remaining_row_number)

    tables_html, remaining_row_number = add_to_page(top_positions, tables_html, remaining_row_number, 'ranking',
                                                    'Top %s Trades' % NUMBER_OF_TOP_POSITIONS)
    tables_html, remaining_row_number = add_to_page(bottom_positions, tables_html, remaining_row_number, 'ranking',
                                                    'Bottom %s Trades' % NUMBER_OF_TOP_POSITIONS)

    remaining_row_number -= 2 + 1  # 2 titles of ranking tables
    table_list = [topix_table, strategy_table, position_table] if g.indexMapping[param_adviser] == 'TPX' else [
        strategy_table, position_table]
    for df in table_list[:-1]:
        tables_html, remaining_row_number = add_to_page(df, tables_html, remaining_row_number)

    for df in table_list[-1:]:
        tables_html, remaining_row_number = add_to_page(df, tables_html, remaining_row_number, 'summary', '', True)

    render_obj['index'] = g.indexMapping[param_adviser]
    render_obj['startDate'] = start_date
    render_obj['endDate'] = end_date
    render_obj['longTurnover'] = Decimal(sum_turnover_per_adv.fillna(0).loc[param_adviser, 'L']
                                         ).quantize(Decimal('1.'), rounding=ROUND_HALF_UP)
    render_obj['shortTurnover'] = Decimal(sum_turnover_per_adv.fillna(0).loc[param_adviser, 'S']
                                          ).quantize(Decimal('1.'), rounding=ROUND_HALF_UP)

    render_obj['totalLong'] = total_ratio.loc[param_adviser, 'L']
    render_obj['totalShort'] = total_ratio.loc[param_adviser, 'S']
    render_obj['longPL'] = Decimal(cs_attr_df['L'].iloc[-1]).quantize(Decimal('1.'), rounding=ROUND_HALF_UP)
    render_obj['shortPL'] = Decimal(cs_attr_df['S'].iloc[-1]).quantize(Decimal('1.'), rounding=ROUND_HALF_UP)
    render_obj['longIndexOP'] = long_index_op
    render_obj['shortIndexOP'] = short_index_op
    render_obj['longBetaOP'] = long_alpha
    render_obj['shortBetaOP'] = short_alpha
    render_obj['longHitRate'] = hit_rate_df['LongsHR'].loc[param_adviser]
    render_obj['shortHitRate'] = (hit_rate_df['ShortsHR'].loc[param_adviser]
                                  if not (np.isnan(hit_rate_df['ShortsHR'].loc[param_adviser]))
                                  else 0
                                  )
    render_obj['longReturn'] = long_short_return.fillna(0)['attribution']['L'].loc[param_adviser]
    render_obj['shortReturn'] = long_short_return.fillna(0)['attribution']['S'].loc[param_adviser]
    render_obj['rhBpsLong'] = total_fund[total_fund['advisor'] == param_adviser].sum()['RHAttr']['L'] * 100
    if total_fund[total_fund['advisor'] == param_adviser].sum()['RHAttr']['S'] is None:
        render_obj['rhBpsShort'] = 0
    else:
        render_obj['rhBpsShort'] = total_fund[total_fund['advisor'] == param_adviser].sum()['RHAttr']['S'] * 100
    render_obj['yaBpsLong'] = total_fund[total_fund['advisor'] == param_adviser].sum()['YAAttr']['L'] * 100
    if total_fund[total_fund['advisor'] == param_adviser].sum()['YAAttr']['S'] is None:
        render_obj['yaBpsShort'] = 0
    else:
        render_obj['yaBpsShort'] = total_fund[total_fund['advisor'] == param_adviser].sum()['YAAttr']['S'] * 100
    render_obj['lrBpsLong'] = total_fund[total_fund['advisor'] == param_adviser].sum()['LRAttr']['L'] * 100
    if total_fund[total_fund['advisor'] == param_adviser].sum()['LRAttr']['S'] is None:
        render_obj['lrBpsShort'] = 0
    else:
        render_obj['lrBpsShort'] = total_fund[total_fund['advisor'] == param_adviser].sum()['LRAttr']['S'] * 100
    render_obj['exposure_avg_long'] = (exposure_avg[
                                           (exposure_avg['advisor'] == param_adviser) &
                                           (exposure_avg['side'] == 'L')
                                       ].mean() * 100 / aum_df['Total'].mean()
                                       ).iloc[0]

    if np.isnan((exposure_avg[
                         (exposure_avg['advisor'] == param_adviser) &
                         (exposure_avg['side'] == 'S')
                     ].mean() * 100 / aum_df['Total'].mean()
                    ).iloc[0]):
        render_obj['exposure_avg_short'] = 0
    else:
        render_obj['exposure_avg_short'] = (exposure_avg[
                                             (exposure_avg['advisor'] == param_adviser) &
                                             (exposure_avg['side'] == 'S')
                                         ].mean() * 100 / aum_df['Total'].mean()
                                        ).iloc[0]
    render_obj['rank_long'] = rank_long_df.loc[param_adviser, 0]
    if param_adviser in ranke_short_df:
        render_obj['rank_short'] = ranke_short_df.loc[param_adviser, 0]
    else:
        render_obj['rank_short'] = len(ranke_short_df.index) + 1
    render_obj['pl_graph'] = pl_graph
    render_obj['netop_graph'] = netop_graph
    render_obj['betaop_graph'] = beta_graph
    render_obj['exposure_graph'] = exposure_graph
    render_obj['op_graph'] = op_graph
    render_obj['gross_exposure_graph'] = gross_exposure_graph
    render_obj['short_exposure_graph'] = short_exposure_graph
    render_obj['names_graph'] = names_graph
    render_obj['tables_html'] = tables_html
    render_obj['analyst_list'] = g.indexMapping.keys()

    return render_template('main/attrib.html', params=render_obj)


@main.route('/summary', methods=['GET'])
def summary():
    start_date = request.args.get('startDate', g.startDate)
    end_date = request.args.get('endDate', g.endDate)
    calendar = jpd.JapaneseHolidayCalendar()
    cday = pd.offsets.CDay(calendar=calendar)
    indexer = pd.date_range(start_date, end_date, freq=cday)
    if start_date not in indexer:
        return render_template('main/error_message.html', error_message="The start-date is not a working date!")

    code_name_map = get_code_name_map()
    turnover_df = get_turnover_df(start_date, end_date)
    turnover_df['JPYPL'] = turnover_df['Turnover']
    turnover_df.sort_index(inplace=True)
    total_turnover = turnover_df.truncate(after=end_date).groupby(["side"]).sum()['JPYPL']

    # calculate turnover for each advisor
    sum_turnover_per_adv = turnover_df.truncate(after=end_date).groupby(["advisor", "side"]).sum()['JPYPL'].unstack()

    t1 = turnover_df.truncate(after=end_date).groupby(["advisor", "side"]).sum()['JPYPL'].unstack()
    t2 = t1.loc[t1.index.str.contains('/')].copy()

    new_data = defaultdict(float)
    for ind in t2.index:
        for new_ind in ind.split('/'):
            if new_ind in new_data:
                new_data[new_ind] += t2.loc[ind]
            else:
                new_data[new_ind] = t2.loc[ind]
    if new_data:
        sum_shared_turnover = (pd.DataFrame.from_dict(new_data, orient='index')
                               .div(2)
                               .fillna(0)
                               .reindex(sum_turnover_per_adv.dropna().index, fill_value=0)
                               )
        sum_turnover_per_adv = sum_turnover_per_adv.dropna() + sum_shared_turnover

    sum_turnover_per_adv = sum_turnover_per_adv.reindex(g.indexMapping.keys())

    total_ratio = (sum_turnover_per_adv * 100 / total_turnover).fillna(0)  # % TOTAL

    aum_df = get_aum_df(start_date, end_date)

    code_beta_df = get_code_beta()

    f_exposure_df = get_exposure_df(start_date, end_date)

    aum_turnover = (turnover_df
                    .merge(aum_df, left_index=True, right_index=True, how='inner')
                    .assign(turnover_per_aum=lambda df: df['JPYPL'] / df['Total'])
                    .truncate(after=end_date)
                    .groupby(["advisor", "side"])
                    .sum()['turnover_per_aum']
                    .div(2).mul(100)
                    .unstack()
                    .assign(port_turnover=lambda df: df['L'].add(df['S']).div(2))
                    #  .dropna()
                    )

    t = aum_turnover.loc[aum_turnover.index.str.contains('/')].copy()

    new_data = defaultdict(float)
    for ind in t.index:
        for new_ind in ind.split('/'):
            if new_ind in new_data:
                new_data[new_ind] += t.loc[ind]
            else:
                new_data[new_ind] = t.loc[ind]

    if new_data:
        shared_turnover = (pd.DataFrame.from_dict(new_data, orient='index')
                           .div(2)
                           .fillna(0)
                           .reindex(aum_turnover.dropna().index, fill_value=0)
                           )

        aum_turnover = aum_turnover[~aum_turnover.index.str.contains('/')] + shared_turnover

    aum_turnover.fillna(0, inplace=True)

    sql_pl_df = get_pl_df(start_date, end_date)
    sql_pl_df = sql_pl_df.merge(code_name_map, left_on='quick', right_on='quick')

    new_df = get_borrow_fee_df(sql_pl_df)

    sql_pl_df = (sql_pl_df
                 .merge(new_df.reset_index(), left_on=['processDate', 'quick'],
                        right_on=['index', 'quick'], how='left')
                 .fillna(0)
                 )
    sql_pl_df['attribution'] -= sql_pl_df['fee']
    sql_pl_df['RHAttr'] -= sql_pl_df['rhfee'] / sql_pl_df['RHNAV']
    sql_pl_df['YAAttr'] -= sql_pl_df['yafee'] / sql_pl_df['YANAV']
    sql_pl_df['LRAttr'] -= sql_pl_df['lrfee'] / sql_pl_df['LRNAV']

    index_net_return, index_df = get_index_return(start_date, end_date)

    exposure_df = (
        f_exposure_df
            .merge(code_beta_df, how='left', left_on='quick', right_on='code')
            .groupby(['processDate', 'advisor', 'side']).sum()[
            ['RHExposure', 'YAExposure', 'LRExposure']]
            .assign(firm_exposure=lambda df: (df['RHExposure'].mul(aum_df['RHAUM'], axis=0)
                                              + df['YAExposure'].mul(aum_df['YAAUM'], axis=0)
                                              + df['LRExposure'].mul(aum_df['LRAUM'], axis=0)
                                              ) * df.index.get_level_values(2).map(lambda x: -1 if x == 'S' else 1)
                    )
        [['firm_exposure']]
            .unstack().unstack()
            .shift(1)
            .stack().stack()
            .reset_index()
            .assign(index_code=lambda df: df['advisor'].map(g.indexMapping))
    )

    beta_exposure_df = (f_exposure_df
                        .merge(code_beta_df, how='left', left_on='quick', right_on='code')
                        # .set_index(['processDate', 'advisor', 'side'])
                        .merge(aum_df, how='inner', left_on='processDate', right_index=True)
                        .assign(beta_exposure=lambda df: (df['RHExposure'] * df['RHAUM']
                                                          + df['YAExposure'] * df['YAAUM']
                                                          + df['LRExposure'] * df['LRAUM']
                                                          ) * df['beta'] * df['side'].map({'S': -1, 'L': 1})
                                )
                        .groupby(['processDate', 'advisor', 'side'])
                        .sum()[['beta_exposure']]
                        .unstack()
                        .unstack()
                        .shift(1)
                        .stack()
                        .stack()
                        .reset_index()
                        )

    attribution_df = pd.pivot_table(sql_pl_df, index=['processDate', 'advisor', 'side'], values=['attribution'],
                          aggfunc=np.sum, fill_value=0
                          )

    index_return_df = index_net_return.dropna().stack().reset_index()

    index_ret_jpy = (exposure_df.merge(index_return_df, how='inner',
                                       left_on=['processDate', 'index_code'],
                                       right_on=['priceDate', 'indexCode'])
                     .merge(attribution_df.reset_index(), how='inner',
                            left_on=['processDate', 'advisor', 'side'],
                            right_on=['processDate', 'advisor', 'side']
                            )
                     .merge(beta_exposure_df, how='inner',
                            left_on=['processDate', 'advisor', 'side'],
                            right_on=['processDate', 'advisor', 'side']
                            )
                     .merge(aum_df.shift(1)[['Total']], how='inner', left_on='processDate', right_index=True)
                     .assign(index_return=lambda df: df['firm_exposure'].mul(df[0]))
                     .assign(index_beta_return=lambda df: df['beta_exposure'].mul(df[0]))
                     .assign(net_op=lambda df: df['attribution'].sub(df['index_return']).div(df['Total']))
                     .assign(alpha=lambda df: df['attribution'].sub(df['index_beta_return']).div(df['Total']))
                     # [['processDate', 'advisor', 'side', 'net_op', 'alpha']]
                     )

    op_alpha = pd.pivot_table(index_ret_jpy, index=['advisor'], values=['net_op', 'alpha'],
                              columns=['side'],
                              aggfunc=[np.sum]
                              ).fillna(0) * 100

    op_alpha.columns = [' '.join(col[1:]) for col in op_alpha.columns.values]

    borrow_pct = (sql_pl_df
                  .assign(total_nav=lambda df: df['RHNAV'] + df['YANAV'] + df['LRNAV'])
                  .assign(fee_pct=lambda df: df['fee'] / df['total_nav'])
                  .groupby('advisor')[['fee_pct']].sum() * 100
                  )

    fund_pl = (pd.pivot_table(sql_pl_df, index=['advisor'],
                              values=['RHAttr', 'YAAttr', 'LRAttr'],
                              columns='side',
                              aggfunc=[np.sum],
                              margins=True, margins_name='PL'
                              )
               ).fillna(0) * 100
    fund_pl.columns = [' '.join(col[1:]) for col in fund_pl.columns.values]

    def format_2f(df):
        t = df.copy()
        t = t.applymap(lambda x: '{:,.2f}'.format(x))
        return t

    def format_fee_pct(df):
        t = df.copy()
        t['fee_pct'] = t['fee_pct'].apply(lambda x: '{:,.4f}'.format(x))
        return t

    final_ret = (total_ratio
                 .merge(aum_turnover, left_index=True, right_index=True, suffixes=('', '_Trnvr'))
                 .merge(op_alpha, left_index=True, right_index=True)
                 .merge(fund_pl, left_index=True, right_index=True)
                 .pipe(format_2f)
                 .merge(borrow_pct, left_index=True, right_index=True)
                 .pipe(format_fee_pct)
                 .rename(columns={'net_op L': 'OPerfVsTpx L', 'net_op S': 'OPerfVsTpx S',
                                  'port_turnover': 'Trnvr'}
                         )
                 [['L', 'S', 'L_Trnvr', 'S_Trnvr', 'Trnvr', 'OPerfVsTpx L', 'OPerfVsTpx S',
                   'alpha L', 'alpha S',
                   'RHAttr L', 'RHAttr S', 'RHAttr PL', 'YAAttr L', 'YAAttr S', 'YAAttr PL',
                   'LRAttr L', 'LRAttr S', 'LRAttr PL', 'fee_pct'
                   ]].loc[['AO', 'AP', 'CS', 'HA', 'RW', 'SJ', 'SM',
                           'TI', 'TT', 'Adv', 'Bal', 'AP-A', 'AQ', 'DL', 'EL'
                           ]
                 ]
                 )
    params = dict()
    params['start_date'] = start_date
    params['end_date'] = end_date

    return render_template('main/summary.html',
                           table=final_ret.reset_index().to_html(index=False),
                           copy_table=final_ret.reset_index().to_csv(index=False, line_terminator='<EOL>'),
                           params=params
                           )


@main.route('/test')
@login_required
def test():
    render_obj = dict()
    render_obj['analyst_list'] = g.indexMapping.keys()
    return render_template('main/test.html', params=render_obj)
