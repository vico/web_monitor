# all the imports
import sqlite3
from flask import Flask, request, session, g, redirect, url_for, \
    abort, render_template, flash, Response
from contextlib import closing


import pandas as pd
from pandas import DataFrame
from pandas.io import sql
from pandas.tseries.offsets import *
import numpy as np
from numpy.random import randn
import pymysql
from datetime import datetime, timedelta
from decimal import *

# configuration
DATABASE = '/tmp/flaskr.db'
DEBUG = True
SECRET_KEY = 'development key'
USERNAME = 'admin'
PASSWORD = 'default'

# create our little application :)
app = Flask(__name__)
app.config.from_object(__name__)


def connect_db():
    return sqlite3.connect(app.config['DATABASE'])


def init_db():
    with closing(connect_db()) as db:
        with app.open_resource('schema.sql', mode='r') as f:
            db.cursor().executescript(f.read())
        db.commit()


@app.before_request
def before_request():
    g.db = connect_db()
    g.con = pymysql.connect(host='127.0.0.1', user='root', passwd='root', db='hkg02p')
    g.fromDate = '2014-12-31'  # not include
    g.endDate = '2015-12-01'  # not include
    g.reportAdvisor = 'AP'
    g.lineWidth = 3
    g.thinLineWidth = 2
    g.indexMapping = {
        'AP': 'TPX',
        'CS': 'TPX',
        'SM': 'TPX',
        'HA': 'TPX',
        'RW': 'TPX',
        'SJ': 'TPX',
        'TI': 'TPX',
        'AQ': 'HSCEI',
        'DH': 'HSCEI',
        'EL': 'TWSE',
        'PK-A': 'KOSPI'
    }
    g.dropList = ['ADV', 'Adv', 'Bal', 'NJD', 'NJA', 'KW']


@app.teardown_request
def teardown_request(exception):
    db = getattr(g, 'db', None)
    if db is not None:
        db.close()
    con = getattr(g, 'con', None)
    if con is not None:
        con.close()


@app.route('/')
def show_entries():
    cur = g.db.execute('select title, text from entries order by id desc')
    entries = [dict(title=row[0], text=row[1]) for row in cur.fetchall()]
    return render_template('show_entries.html', entries=entries)


@app.route('/add', methods=['POST'])
def add_entry():
    if not session.get('logged_in'):
        abort(401)
    g.db.execute('insert into entries (title, text) values (?, ?)',
                 [request.form['title'], request.form['text']])
    g.db.commit()
    flash('New entry was succesfully posted')
    return redirect(url_for('show_entries'))


@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        if request.form['username'] != app.config['USERNAME']:
            error = 'Invalid username'
        elif request.form['password'] != app.config['PASSWORD']:
            error = 'Invalid password'
        else:
            session['logged_in'] = True
            flash('You were loggined in')
            return redirect(url_for('show_entries'))
            # return redirect(url_for('enable_tfa_via_app'))

    return render_template('login.html', error=error)


@app.route('/attrib')
def attrib():
    code_name_map = sql.read_sql('''SELECT quick, name FROM t01Instrument;''', g.con)
    hitRateDf = sql.read_sql('''select advisor, SUM(IF(RHAttr > 0 AND side='L',1,0)) as LongsWin,
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
    ;''' % (g.fromDate, g.endDate), g.con, coerce_float=True, index_col='advisor')

    sqlFxDf = sql.read_sql('''SELECT a.base, AVG(a.rate) AS AvgOfrate
                FROM (
                SELECT t06DailyCrossRate.priceDate, t06DailyCrossRate.base, t06DailyCrossRate.quote, t06DailyCrossRate.Rate
                FROM t06DailyCrossRate
                WHERE priceDate> '%s' AND QUOTE="JPY"
                ) a
                GROUP BY a.base;
                ''' % g.fromDate, g.con, coerce_float=True, index_col='base')

    avgRate = {'base': ['AUD', 'CNY', 'EUR', 'HKD', 'JPY', 'KRW', 'MYR', 'PHP', 'SGD', 'THB', 'TWD', 'USD'],
               'AvgOfrate': [91.49250673, 19.29678027, 134.6973991, 15.58508341, 1, 0.107409013, 31.56756502,
                             2.670135747, 88.29089686, 3.55963991, 3.826318386, 120.8260538]}

    sqlFxDf = DataFrame(avgRate, index=avgRate['base'])

    # there is some trades on 2014/12/31 both long and short sides, which is not in database table
    sqlTurnoverDf = sql.read_sql('''#A15_Turnover
        SELECT aa.tradeDate, aa.code,
        aa.currencyCode, aa.side,
        ABS(Notl) AS Turnover, e.advisor, e.strategy, e.sector,
        f.value AS GICS,
        IF(g.value IS NOT NULL, g.value, 'Non-Japan') AS TOPIX
        FROM (
        SELECT b.code,
        d.currencyCode,
        b.side,
        IF(orderType="B",1,-1)*quantity AS Qty,
        IF(orderType="B",-1,1)*net AS Notl,
        MAX(a.adviseDate) AS `MaxOfDate`,
        b.reconcileID,
        b.tradeDate,
        b.equityType,
        c.instrumentType,
        c.instrumentID,
        b.orderType
        FROM t08AdvisorTag a
        INNER JOIN t08Reconcile b ON a.code = b.code
        INNER JOIN t01Instrument c ON (b.equityType = c.instrumentType) AND (b.code = c.quick)
        INNER JOIN t02Currency d ON c.currencyID = d.currencyID
        WHERE a.adviseDate<= b.processDate
    AND b.processDate >= '%s' # Grab Analyst Trades start date
      AND b.equityType<>"OP"
        AND b.srcFlag="K"
        GROUP BY c.instrumentID, b.tradeDate, b.orderType, b.reconcileID, b.side, Qty, Notl, b.code
        ORDER BY b.code
        ) aa
        LEFT JOIN t08AdvisorTag e ON (aa.MaxOfDate = e.adviseDate) AND (aa.code = e.code)
        LEFT JOIN t06DailyBBStaticSnapshot f ON aa.instrumentID = f.instrumentID AND f.dataType = 'GICS_SECTOR_NAME'
        LEFT JOIN t06DailyBBStaticSnapshot g ON aa.instrumentID = g.instrumentID AND g.dataType = 'JAPANESE_INDUSTRY_GROUP_NAME_ENG'
        WHERE (aa.side="L" AND aa.orderType="B") OR (aa.side="S" AND aa.orderType="S")
        ORDER BY aa.tradeDate
        ;
         ''' % g.fromDate, g.con, parse_dates=['tradeDate'], coerce_float=True, index_col='tradeDate')

    df20141231 = pd.read_csv('turnover20141231.csv', index_col=0, parse_dates=0)

    # concat with data in Access DB
    turnover_df = pd.concat([df20141231, sqlTurnoverDf])

    # merge with FX df to get to-JPY-fx rate
    turnover_merged_df = turnover_df.merge(sqlFxDf, left_on='currencyCode', right_index=True).sort_index()
    # create new column which contain turnover in JPY
    turnover_merged_df['JPYPL'] = (turnover_merged_df['Turnover'] * turnover_merged_df['AvgOfrate']).values

    # calculate total turnover for each side
    total_turnover = turnover_merged_df.truncate(after=g.endDate).groupby(["side"]).sum()['JPYPL']

    # calculate turnover for each advisor
    sumTurnoverPerAdv = turnover_merged_df.truncate(after=g.endDate).groupby(["advisor", "side"]).sum()['JPYPL'].unstack()

    totalRatio = sumTurnoverPerAdv * 100 / total_turnover['L']  # % TOTAL

    aumDf = sql.read_sql('''SELECT processDate, MAX(RHAUM) AS RHAUM, MAX(YAAUM) AS YAAUM, MAX(LRAUM) AS LRAUM
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
        ;''' % (g.fromDate, g.endDate), g.con, coerce_float=True, parse_dates=['processDate'], index_col='processDate')

    aumDf['Total'] = aumDf['RHAUM'] + aumDf['YAAUM'] + aumDf['LRAUM']

    codeBetaDf = sql.read_sql('''SELECT a.code, a.beta, a.sector
  FROM t08AdvisorTag a,
    (SELECT advisorTagID, code, MAX(t08AdvisorTag.adviseDate) AS MaxOfadviseDate
    FROM t08AdvisorTag
    GROUP BY t08AdvisorTag.code) b
    WHERE #a.advisorTagID = b.advisorTagID
    a.code = b.code
    AND b.MaxOfadviseDate = a.adviseDate
    ;''', g.con, coerce_float=True)

    fExposureDf = sql.read_sql('''SELECT processDate, advisor, quick,
         side, RHExposure, YAExposure, LRExposure
         FROM `t05PortfolioResponsibilities`
         WHERE processDate >= '%s' AND processDate < '%s'
         AND advisor <> ''
         ;''' % (g.fromDate, g.endDate), g.con, coerce_float=True, parse_dates=['processDate'])
    names_df = fExposureDf.groupby(by=['processDate', 'advisor']).count()['quick']

    mfExposureDf = fExposureDf.merge(codeBetaDf, how='left', left_on='quick', right_on='code')
    sumExposureDf = mfExposureDf.groupby(['processDate', 'advisor', 'side']).sum()[
        ['RHExposure', 'YAExposure', 'LRExposure']]

    temp2 = mfExposureDf.set_index(['processDate', 'advisor', 'side'])

    t2 = (temp2['RHExposure'].mul(aumDf['RHAUM'], axis=0).mul(temp2['beta'].fillna(0), axis=0) +
          temp2['YAExposure'].mul(aumDf['YAAUM'], axis=0).mul(temp2['beta'].fillna(0), axis=0) +
          temp2['LRExposure'].mul(aumDf['LRAUM'], axis=0).mul(temp2['beta'].fillna(0), axis=0))

    t3 = t2.reset_index()  # .drop('quick',1)
    t4 = t3.groupby(['processDate', 'advisor', 'side']).sum()
    t4.columns = ['exposure']

    betaExposure = t4['exposure']
    tExposureDf = (sumExposureDf['RHExposure'].mul(aumDf['RHAUM'], axis=0) +
                   sumExposureDf['YAExposure'].mul(aumDf['YAAUM'], axis=0) +
                   sumExposureDf['LRExposure'].mul(aumDf['LRAUM'], axis=0))

    tExposureDf.columns = ['Exposure']
    sqlPlDf = sql.read_sql('''SELECT processDate,advisor, side, quick, attribution,
                          RHAttribution AS RHAttr,
                          YAAttribution AS YAAttr,
                          LRAttribution AS LRAttr, GICS, TPX,strategy
                          FROM `t05PortfolioResponsibilities`
                          WHERE processDate >= '%s' AND processDate < '%s'
                          AND quick NOT LIKE "*DIV"
                            AND quick NOT LIKE "FX*"
                          AND advisor <> ''
                          ;''' % (g.fromDate, g.endDate), g.con, coerce_float=True,
                           parse_dates=['processDate'])  # ,index_col = 'processDate')
    sqlPlDf = sqlPlDf.merge(code_name_map, left_on='quick', right_on='quick')

    t = sqlPlDf.groupby(['processDate', 'advisor', 'side']).sum().drop(['RHAttr', 'YAAttr', 'LRAttr'],
                                                                       axis=1).unstack().reset_index().set_index(
            'processDate')
    attr_df = t[t['advisor'] == g.reportAdvisor]['attribution']
    attr_df['Total'] = attr_df['L'] + attr_df['S']
    cs_attr_df = attr_df
    cs_attr_df.ix[g.fromDate] = 0
    cs_attr_df = cs_attr_df.cumsum()

    long_short_return = sqlPlDf.groupby(["advisor", "side"]).sum().drop(['RHAttr', 'YAAttr', 'LRAttr'],
                                                                        axis=1).unstack().div(sumTurnoverPerAdv,
                                                                                              axis=0) * 100

    index_df = sql.read_sql('''SELECT b.priceDate, a.indexCode, b.close
  FROM `t07Index` a, `t06DailyIndex` b
  WHERE a.indexID = b.indexID
  AND b.priceDate >= '%s' AND b.priceDate < '%s'
  AND a.indexCode IN ('TPX','KOSPI','TWSE','HSCEI')
  ;''' % (g.fromDate, g.endDate), g.con, coerce_float=True, parse_dates=['priceDate'])
    pIndexDf = index_df.pivot('priceDate', 'indexCode', 'close')
    indexReturn = pIndexDf / pIndexDf.shift(1) - 1
    csIndexReturn = pIndexDf / pIndexDf.ix[1] - 1

    tExposure = tExposureDf[:, g.reportAdvisor].unstack().shift(1)

    exposure_avg = DataFrame(tExposureDf).reset_index()

    t = DataFrame(tExposureDf).reset_index()
    gross_exposure = t.groupby(by=['processDate', 'advisor'])[0].sum().div(aumDf['Total'], axis=0)
    t2 = t[t['side'] == 'S'].set_index(['processDate', 'advisor'])[0].div(aumDf['Total'], axis=0)
    t3 = DataFrame(
            t[t['side'] == 'S'].set_index(['processDate', 'advisor'])[0].div(aumDf['Total'], axis=0)).reset_index()
    t3[t3['advisor'] == 'Bal'] = 0
    t4 = t3.groupby(by='processDate')[0].sum().truncate(before=g.fromDate)
    short_exposure = t2.div(t4, axis=0)

    rankLongDf = exposure_avg[(exposure_avg['side'] == 'L')].groupby(by='advisor').mean() * 100 / aumDf['Total'].mean()
    rankShortDf = exposure_avg[(exposure_avg['side'] == 'S')].groupby(by='advisor').mean() * 100 / aumDf['Total'].mean()
    rankLongDf = rankLongDf.drop(g.dropList, errors='ignore').rank(ascending=False)
    rankShortDf = rankShortDf.drop(g.dropList, errors='ignore').rank(ascending=False)

    net_op = DataFrame()
    net_op['L'] = attr_df['L'].sub(tExposure['L'].mul(indexReturn[g.indexMapping[g.reportAdvisor]], axis=0),
                                   axis=0).div(
            aumDf.shift(1)['Total'], axis=0)
    net_op['S'] = attr_df['S'].sub((tExposure['S'] * -1).mul(indexReturn[g.indexMapping[g.reportAdvisor]], axis=0),
                                   axis=0).div(aumDf.shift(1)['Total'], axis=0)
    net_op.ix[g.fromDate] = 0
    net_op = net_op.cumsum()
    net_op['Total'] = net_op['L'] + net_op['S']

    btExposure = betaExposure[:, g.reportAdvisor].unstack().shift(1)
    beta_op = DataFrame()
    beta_op['L'] = attr_df['L'].sub(btExposure['L'].mul(indexReturn[g.indexMapping[g.reportAdvisor]], axis=0),
                                    axis=0).div(aumDf.shift(1)['Total'], axis=0)
    beta_op['S'] = attr_df['S'].sub((btExposure['S'] * -1).mul(indexReturn[g.indexMapping[g.reportAdvisor]], axis=0),
                                    axis=0).div(aumDf.shift(1)['Total'], axis=0)
    beta_op = beta_op.cumsum()
    beta_op['Total'] = beta_op['L'] + beta_op['S']

    totalFund = sqlPlDf.groupby(['processDate', 'advisor', 'side']).sum().drop(['attribution'],
                                                                               axis=1).unstack().reset_index().set_index(
            'processDate')

    pl_graph = dict()
    pl_graph['index'] = [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in cs_attr_df.index]
    pl_graph['columns'] = {col: cs_attr_df[col].values.tolist() for col in cs_attr_df.columns}
    pl_graph['market'] = ((tExposure['L'] + tExposure['S']) * indexReturn[g.indexMapping[g.reportAdvisor]]).sub(
            pIndexDf[g.indexMapping[g.reportAdvisor]], axis=0).fillna(0).cumsum().values.tolist()

    netop_graph = dict()
    netop_graph['index'] = [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in net_op.index]
    netop_graph['columns'] = {col: (net_op[col]*100).values.tolist() for col in net_op.columns}

    beta_graph = dict()
    beta_graph['index'] = [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in beta_op.index]
    beta_graph['columns'] = {col: (beta_op[col]*100).fillna(0).values.tolist() for col in beta_op.columns}

    exposure_graph = dict()
    exposure_graph['index'] = [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in
                               tExposureDf[:, g.reportAdvisor, 'L'].index]
    exposure_graph['columns'] = {col: tExposureDf[:, g.reportAdvisor, col].fillna(0).values.tolist() for col in
                                 ['L', 'S']}

    bm_index = pd.date_range(start=g.fromDate, end=g.endDate, freq='BM')
    bm_net_op = net_op.reindex(bm_index)
    bm_beta_op = beta_op.reindex(bm_index)
    bm_net_op = bm_net_op - bm_net_op.shift(1)
    bm_beta_op = bm_beta_op - bm_beta_op.shift(1)
    graph_op = DataFrame()
    graph_op['Long OP'] = bm_net_op['L'].fillna(0)*100
    graph_op['Long Beta OP'] = bm_beta_op['L'].fillna(0)*100
    graph_op['Short OP'] = bm_net_op['S'].fillna(0)*100
    graph_op['Short Beta OP'] = bm_beta_op['S'].fillna(0)*100

    op_graph = dict()
    op_graph['index'] = [x.strftime('%Y-%m') for x in graph_op.index]
    op_graph['columns'] = {col: (graph_op[col] * 100).values.tolist() for col in graph_op.columns}

    gross_exposure_graph = [{
                                'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in
                                      gross_exposure[:, col].index],
                                'y': (gross_exposure[:, col] * 100).values.tolist(),
                                'name': col,
                                'line': {
                                    'color': "rgb(214, 39, 40)" if (col == g.reportAdvisor) else "rgb(190, 190, 190)",
                                    'width': g.lineWidth if (col == g.reportAdvisor) else g.thinLineWidth
                                }
                            } for col in gross_exposure.index.levels[1] if not col in g.dropList]

    short_exposure_graph = [{
                                'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in
                                      short_exposure[:, col].index],
                                'y': (short_exposure[:, col] * 100).values.tolist(),
                                'name': col,
                                'line': {
                                    'color': "rgb(214, 39, 40)" if (col == g.reportAdvisor) else "rgb(190, 190, 190)",
                                    'width': g.lineWidth if (col == g.reportAdvisor) else g.thinLineWidth

                                }
                            } for col in short_exposure.index.levels[1] if not col in g.dropList]

    names_graph = [{
                       'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in names_df[:, col].index],
                       'y': (names_df[:, col]).values.tolist(),
                       'name': col,
                       'line': {
                           'color': "rgb(214, 39, 40)" if (col == g.reportAdvisor) else "rgb(190, 190, 190)",
                           'width': g.lineWidth if (col == g.reportAdvisor) else g.thinLineWidth

                       }
                   } for col in names_df.index.levels[1] if not col in g.dropList]

    gicsTable = turnover_merged_df.truncate(after=g.endDate).groupby(["advisor", "GICS"]).sum()[['JPYPL']].loc[
                (slice(g.reportAdvisor, g.reportAdvisor), slice(None)), :].reset_index().drop('advisor', 1).set_index(
            'GICS')
    fundGics = sqlPlDf.groupby(['advisor', 'GICS']).sum()[['RHAttr', 'YAAttr', 'LRAttr']].loc[
               (slice(g.reportAdvisor, g.reportAdvisor), slice(None)), :].reset_index().drop('advisor', 1).set_index(
            'GICS')
    gicsPl = sqlPlDf.groupby(['advisor', 'GICS', 'side']).sum()[['attribution']].loc[
             (slice(g.reportAdvisor, g.reportAdvisor), slice(None)), :].unstack()['attribution'].reset_index().drop(
            'advisor', 1).set_index('GICS').fillna(0)
    gicsTable = gicsTable.merge(fundGics, left_index=True, right_index=True).merge(gicsPl, left_index=True,
                                                                                   right_index=True)

    totalTurnOver = gicsTable['JPYPL'].sum()
    gicsTable['TO'] = gicsTable['JPYPL'] / totalTurnOver

    percent_fmt = lambda x: '{:.2f}%'.format(x * 100)
    percent1_fmt = lambda x: '{:.1f}%'.format(x * 100)
    money_fmt = lambda x: '{:,.0f}'.format(x)
    frmt_map = {'LongPL': money_fmt, 'ShortPL': money_fmt, 'Turnover': money_fmt, 'Rockhampton': percent_fmt,
                'Yaraka': percent_fmt, 'Longreach': percent_fmt, 'TO %': percent1_fmt, 'Return': percent1_fmt}

    total_series = gicsTable.sum()
    total_series.name = 'Total'
    gicsTotal = pd.DataFrame(total_series).T
    gicsTable = pd.concat([gicsTable, gicsTotal])
    gicsTable['Return'] = (gicsTable['L'] + gicsTable['S']) / gicsTable['JPYPL']
    gicsTable = gicsTable[['RHAttr', 'YAAttr', 'LRAttr', 'L', 'S', 'JPYPL', 'TO', 'Return']]
    gicsTable = gicsTable.rename(
            columns={'JPYPL': 'Turnover', 'RHAttr': 'Rockhampton', 'YAAttr': 'Yaraka', 'LRAttr': 'Longreach',
                     'L': 'LongPL',
                     'S': 'ShortPL', 'TO': 'TO %'})
    frmt = {col: frmt_map[col] for col in gicsTable.columns if col in frmt_map.keys()}
    gics_table_html = gicsTable.to_html(index_names=False, formatters=frmt, classes="borderTable")

    codeBetaDf['code'] = codeBetaDf[['code']].applymap(str.upper)[
        'code']  # some code has inconsistent format like xxxx Hk instead of HK
    t = sqlPlDf.merge(codeBetaDf, left_on='quick', right_on='code', how='left')
    sectorTable = turnover_merged_df.truncate(after=g.endDate).groupby(["advisor", "sector"]).sum()[['JPYPL']].loc[
                  (slice(g.reportAdvisor, g.reportAdvisor), slice(None)), :].reset_index().drop('advisor', 1).set_index(
            'sector')
    fundSector = t.groupby(['advisor', 'sector']).sum()[['RHAttr', 'YAAttr', 'LRAttr']].loc[
                 (slice(g.reportAdvisor, g.reportAdvisor), slice(None)), :].reset_index().drop('advisor', 1).set_index(
            'sector')

    sectorPl = t.groupby(['advisor', 'sector', 'side']).sum()[['attribution']].loc[
               (slice(g.reportAdvisor, g.reportAdvisor), slice(None)), :].unstack()['attribution'].reset_index().drop(
            'advisor', 1).set_index('sector').fillna(0)

    sectorTable = sectorTable.merge(fundSector, left_index=True, right_index=True, how='left').merge(sectorPl,
                                                                                                     left_index=True,
                                                                                                     right_index=True,
                                                                                                     how='left').fillna(
            0)

    sectorTotalTurnOver = sectorTable['JPYPL'].sum()

    sectorTable['TO'] = sectorTable['JPYPL'] / sectorTotalTurnOver

    sectorSeries = sectorTable.sum()
    sectorSeries.name = 'Total'
    sectorTable.ix['Tail'] = sectorTable.ix['Tail'] + sectorTable.ix['TailSens'] + sectorTable.ix['TailRR']
    sectorTable = sectorTable.drop(['TailSens', 'TailRR'])
    sectorTotal = pd.DataFrame(sectorSeries).T
    sectorTable = pd.concat([sectorTable, sectorTotal])
    sectorTable['Return'] = (sectorTable['L'] + sectorTable['S']) / sectorTable['JPYPL']
    sectorTable = sectorTable[['RHAttr', 'YAAttr', 'LRAttr', 'L', 'S', 'JPYPL', 'TO', 'Return']]
    sectorTable = sectorTable.rename(
            columns={'JPYPL': 'Turnover', 'RHAttr': 'Rockhampton', 'YAAttr': 'Yaraka', 'LRAttr': 'Longreach',
                     'L': 'LongPL',
                     'S': 'ShortPL', 'TO': 'TO %'})
    sectorTableHtml = sectorTable.to_html(index_names=False, formatters=frmt, classes="borderTable")

    topixTable = turnover_merged_df.truncate(after=g.endDate).groupby(["advisor", "TOPIX"]).sum()[['JPYPL']].loc[
                 (slice(g.reportAdvisor, g.reportAdvisor), slice(None)), :].reset_index().drop('advisor', 1).set_index(
            'TOPIX')
    fundTopix = sqlPlDf.groupby(['advisor', 'TPX']).sum()[['RHAttr', 'YAAttr', 'LRAttr']].loc[
                (slice(g.reportAdvisor, g.reportAdvisor), slice(None)), :].reset_index().drop('advisor', 1).set_index(
            'TPX')
    fundTopix = fundTopix.rename(index={'Warehousing  and  Harbor Transpo': 'Warehousing  and  Harbor Transport'})
    topixPl = sqlPlDf.groupby(['advisor', 'TPX', 'side']).sum()[['attribution']].loc[
              (slice(g.reportAdvisor, g.reportAdvisor), slice(None)), :].unstack()['attribution'].reset_index().drop(
            'advisor', 1).set_index('TPX')
    topixPl = topixPl.rename(index={'Warehousing  and  Harbor Transpo': 'Warehousing  and  Harbor Transport'})
    topixTable = topixTable.merge(fundTopix, left_index=True, right_index=True).merge(topixPl.fillna(0),
                                                                                      left_index=True, right_index=True)
    totalTurnOver = topixTable['JPYPL'].sum()
    topixTable['TO'] = topixTable['JPYPL'] / totalTurnOver

    topixSeries = topixTable.sum()
    topixSeries.name = 'Total'
    topixTotal = pd.DataFrame(topixSeries).T
    topixTable = pd.concat([topixTable, topixTotal])
    topixTable['Return'] = (topixTable['L'] + topixTable['S'].fillna(0)) / topixTable['JPYPL']
    topixTable = topixTable[['RHAttr', 'YAAttr', 'LRAttr', 'L', 'S', 'JPYPL', 'TO', 'Return']]
    topixTable = topixTable.rename(
            columns={'JPYPL': 'Turnover', 'RHAttr': 'Rockhampton', 'YAAttr': 'Yaraka', 'LRAttr': 'Longreach',
                     'L': 'LongPL',
                     'S': 'ShortPL', 'TO': 'TO %'})

    topixTableHtml = topixTable.to_html(index_names=False, formatters=frmt, classes="borderTable")

    strategyTable = turnover_merged_df.truncate(after=g.endDate).groupby(["advisor", "strategy"]).sum()[['JPYPL']].loc[
                    (slice(g.reportAdvisor, g.reportAdvisor), slice(None)), :].reset_index().drop('advisor',
                                                                                                  1).set_index(
            'strategy')
    fundStrategy = sqlPlDf.groupby(['advisor', 'strategy']).sum()[['RHAttr', 'YAAttr', 'LRAttr']].loc[
                   (slice(g.reportAdvisor, g.reportAdvisor), slice(None)), :].reset_index().drop('advisor',
                                                                                                 1).set_index(
            'strategy')
    fundStrategy = fundStrategy.fillna(0).drop([''])
    strategyPl = sqlPlDf.groupby(['advisor', 'strategy', 'side']).sum()[['attribution']].loc[
                 (slice(g.reportAdvisor, g.reportAdvisor), slice(None)), :].unstack()['attribution'].reset_index().drop(
            'advisor', 1).set_index('strategy')
    strategyPl = strategyPl.fillna(0).drop([''])
    strategyTable = strategyTable.merge(fundStrategy, left_index=True, right_index=True).merge(strategyPl,
                                                                                               left_index=True,
                                                                                               right_index=True)

    totalStrategyTurnOver = strategyTable['JPYPL'].sum()
    strategyTable['TO'] = strategyTable['JPYPL'] / totalStrategyTurnOver

    strategySeries = strategyTable.sum()
    strategySeries.name = 'Total'
    strategyTotal = pd.DataFrame(strategySeries).T
    strategyTable = pd.concat([strategyTable, strategyTotal])
    strategyTable['Return'] = (strategyTable['L'] + strategyTable['S'].fillna(0)) / strategyTable['JPYPL']
    strategyTable = strategyTable[['RHAttr', 'YAAttr', 'LRAttr', 'L', 'S', 'JPYPL', 'TO', 'Return']]
    strategyTable = strategyTable.rename(
            columns={'JPYPL': 'Turnover', 'RHAttr': 'Rockhampton', 'YAAttr': 'Yaraka', 'LRAttr': 'Longreach',
                     'L': 'LongPL',
                     'S': 'ShortPL', 'TO': 'TO %'})
    strategy_table_html = strategyTable.to_html(index_names=False, formatters=frmt, classes="borderTable")

    positionTable = turnover_merged_df.truncate(after=g.endDate).groupby(["advisor", "code"]).sum()[['JPYPL']].loc[
                    (slice(g.reportAdvisor, g.reportAdvisor), slice(None)), :].reset_index().drop('advisor',
                                                                                                  1).set_index('code')
    positionPl = sqlPlDf.groupby(['advisor', 'quick', 'name']).sum()[['RHAttr', 'YAAttr', 'LRAttr']].loc[
                 (slice(g.reportAdvisor, g.reportAdvisor), slice(None)), :].reset_index().drop('advisor', 1).set_index(
            'quick')
    sidePl = sqlPlDf.groupby(['advisor', 'quick', 'side']).sum()[['attribution']].loc[
             (slice(g.reportAdvisor, g.reportAdvisor), slice(None)), :].unstack()['attribution'].reset_index().drop(
            'advisor', 1).set_index('quick').fillna(0)
    positionTable = positionTable.merge(positionPl, left_index=True, right_index=True).merge(sidePl, left_index=True,
                                                                                             right_index=True)

    totalPositionTurnOver = positionTable['JPYPL'].sum()
    positionTable['TO'] = positionTable['JPYPL'] / totalPositionTurnOver
    positionTable = positionTable.reset_index().set_index(['name']).sort_index().drop('code', 1)
    positionSeries = positionTable.sum()
    positionSeries.name = 'Total'
    positionTotal = pd.DataFrame(positionSeries).T

    positionTable = pd.concat([positionTable, positionTotal])
    positionTable['Return'] = (positionTable['L'] + positionTable['S'].fillna(0)) / positionTable['JPYPL']
    positionTable = positionTable[['RHAttr', 'YAAttr', 'LRAttr', 'L', 'S', 'JPYPL', 'TO', 'Return']]
    positionTable = positionTable.rename(
            columns={'JPYPL': 'Turnover', 'RHAttr': 'Rockhampton', 'YAAttr': 'Yaraka', 'LRAttr': 'Longreach',
                     'L': 'LongPL',
                     'S': 'ShortPL', 'TO': 'TO %'})

    position_table_csv = positionTable.to_csv()
    rows = positionTable.to_csv().split('\n')
    count = 0
    position_table_html = '<section class="sheet padding-10mm"><table class="dataframe borderTable" border="1">'
    table_header = '<thead><tr>' + ''.join(['<th>' + h + '</th>' for h in rows[0].split(',')]) + '</tr></thead><tbody>'
    position_table_html += table_header
    # TODO: put this into a function with number of rows to roll over a new page as a parameter and return the total number of rows
    for r in rows[1:]:
        if (count > 30):
            position_table_html += '</tbody></table></section><section class="sheet padding-10mm"><table class="dataframe borderTable" border="1">' + table_header
            count %= 30
        elif (r != ''):
            elements = r.split(',')
            position_table_html += '<tr>' + ''.join(['<th>' + elements[0] + '</th>'] + [
                '<td>' + '{:.2f}%'.format(float(h) * 100) + '</td>' for h in elements[1:4]] + [
                                                        '<td>' + '{:,.0f}'.format(float(h)) + '</td>' for h in
                                                        elements[4:7]] + [
                                                        '<td>' + '{:.1f}%'.format(float(h)*100) + '</td>' for h in
                                                        elements[7:]
                                                        ]) + '</tr>'
        count += 1
    position_table_html += '</tbody></table></section>'

    render_obj = dict()
    render_obj['graph_width'] = 750
    render_obj['graph_height'] = 240
    render_obj['graph_line_width'] = g.lineWidth
    render_obj['margin_left'] = 40
    render_obj['margin_top']  = 40
    render_obj['margin_bottom'] = 30
    render_obj['margin_right'] = 5
    render_obj['graph_font']  = 'Calibri'
    render_obj['graph_font_size'] = 10
    render_obj['analyst'] = g.reportAdvisor
    render_obj['index'] = g.indexMapping[g.reportAdvisor]
    render_obj['startDate'] = g.fromDate
    render_obj['endDate'] = g.endDate
    render_obj['longTurnover'] = Decimal(sumTurnoverPerAdv.ix[g.reportAdvisor]['L']).quantize(Decimal('1.'),
                                                                                              rounding=ROUND_HALF_UP)
    render_obj['shortTurnover'] = Decimal(sumTurnoverPerAdv.ix[g.reportAdvisor]['S']).quantize(Decimal('1.'),
                                                                                               rounding=ROUND_HALF_UP)
    render_obj['totalLong'] = totalRatio.ix[g.reportAdvisor]['L']
    render_obj['totalShort'] = totalRatio.ix[g.reportAdvisor]['S']
    render_obj['longPL'] = Decimal(cs_attr_df['L'].iloc[-1]).quantize(Decimal('1.'), rounding=ROUND_HALF_UP)
    render_obj['shortPL'] = Decimal(cs_attr_df['S'].iloc[-1]).quantize(Decimal('1.'), rounding=ROUND_HALF_UP)
    render_obj['longIndexOP'] = net_op['L'].iloc[-1] * 100
    render_obj['shortIndexOP'] = net_op['S'].iloc[-1] * 100
    render_obj['longBetaOP'] = beta_op['L'].iloc[-1] * 100
    render_obj['shortBetaOP'] = beta_op['S'].iloc[-1] * 100
    render_obj['longHitRate'] = hitRateDf['LongsHR'].ix[g.reportAdvisor]
    render_obj['shortHitRate'] = hitRateDf['ShortsHR'].ix[g.reportAdvisor]
    render_obj['longReturn'] = long_short_return['attribution']['L'].ix[g.reportAdvisor]
    render_obj['shortReturn'] = long_short_return['attribution']['S'].ix[g.reportAdvisor]
    render_obj['rhBpsLong'] = totalFund[totalFund['advisor'] == g.reportAdvisor].sum()['RHAttr']['L'] * 100
    render_obj['rhBpsShort'] = totalFund[totalFund['advisor'] == g.reportAdvisor].sum()['RHAttr']['S'] * 100
    render_obj['yaBpsLong'] = totalFund[totalFund['advisor'] == g.reportAdvisor].sum()['YAAttr']['L'] * 100
    render_obj['yaBpsShort'] = totalFund[totalFund['advisor'] == g.reportAdvisor].sum()['YAAttr']['S'] * 100
    render_obj['lrBpsLong'] = totalFund[totalFund['advisor'] == g.reportAdvisor].sum()['LRAttr']['L'] * 100
    render_obj['lrBpsShort'] = totalFund[totalFund['advisor'] == g.reportAdvisor].sum()['LRAttr']['S'] * 100
    render_obj['exposure_avg_long'] = (
        exposure_avg[(exposure_avg['advisor'] == g.reportAdvisor) & (exposure_avg['side'] == 'L')].mean() * 100 / aumDf[
            'Total'].mean()).iloc[0]
    render_obj['exposure_avg_short'] = (
        exposure_avg[(exposure_avg['advisor'] == g.reportAdvisor) & (exposure_avg['side'] == 'S')].mean() * 100 / aumDf[
            'Total'].mean()).iloc[0]
    render_obj['rank_long'] = rankLongDf.ix[g.reportAdvisor][0]
    render_obj['rank_short'] = rankShortDf.ix[g.reportAdvisor][0]
    render_obj['netop_graph'] = netop_graph
    render_obj['betaop_graph'] = beta_graph
    render_obj['exposure_graph'] = exposure_graph
    render_obj['op_graph'] = op_graph
    render_obj['gross_exposure_graph'] = gross_exposure_graph
    render_obj['short_exposure_graph'] = short_exposure_graph
    render_obj['names_graph'] = names_graph
    render_obj['gics_table'] = gics_table_html
    render_obj['sector_table'] = sectorTableHtml
    render_obj['topix_table'] = topixTableHtml
    render_obj['strategy_table'] = strategy_table_html
    render_obj['position_table'] = position_table_html

    # renderObj['test'] = long_short_return

    return render_template('attrib.html', params=render_obj, pl_graph=pl_graph)


@app.route('/test')
def test():
    return render_template('test.html')


@app.route('/test2')
def test2():
    return render_template('test2.html')


@app.route('/logout')
def logout():
    session.pop('logged_in', None)
    flash('You were logged out')
    return redirect(url_for('show_entries'))


# default port 5000
if __name__ == '__main__':
    app.run(host='0.0.0.0')
