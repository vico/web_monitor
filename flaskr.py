# all the imports
import urlparse
import StringIO
import sqlite3
from flask import Flask, request, session, g, redirect, url_for, \
    abort, render_template, flash, Response
from contextlib import closing
import pyotp
import qrcode

import pandas as pd
from pandas import DataFrame
from pandas.io import sql
from pandas.tseries.offsets import *
import numpy as np
from numpy.random import randn
# import pylab as plt
import pymysql
# from matplotlib import pyplot
from datetime import datetime, timedelta
import csv
import math
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

    trade20141231 = {
        'tradeDate': ['31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14',
                      '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14', '31-Dec-14'],
        'code': ['1407', '1407', '1407', '1552', '1552', '1552', '1793', '1799', '1810', '1812', '1812', '1812', '1824',
                 '1824', '1824', '1867', '1869', '1885', '1885', '1885', '1893', '1893', '1893', '1938', '1944', '1944',
                 '1944', '1961', '1961', '2206', '2206', '2206', '2282', '2282', '2282', '2326', '2326', '2326', '2413',
                 '2413', '2413', '2432', '2432', '2432', '2468', '2468', '2503', '2503', '2503', '2607', '2607', '2607',
                 '2685', '2685', '2702', '2702', '2702', '2706', '2706', '2706', '2726', '2767', '2802', '2802', '2802',
                 '2809', '2809', '2809', '2904', '2931', '2931', '2931', '3107', '3107', '3107', '3166', '3230', '3278',
                 '3278', '3278', '3288', '3288', '3288', '3291', '3291', '3291', '3309', '3309', '3309', '3333', '3333',
                 '3397', '3397', '3401', '3401', '3401', '3402', '3402', '3402', '3436', '3436', '3436', '3451', '3451',
                 '3451', '3626', '3626', '3626', '3655', '3769', '3793', '3793', '3880', '3880', '3880', '4042', '4042',
                 '4042', '4082', '4182', '4182', '4182', '4293', '4452', '4452', '4452', '4519', '4519', '4519', '4549',
                 '4549', '4549', '4551', '4551', '4587', '4587', '4587', '4651', '4651', '4651', '4680', '4680', '4680',
                 '4704', '4704', '4704', '4708', '4708', '4708', '4813', '4813', '4813', '4819', '4819', '4819', '4901',
                 '4901', '4901', '4922', '4922', '4922', '5007', '5007', '5007', '5012', '5012', '5012', '5020', '5020',
                 '5020', '5201', '5201', '5201', '5401', '5401', '5401', '5411', '5411', '5411', '5471', '5471', '5471',
                 '5486', '5486', '5486', '5706', '5706', '5706', '5711', '5711', '5711', '5727', '5803', '5803', '5803',
                 '5809', '5809', '5809', '5915', '5921', '5938', '5938', '5938', '5958', '5958', '5973', '6027', '6027',
                 '6027', '6028', '6028', '6028', '6091', '6268', '6268', '6268', '6273', '6273', '6273', '6303', '6305',
                 '6305', '6305', '6310', '6310', '6366', '6366', '6366', '6383', '6383', '6383', '6460', '6460', '6460',
                 '6479', '6479', '6479', '6501', '6501', '6501', '6503', '6503', '6503', '6507', '6581', '6586', '6586',
                 '6586', '6592', '6592', '6592', '6594', '6594', '6594', '6640', '6645', '6645', '6645', '6740', '6740',
                 '6740', '6754', '6754', '6754', '6755', '6755', '6755', '6762', '6762', '6762', '6804', '6804', '6804',
                 '6816', '6816', '6816', '6856', '6856', '6856', '6860', '6889', '6902', '6902', '6902', '6952', '6952',
                 '6952', '6963', '6963', '6963', '6971', '6971', '6971', '6981', '6981', '6981', '7012', '7012', '7012',
                 '7201', '7201', '7201', '7205', '7205', '7205', '7222', '7222', '7222', '7230', '7230', '7230', '7246',
                 '7246', '7246', '7259', '7259', '7259', '7261', '7261', '7261', '7262', '7262', '7262', '7267', '7267',
                 '7267', '7269', '7269', '7269', '7272', '7272', '7272', '7287', '7287', '7312', '7312', '7312', '7425',
                 '7453', '7453', '7453', '7475', '7485', '7510', '7532', '7532', '7532', '7552', '7552', '7552', '7606',
                 '7606', '7606', '7731', '7731', '7731', '7741', '7741', '7741', '7751', '7751', '7751', '7956', '7956',
                 '7956', '8001', '8001', '8001', '8006', '8015', '8015', '8015', '8028', '8028', '8028', '8031', '8031',
                 '8031', '8046', '8046', '8050', '8050', '8050', '8088', '8088', '8088', '8125', '8125', '8141', '8141',
                 '8237', '8237', '8253', '8253', '8253', '8397', '8397', '8397', '8595', '8595', '8595', '8601', '8601',
                 '8601', '8604', '8604', '8604', '8697', '8697', '8697', '8801', '8801', '8801', '8802', '8802', '8802',
                 '8841', '8841', '8841', '8963', '8963', '8963', '8966', '8966', '8966', '8975', '8975', '8975', '9020',
                 '9020', '9020', '9024', '9024', '9024', '9064', '9064', '9064', '9101', '9101', '9101', '9104', '9104',
                 '9104', '9107', '9107', '9107', '9201', '9201', '9201', '9202', '9202', '9202', '9204', '9204', '9204',
                 '9424', '9424', '9424', '9433', '9433', '9433', '9468', '9468', '9468', '9616', '9616', '9616', '9639',
                 '9639', '9640', '9640', '9684', '9684', '9684', '9706', '9706', '9706', '9749', '9749', '9749', '9766',
                 '9766', '9766', '9792', '9792', '9837', '9956', '9983', '9983', '9983', '9984', '9984', '9984',
                 '005930 KS', '005930 KS', '005930 KS', '035420 KS', '035420 KS', '035420 KS', '1109 HK', '1109 HK',
                 '1109 HK', '1138 HK', '1138 HK', '1138 HK', '1148 HK', '1148 HK', '1148 HK', '1330 HK', '1330 HK',
                 '1330 HK', '1347 HK', '1347 HK', '1347 HK', '152 HK', '152 HK', '152 HK', '1685 HK', '1685 HK',
                 '1685 HK', '1882 HK', '1882 HK', '1882 HK', '200581 CH', '200581 CH', '200581 CH', '200625 CH',
                 '200625 CH', '200625 CH', '2009 HK', '2009 HK', '2009 HK', '2038 HK', '2038 HK', '2038 HK', '2313 HK',
                 '2313 HK', '2313 HK', '2318 HK', '2318 HK', '2318 HK', '2330 TT', '2330 TT', '2330 TT', '2333 HK',
                 '2333 HK', '2333 HK', '2338 HK', '2338 HK', '2338 HK', '2343 HK', '2343 HK', '2343 HK', '2357 HK',
                 '2357 HK', '2357 HK', '2357 TT', '2357 TT', '2357 TT', '2369 HK', '2369 HK', '2369 HK', '2379 TT',
                 '2379 TT', '2379 TT', '2380 HK', '2380 HK', '2380 HK', '2409 TT', '2409 TT', '2409 TT', '2448 TT',
                 '2448 TT', '2448 TT', '2474 TT', '2474 TT', '2474 TT', '2618 HK', '2618 HK', '2618 HK', '267 HK',
                 '267 HK', '267 HK', '2727 HK', '2727 HK', '2727 HK', '2777 HK', '2777 HK', '2777 HK', '3 HK', '3 HK',
                 '3 HK', '3008 TT', '3008 TT', '3008 TT', '3105 TT', '3105 TT', '3105 TT', '316 HK', '316 HK', '316 HK',
                 '3311 HK', '3311 HK', '3311 HK', '3337 HK', '3337 HK', '3337 HK', '3368 HK', '3368 HK', '3368 HK',
                 '3673 TT', '3673 TT', '3673 TT', '371 HK', '371 HK', '371 HK', '3818 HK', '3818 HK', '3818 HK',
                 '386 HK', '386 HK', '386 HK', '493 HK', '493 HK', '493 HK', '5490 TT', '5490 TT', '5490 TT',
                 '600741 C1', '600741 C1', '600741 C1', '6030 HK', '6030 HK', '6030 HK', '6136 HK', '6136 HK',
                 '6136 HK', '6176 TT', '6176 TT', '6176 TT', '6199 HK', '6199 HK', '6199 HK', '636 HK', '636 HK',
                 '636 HK', '750 HK', '750 HK', '750 HK', '753 HK', '753 HK', '753 HK', '816 HK', '816 HK', '816 HK',
                 '861 HK', '861 HK', '861 HK', '916 HK', '916 HK', '916 HK', '940 HK', '940 HK', '940 HK', '967 HK',
                 '967 HK', '967 HK', '991 HK', '991 HK', '991 HK', 'CAJ US', 'CAJ US', 'CAJ US', 'HIF5', 'HIF5', 'HIF5',
                 'JPWH5', 'JPWH5', 'JPWH5', 'NKH5', 'NKH5', 'NKH5', 'TPH5', 'TPH5', 'TPH5'],
        'currencyCode': ['JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'KRW',
                         'KRW', 'KRW', 'KRW', 'KRW', 'KRW', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD',
                         'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD',
                         'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD',
                         'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'TWD', 'TWD', 'TWD', 'HKD', 'HKD',
                         'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'TWD', 'TWD', 'TWD',
                         'HKD', 'HKD', 'HKD', 'TWD', 'TWD', 'TWD', 'HKD', 'HKD', 'HKD', 'TWD', 'TWD', 'TWD', 'TWD',
                         'TWD', 'TWD', 'TWD', 'TWD', 'TWD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD',
                         'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'TWD', 'TWD', 'TWD', 'TWD', 'TWD', 'TWD',
                         'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'TWD',
                         'TWD', 'TWD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD',
                         'HKD', 'TWD', 'TWD', 'TWD', 'CNY', 'CNY', 'CNY', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD',
                         'TWD', 'TWD', 'TWD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD',
                         'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD',
                         'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'HKD', 'USD', 'USD', 'USD', 'HKD', 'HKD', 'HKD',
                         'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY', 'JPY'],
        'side': ['S', 'S', 'S', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L',
                 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L',
                 'L', 'L', 'L', 'L', 'L', 'L', 'S', 'S', 'S', 'L', 'L', 'L', 'L', 'L', 'S', 'S', 'S', 'L', 'L', 'L',
                 'L', 'S', 'L', 'L', 'L', 'L', 'L', 'L', 'S', 'S', 'S', 'S', 'L', 'L', 'L', 'S', 'L', 'L', 'L', 'L',
                 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L',
                 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'S', 'S', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'S',
                 'S', 'S', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'S', 'S', 'S',
                 'S', 'S', 'S', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'S', 'S', 'S', 'L', 'L', 'L', 'L', 'L',
                 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'S', 'S', 'S', 'L', 'L', 'L', 'L', 'L', 'L', 'L',
                 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L',
                 'S', 'S', 'S', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'S', 'S', 'S', 'S', 'S', 'S', 'L',
                 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'L', 'L', 'L', 'S', 'S', 'S', 'L', 'L', 'L', 'L', 'L', 'L',
                 'L', 'L', 'L', 'L', 'S', 'S', 'S', 'S', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'S', 'S', 'S', 'L', 'L',
                 'L', 'S', 'S', 'S', 'S', 'S', 'S', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L',
                 'S', 'L', 'L', 'L', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'L', 'L', 'L', 'L', 'L', 'L', 'S',
                 'S', 'S', 'S', 'S', 'S', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'S', 'S', 'S', 'L', 'L', 'L',
                 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'L', 'L', 'S', 'S', 'S', 'L', 'L', 'L',
                 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'S', 'S', 'S', 'L', 'L', 'L', 'L',
                 'L', 'L', 'L', 'L', 'L', 'S', 'S', 'S', 'L', 'S', 'S', 'S', 'S', 'S', 'S', 'L', 'L', 'L', 'L', 'L',
                 'S', 'S', 'S', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'S', 'S',
                 'S', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L',
                 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'S', 'S', 'S', 'S', 'S', 'S', 'L', 'L', 'L',
                 'S', 'S', 'S', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S',
                 'S', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L',
                 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L',
                 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L',
                 'S', 'S', 'S', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L',
                 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'S', 'S', 'S', 'L', 'L', 'L', 'L', 'L', 'L', 'S', 'S', 'S', 'L',
                 'L', 'L', 'S', 'S', 'S', 'L', 'L', 'L', 'S', 'S', 'S', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L',
                 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'S', 'S', 'S', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L',
                 'L', 'S', 'S', 'S', 'S', 'S', 'S', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'S',
                 'S', 'S', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'S', 'S', 'S', 'L', 'L', 'L', 'S', 'S', 'S',
                 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L',
                 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'L', 'S', 'S', 'S', 'L', 'L', 'L', 'S',
                 'S', 'S', 'S', 'S', 'S'],
        'Turnover': [68715000, 10892600, 2850400, 2858498100, 444153880, 114364520, 28908000, 10286000, 27048000,
                     520457000, 88323000, 25948000, 1093596000, 169764000, 44415000, 3990000, 23193000, 512325000,
                     94599000, 90045000, 725493600, 31588200, 118652400, 43475000, 699556000, 34244000, 123523000,
                     30704000, 115544000, 1448825000, 65450000, 240550000, 1437710000, 60674000, 229506000, 589360000,
                     44520000, 108120000, 1364918100, 212617300, 54823300, 1971187200, 313203600, 81843600, 45562500,
                     12500000, 1422449400, 58981800, 225597900, 553372400, 94279400, 26299800, 134300000, 34444000,
                     675121000, 107304000, 27878000, 333926000, 56735000, 14589000, 215812000, 181557600, 1390660000,
                     237758000, 65047000, 634556900, 100394600, 26111600, 20197100, 249559200, 41141100, 10699700,
                     398370000, 61950000, 17010000, 16915000, 36205000, 702975000, 109592000, 28119000, 189128100,
                     30374400, 7830900, 705153800, 112180200, 29116600, 356875100, 57421800, 14563500, 70659600,
                     19220300, 361164600, 26828400, 674421000, 110745000, 30174000, 710658800, 110374800, 28077800,
                     350674500, 14691600, 56142900, 381743100, 14677200, 56806200, 1449743000, 65484000, 238107100,
                     4564800, 171612000, 9709200, 39735800, 605268000, 29670000, 105823000, 2063306000, 82626000,
                     321128000, 66504000, 1190934000, 49167000, 187563000, 226319800, 1464680300, 232141600, 60889600,
                     232970400, 39717600, 11856000, 297137300, 92477400, 28289500, 65512200, 18296200, 617288000,
                     98301000, 25368000, 651572000, 108445000, 27183000, 281248000, 45056000, 11545600, 609550000,
                     96192000, 25050000, 295363500, 50286500, 13157000, 507949200, 85750400, 48310800, 364149800,
                     57623300, 14912100, 2737259600, 436347900, 112880500, 1385842500, 219240000, 57172500, 332082000,
                     53010000, 13680000, 668850000, 105987000, 27783000, 2037992400, 324631800, 84000240, 1282842000,
                     210273000, 55955000, 1380228300, 219356100, 56870100, 1390057600, 229699200, 63356000, 1365516000,
                     216618000, 57125000, 1732080000, 276308000, 72170000, 1186943000, 197482000, 55377000, 1401774000,
                     222708000, 58290000, 223161600, 554500000, 92500000, 26000000, 1033066400, 175146900, 49729300,
                     35280000, 27690000, 669914200, 109822000, 29881800, 45715000, 41205000, 26779400, 319930000,
                     59117500, 13375000, 725077500, 115223200, 29792300, 2701800, 1304328200, 218073800, 60837300,
                     1029434000, 163047000, 41561000, 11520000, 1424726400, 225727200, 59320800, 169918000, 46945000,
                     624726000, 101606000, 28168000, 1403731200, 220078800, 57223200, 1568603600, 266231600, 75466000,
                     824428000, 129888000, 34276000, 1286199600, 214366600, 60346900, 858924000, 146046000, 40488000,
                     61588000, 200386400, 2079660000, 329348000, 84940000, 1221740000, 211640000, 58682000, 1309546800,
                     215117400, 54171900, 152575800, 1380485000, 219635000, 56680000, 486254000, 77108000, 20165000,
                     1368559300, 217230300, 56851600, 1409650000, 227175000, 57085000, 1721264000, 287116000, 79476000,
                     600989000, 102901700, 29477400, 670656000, 123951600, 33732400, 1272353500, 209181500, 55407000,
                     211767600, 109021000, 2720307600, 436334400, 123213600, 1173360500, 204337800, 57877100,
                     1310816000, 209024000, 54464000, 1301215200, 222240000, 62227200, 1060000000, 166950000, 45050000,
                     2230802000, 364980000, 97328000, 1908202100, 303887500, 81600400, 1849862000, 311302600, 86081100,
                     346390000, 61318400, 43040800, 572035300, 96038800, 26864000, 315923000, 105164000, 31032000,
                     2025946000, 321834500, 84051500, 1812708000, 301239750, 84604750, 2626034900, 414329600, 108319400,
                     1300036200, 204155400, 52537400, 1031212000, 163800000, 42588000, 1078387200, 171184200, 44688600,
                     216065000, 65640000, 220318800, 34917900, 9058200, 10212000, 1377888000, 220224000, 56544000,
                     32200200, 25179000, 35617400, 1593180000, 283900000, 79325000, 1229985900, 200847900, 53116800,
                     1207575000, 204525000, 58387500, 1341264800, 213813200, 55017200, 1318526000, 221259500, 61164500,
                     1280038650, 203162450, 52998900, 1367522000, 218154000, 57186000, 1771202800, 287082400, 76098800,
                     55567000, 2082527100, 335090100, 92876700, 1316315000, 216580000, 56875000, 1877048400, 299004600,
                     77669850, 41265000, 12576000, 375487000, 63147000, 17654000, 935256000, 154014000, 39900000,
                     165969600, 45780800, 108932000, 28438000, 128880900, 37328800, 1286726400, 222285600, 63026100,
                     340263000, 52046000, 13748000, 1029171500, 164934000, 42899500, 685114800, 108974000, 28428000,
                     5452649020, 851568760, 219414580, 617680800, 97274800, 26374800, 1341060000, 218085000, 55335000,
                     983867500, 163552000, 43443500, 213145320, 51918680, 32912000, 829547600, 148737100, 40491500,
                     514240200, 83483100, 20993400, 104083200, 16848000, 4399200, 2188607700, 350323200, 93054600,
                     1378740600, 231557400, 65595600, 1987195550, 326370350, 88117600, 1357740000, 223668000, 58482000,
                     2052403000, 325613000, 85083000, 1376700000, 236925000, 67600000, 86400000, 2104200000, 336600000,
                     55071200, 1335775900, 211305800, 216416700, 8957700, 34365600, 27496000, 625688000, 101864000,
                     54222700, 1305927000, 207726400, 1060704000, 42481500, 164973000, 672792000, 30264000, 115818000,
                     29056000, 99880000, 5217000, 17538000, 1394197200, 57684000, 220704000, 1337407500, 54549000,
                     212932500, 265541900, 25348300, 94994600, 2003328000, 82362000, 318126000, 59080800, 226954400,
                     14618500, 302364300, 66060000, 1413684000, 237816000, 103103000, 2276918000, 363384000, 2005097000,
                     86255000, 330423000, 2381640000, 404416000, 113920000, 17750600, 2822100, 695300, 7714000, 1308720,
                     372400, 4130920, 729560, 200080, 7743120, 1288980, 360360, 16814080, 2785280, 757760, 16734290,
                     2736890, 728320, 4892440, 830400, 242200, 8130920, 1406960, 392640, 11397151, 545031, 1935255,
                     13745827, 642330, 2283395, 8596005, 389400, 1398595, 11740360, 575850, 2114940, 12748800, 614400,
                     2201600, 31165400, 1265600, 4904200, 44697000, 2397000, 7896000, 20462400, 904050, 3329550,
                     18671700, 817500, 3073800, 5612090, 281700, 970300, 13143760, 574800, 2145920, 74497500, 3118500,
                     12127500, 7650000, 1211760, 318240, 31270000, 5406000, 1484000, 17248770, 2919990, 844950,
                     42606000, 7241400, 2057400, 76234800, 12076800, 3145000, 61132000, 2958000, 10106500, 14917490,
                     648900, 2458610, 12122740, 1916900, 502360, 8260000, 1313340, 338660, 12906400, 2125760, 626340,
                     17760000, 2823840, 728160, 34727500, 7185000, 2395000, 67695200, 11070400, 2930400, 13990475,
                     2222150, 566875, 9068800, 1482400, 392400, 4084820, 647960, 170340, 8507870, 1373520, 353080,
                     31267500, 1326500, 4927000, 19038710, 856980, 3211030, 7999860, 354660, 1305480, 17712500, 725000,
                     2812500, 16315680, 2671020, 712500, 71106900, 12048200, 3546900, 6721578, 1067064, 276018,
                     18001800, 2861600, 744600, 8756790, 1392580, 363580, 72978500, 11571000, 3045000, 15595800,
                     2841160, 911840, 8610000, 1445250, 399750, 9223200, 1522800, 432000, 2997060, 19010640, 777480,
                     13607200, 2219200, 598600, 14912060, 2523690, 726190, 14743890, 726300, 2542050, 280800, 6582600,
                     1085400, 13775320, 628600, 2227040, 592140, 11926200, 2076660, 210222, 5059838, 804164, 3547350,
                     89866200, 14189400, 174949000, 670425000, 4071076000, 174500000, 610750000, 3629600000, 1083775000,
                     3884700000, 23716375000],
        'advisor': ['HA', 'HA', 'HA', 'Bal', 'Bal', 'Bal', 'HA', 'HA', 'HA', 'HA', 'HA', 'HA', 'HA', 'HA', 'HA', 'HA',
                    'HA', 'HA', 'HA', 'HA', 'HA', 'HA', 'HA', 'HA', 'HA', 'HA', 'HA', 'HA', 'HA', 'SM', 'SM', 'SM',
                    'SM', 'SM', 'SM', 'TT', 'TT', 'TT', 'HA', 'HA', 'HA', 'TT', 'TT', 'TT', 'TT', 'TT', 'SM', 'SM',
                    'SM', 'SM', 'SM', 'SM', 'SM', 'SM', 'SM', 'SM', 'SM', 'TT', 'TT', 'TT', 'SM', 'TT', 'SM', 'SM',
                    'SM', 'SM', 'SM', 'SM', 'HA', 'HA', 'HA', 'HA', 'Adv', 'Adv', 'Adv', 'HA', 'HA', 'HA', 'HA', 'HA',
                    'HA', 'HA', 'HA', 'HA', 'HA', 'HA', 'HA', 'HA', 'HA', 'SM', 'SM', 'SM', 'SM', 'CS', 'CS', 'CS',
                    'CS', 'CS', 'CS', 'CS', 'CS', 'CS', 'HA', 'HA', 'HA', 'TT', 'TT', 'TT', 'TT', 'TT', 'TT', 'TT',
                    'CS', 'CS', 'CS', 'CS', 'CS', 'CS', 'CS', 'CS', 'CS', 'CS', 'TT', 'SM', 'SM', 'SM', 'HA', 'HA',
                    'HA', 'HA', 'HA', 'HA', 'Adv', 'Adv', 'HA', 'HA', 'HA', 'HA', 'HA', 'HA', 'SM', 'SM', 'SM', 'TT',
                    'TT', 'TT', 'TT', 'TT', 'TT', 'Adv', 'Adv', 'Adv', 'TT', 'TT', 'TT', 'CS', 'CS', 'CS', 'SM', 'SM',
                    'SM', 'TNi', 'TNi', 'TNi', 'TNi', 'TNi', 'TNi', 'TNi', 'TNi', 'TNi', 'CS', 'CS', 'CS', 'TI', 'TI',
                    'TI', 'TI', 'TI', 'TI', 'TI', 'TI', 'TI', 'TI', 'TI', 'TI', 'TI', 'TI', 'TI', 'TI', 'TI', 'TI',
                    'TI', 'TI', 'TI', 'TI', 'TI', 'TI', 'TI', 'HA', 'HA', 'HA', 'HA', 'HA', 'HA', 'HA', 'HA', 'HA',
                    'HA', 'HA', 'HA', 'HA', 'HA', 'HA', 'TI', 'TI', 'TI', 'TI', 'TI', 'TI', 'HA', 'TI', 'TI', 'TI',
                    'TI', 'TI', 'TI', 'TI', 'TI', 'TI', 'TI', 'TI', 'TT', 'TT', 'TT', 'CS', 'CS', 'CS', 'CS', 'CS',
                    'CS', 'TI', 'TI', 'TI', 'HA', 'TI', 'TI', 'TI', 'TI', 'CS', 'CS', 'CS', 'CS', 'CS', 'CS', 'CS',
                    'TI', 'TI', 'TI', 'CS', 'CS', 'CS', 'TT', 'TT', 'TT', 'TI', 'TI', 'TI', 'CS', 'CS', 'CS', 'TT',
                    'TT', 'TT', 'AP', 'AP', 'AP', 'CS', 'CS', 'CS', 'CS', 'HA', 'AP', 'AP', 'AP', 'Adv', 'Adv', 'Adv',
                    'CS', 'CS', 'CS', 'CS', 'CS', 'CS', 'CS', 'CS', 'CS', 'TI', 'TI', 'TI', 'AP', 'AP', 'AP', 'AP',
                    'AP', 'AP', 'AP', 'AP', 'AP', 'AP', 'AP', 'AP', 'AP', 'AP', 'AP', 'AP', 'AP', 'AP', 'AP', 'AP',
                    'AP', 'AP', 'AP', 'AP', 'AP', 'AP', 'AP', 'AP', 'AP', 'AP', 'AP', 'AP', 'AP', 'AP', 'AP', 'AP',
                    'AP', 'AP', 'HA', 'SM', 'SM', 'SM', 'HA', 'HA', 'HA', 'SM', 'SM', 'SM', 'TT', 'TT', 'TT', 'SM',
                    'SM', 'SM', 'CS', 'CS', 'CS', 'CS', 'CS', 'CS', 'CS', 'CS', 'CS', 'SM', 'SM', 'SM', 'TNi', 'TNi',
                    'TNi', 'HA', 'TNi', 'TNi', 'TNi', 'SM', 'SM', 'SM', 'TNi', 'TNi', 'TNi', 'HA', 'HA', 'Adv', 'Adv',
                    'Adv', 'HA', 'HA', 'HA', 'HA', 'HA', 'HA', 'HA', 'SM', 'SM', 'SM', 'SM', 'SM', 'HA', 'HA', 'HA',
                    'TT', 'TT', 'TT', 'Adv', 'Adv', 'Adv', 'Adv', 'Adv', 'Adv', 'Adv', 'Adv', 'Adv', 'HA', 'HA', 'HA',
                    'HA', 'HA', 'HA', 'HA', 'HA', 'HA', 'HA', 'HA', 'HA', 'HA', 'HA', 'HA', 'HA', 'HA', 'HA', 'HA',
                    'HA', 'HA', 'HA', 'HA', 'HA', 'AP', 'AP', 'AP', 'AP', 'AP', 'AP', 'AP', 'AP', 'AP', 'AP', 'AP',
                    'AP', 'AP', 'AP', 'AP', 'Adv', 'Adv', 'Adv', 'AP', 'AP', 'AP', 'TT', 'TT', 'TT', 'TT', 'TT', 'TT',
                    'TT', 'TT', 'TT', 'HA', 'HA', 'HA', 'HA', 'HA', 'TT', 'TT', 'TT', 'TT', 'TT', 'Adv', 'Adv', 'Adv',
                    'TT', 'TT', 'TT', 'TT', 'TT', 'TT', 'HA', 'HA', 'HA', 'SM', 'SM', 'SM', 'SM', 'TT', 'TT', 'TT',
                    'EL', 'EL', 'EL', 'TT', 'TT', 'TT', 'DH', 'DH', 'DH', 'AQ', 'AQ', 'AQ', 'AQ', 'AQ', 'AQ', 'DH',
                    'DH', 'DH', 'EL', 'EL', 'EL', 'AQ', 'AQ', 'AQ', 'AQ', 'AQ', 'AQ', 'AQ', 'AQ', 'AQ', 'AQ', 'AQ',
                    'AQ', 'AQ', 'AQ', 'AQ', 'KW', 'KW', 'KW', 'DH', 'DH', 'DH', 'DH', 'DH', 'DH', 'DH', 'DH', 'DH',
                    'EL', 'EL', 'EL', 'AQ', 'AQ', 'AQ', 'AQ', 'AQ', 'AQ', 'AQ', 'AQ', 'AQ', 'AQ', 'AQ', 'AQ', 'EL',
                    'EL', 'EL', 'EL', 'EL', 'EL', 'EL', 'EL', 'EL', 'DH', 'DH', 'DH', 'EL', 'EL', 'EL', 'EL', 'EL',
                    'EL', 'EL', 'EL', 'EL', 'EL', 'EL', 'EL', 'KW', 'KW', 'KW', 'AQ', 'AQ', 'AQ', 'DH', 'DH', 'DH',
                    'DH', 'DH', 'DH', 'EL', 'EL', 'EL', 'EL', 'EL', 'EL', 'AQ', 'AQ', 'AQ', 'AQ', 'AQ', 'AQ', 'DH',
                    'DH', 'DH', 'DH', 'DH', 'DH', 'EL', 'EL', 'EL', 'DH', 'DH', 'DH', 'DH', 'DH', 'DH', 'DH', 'DH',
                    'DH', 'DH', 'DH', 'DH', 'EL', 'EL', 'EL', 'AQ', 'AQ', 'AQ', 'KW', 'KW', 'KW', 'DH', 'DH', 'DH',
                    'EL', 'EL', 'EL', 'AQ', 'AQ', 'AQ', 'AQ', 'AQ', 'AQ', 'DH', 'DH', 'DH', 'AQ', 'AQ', 'AQ', 'DH',
                    'DH', 'DH', 'DH', 'DH', 'DH', 'DH', 'DH', 'DH', 'DH', 'DH', 'DH', 'DH', 'DH', 'DH', 'DH', 'DH',
                    'DH', 'Adv', 'Adv', 'Adv', 'NJD', 'NJD', 'NJD', 'Bal', 'Bal', 'Bal', 'Bal', 'Bal', 'Bal', 'Bal',
                    'Bal', 'Bal'],
        'strategy': ['Solar', 'Solar', 'Solar', 'Balance', 'Balance', 'Balance', 'Abe', 'Abe', 'Abe', 'Abe', 'Abe',
                     'Abe', 'Abe', 'Abe', 'Abe', 'Abe', 'Abe', 'Abe', 'Abe', 'Abe', 'Abe', 'Abe', 'Abe', 'Abe', 'North',
                     'North', 'North', 'Abe', 'Abe', 'Foods', 'Foods', 'Foods', 'Foods', 'Foods', 'Foods', 'Blackswan',
                     'Blackswan', 'Blackswan', 'Healthcare', 'Healthcare', 'Healthcare', 'SNS', 'SNS', 'SNS', 'Voice',
                     'Voice', 'Foods', 'Foods', 'Foods', 'Foods', 'Foods', 'Foods', 'Retail', 'Retail', 'Yutai',
                     'Yutai', 'Yutai', 'Gamesoft', 'Gamesoft', 'Gamesoft', 'Retail', 'Pachinko', 'Foods', 'Foods',
                     'Foods', 'Foods', 'Foods', 'Foods', 'Index', 'Biohazard', 'Biohazard', 'Biohazard', 'Blackswan',
                     'Blackswan', 'Blackswan', 'Index', 'Real Estate', 'REIT', 'REIT', 'REIT', 'Housing', 'Housing',
                     'Housing', 'Housing', 'Housing', 'Housing', 'IPO', 'IPO', 'IPO', 'Bicycle', 'Bicycle', 'Retail',
                     'Retail', 'Chemicals', 'Chemicals', 'Chemicals', 'Chemicals', 'Chemicals', 'Chemicals', 'Tech',
                     'Tech', 'Tech', 'IPO', 'IPO', 'IPO', 'Software', 'Software', 'Software', 'Wearable', 'NFC', 'SNS',
                     'SNS', 'Paper', 'Paper', 'Paper', 'Chemicals', 'Chemicals', 'Chemicals', 'Hydro', 'Chemicals',
                     'Chemicals', 'Chemicals', 'Internet', 'Toilet', 'Toilet', 'Toilet', 'Blackswan', 'Blackswan',
                     'Blackswan', 'Blackswan', 'Blackswan', 'Blackswan', 'EF', 'EF', 'Biohazard', 'Biohazard',
                     'Biohazard', 'Solar', 'Solar', 'Solar', 'Leisure', 'Leisure', 'Leisure', 'Internet', 'Internet',
                     'Internet', 'Big Bro', 'Big Bro', 'Big Bro', 'Internet', 'Internet', 'Internet', 'Internet',
                     'Internet', 'Internet', 'Tech', 'Tech', 'Tech', 'Cosmetics', 'Cosmetics', 'Cosmetics', 'Oil',
                     'Oil', 'Oil', 'Oil', 'Oil', 'Oil', 'Oil', 'Oil', 'Oil', 'Tech', 'Tech', 'Tech', 'Steel', 'Steel',
                     'Steel', 'Steel', 'Steel', 'Steel', 'Steel', 'Steel', 'Steel', 'Commodities', 'Commodities',
                     'Commodities', 'Commodities', 'Commodities', 'Commodities', 'Commodities', 'Commodities',
                     'Commodities', 'Commodities', 'Commodities', 'Commodities', 'Commodities', 'Commodities',
                     'Commodities', 'Commodities', 'Abe', 'Abe', 'Housing', 'Housing', 'Housing', 'Abe', 'Abe', 'Abe',
                     'IPO', 'IPO', 'IPO', 'IPO', 'IPO', 'IPO', 'Index', 'Machinery', 'Machinery', 'Machinery',
                     'Machinery', 'Machinery', 'Machinery', 'Water', 'Machinery', 'Machinery', 'Machinery', 'Machinery',
                     'Machinery', 'Machinery', 'Machinery', 'Machinery', 'Machinery', 'Machinery', 'Machinery',
                     'Pachinko', 'Pachinko', 'Pachinko', 'Tech', 'Tech', 'Tech', 'Tech', 'Tech', 'Tech', 'Machinery',
                     'Machinery', 'Machinery', 'Pension', 'Machinery', 'Machinery', 'Machinery', 'Machinery', 'Tech',
                     'Tech', 'Tech', 'Tech', 'Tech', 'Tech', 'Tech', 'Machinery', 'Machinery', 'Machinery', 'Tech',
                     'Tech', 'Tech', 'Tech', 'Tech', 'Tech', 'Machinery', 'Machinery', 'Machinery', 'Tech', 'Tech',
                     'Tech', 'Smart Car', 'Smart Car', 'Smart Car', 'CET', 'CET', 'CET', 'Tech', 'Tech', 'Tech',
                     'Machinery', 'LED', 'Autos', 'Autos', 'Autos', 'Smartwatch', 'Smartwatch', 'Smartwatch', 'Tech',
                     'Tech', 'Tech', 'Tech', 'Tech', 'Tech', 'Tech', 'Tech', 'Tech', 'Machinery', 'Machinery',
                     'Machinery', 'Autos', 'Autos', 'Autos', 'Autos', 'Autos', 'Autos', 'EF', 'EF', 'EF', 'CET', 'CET',
                     'CET', 'Autos', 'Autos', 'Autos', 'Autos', 'Autos', 'Autos', 'Autos', 'Autos', 'Autos',
                     'Indonesia', 'Indonesia', 'Indonesia', 'Autos', 'Autos', 'Autos', 'India', 'India', 'India',
                     'Indonesia', 'Indonesia', 'Indonesia', 'TSE', 'TSE', 'Autos', 'Autos', 'Autos', 'Abe', 'Retail',
                     'Retail', 'Retail', 'Index', 'Maglev', 'Index', 'Retail', 'Retail', 'Retail', 'Gamesoft',
                     'Gamesoft', 'Gamesoft', 'Retail', 'Retail', 'Retail', 'Tech', 'Tech', 'Tech', 'Tech', 'Tech',
                     'Tech', 'Tech', 'Tech', 'Tech', 'Childcare', 'Childcare', 'Childcare', 'Commodities',
                     'Commodities', 'Commodities', 'Tourist', 'Commodities', 'Commodities', 'Commodities', 'Retail',
                     'Retail', 'Retail', 'Commodities', 'Commodities', 'Commodities', 'Abe', 'Abe', 'Smartwatch',
                     'Smartwatch', 'Smartwatch', 'Hydro', 'Hydro', 'Hydro', 'Abe', 'Abe', 'ISS', 'ISS', 'Tourist',
                     'Tourist', 'Consumer Finance', 'Consumer Finance', 'Consumer Finance', 'ISS', 'ISS', 'ISS',
                     'Venture', 'Venture', 'Venture', 'Blackfield', 'Blackfield', 'Blackfield', 'Blackfield',
                     'Blackfield', 'Blackfield', 'Blackfield', 'Blackfield', 'Blackfield', 'Real Estate', 'Real Estate',
                     'Real Estate', 'Real Estate', 'Real Estate', 'Real Estate', 'EF', 'EF', 'EF', 'REIT', 'REIT',
                     'REIT', 'S-Event', 'S-Event', 'S-Event', 'REIT', 'REIT', 'REIT', 'Tourist', 'Tourist', 'Tourist',
                     'Railways', 'Railways', 'Railways', 'Trucking', 'Trucking', 'Trucking', 'Shipping', 'Shipping',
                     'Shipping', 'Shipping', 'Shipping', 'Shipping', 'Shipping', 'Shipping', 'Shipping', 'Airlines',
                     'Airlines', 'Airlines', 'Airlines', 'Airlines', 'Airlines', 'Airlines', 'Airlines', 'Airlines',
                     'MVNO', 'MVNO', 'MVNO', 'Telecom', 'Telecom', 'Telecom', 'Gamesoft', 'Gamesoft', 'Gamesoft',
                     'Tourist', 'Tourist', 'Tourist', 'Abe', 'Abe', 'EF', 'EF', 'Gamesoft', 'Gamesoft', 'Gamesoft',
                     'Tourist', 'Tourist', 'Tourist', 'Smart Car', 'Smart Car', 'Smart Car', 'Gamesoft', 'Gamesoft',
                     'Gamesoft', 'Healthcare', 'Healthcare', 'Index', 'Retail', 'Retail', 'Retail', 'Retail', 'Telecom',
                     'Telecom', 'Telecom', 'A-Tech', 'A-Tech', 'A-Tech', 'A-Internet Games', 'A-Internet Games',
                     'A-Internet Games', 'A-Properties', 'A-Properties', 'A-Properties', 'A-Shipping', 'A-Shipping',
                     'A-Shipping', 'A-Auto Parts', 'A-Auto Parts', 'A-Auto Parts', 'A-Water', 'A-Water', 'A-Water',
                     'A-Tech', 'A-Tech', 'A-Tech', 'A-Logistics', 'A-Logistics', 'A-Logistics', 'A-Elec Equipment',
                     'A-Elec Equipment', 'A-Elec Equipment', 'A-Machinery', 'A-Machinery', 'A-Machinery',
                     'A-Auto Parts', 'A-Auto Parts', 'A-Auto Parts', 'A-Auto OEM', 'A-Auto OEM', 'A-Auto OEM',
                     'A-Cement', 'A-Cement', 'A-Cement', 'A-Electronics', 'A-Electronics', 'A-Electronics',
                     'A-Textiles', 'A-Textiles', 'A-Textiles', 'A-Insurance', 'A-Insurance', 'A-Insurance', 'A-Tech',
                     'A-Tech', 'A-Tech', 'A-Auto OEM', 'A-Auto OEM', 'A-Auto OEM', 'A-Machinery', 'A-Machinery',
                     'A-Machinery', 'A-Shipping', 'A-Shipping', 'A-Shipping', 'A-Airlines', 'A-Airlines', 'A-Airlines',
                     'A-Tech', 'A-Tech', 'A-Tech', 'A-Tech', 'A-Tech', 'A-Tech', 'A-Tech', 'A-Tech', 'A-Tech',
                     'A-Utilities', 'A-Utilities', 'A-Utilities', 'A-Tech', 'A-Tech', 'A-Tech', 'A-Tech', 'A-Tech',
                     'A-Tech', 'A-Tech', 'A-Tech', 'A-Tech', 'A-Tech', 'A-Tech', 'A-Tech', 'A-Financials',
                     'A-Financials', 'A-Financials', 'A-Elec Equipment', 'A-Elec Equipment', 'A-Elec Equipment',
                     'A-Properties', 'A-Properties', 'A-Properties', 'A-Oil', 'A-Oil', 'A-Oil', 'A-Tech', 'A-Tech',
                     'A-Tech', 'A-Tech', 'A-Tech', 'A-Tech', 'A-Shipping', 'A-Shipping', 'A-Shipping', 'A-Construction',
                     'A-Construction', 'A-Construction', 'A-Oil', 'A-Oil', 'A-Oil', 'A-Retail', 'A-Retail', 'A-Retail',
                     'A-Tech', 'A-Tech', 'A-Tech', 'A-Water', 'A-Water', 'A-Water', 'A-Baba', 'A-Baba', 'A-Baba',
                     'A-Oil', 'A-Oil', 'A-Oil', 'A-Electronic Retail', 'A-Electronic Retail', 'A-Electronic Retail',
                     'A-Tech', 'A-Tech', 'A-Tech', 'A-Auto Parts', 'A-Auto Parts', 'A-Auto Parts', 'A-Financials',
                     'A-Financials', 'A-Financials', 'A-Water', 'A-Water', 'A-Water', 'A-Tech', 'A-Tech', 'A-Tech',
                     'A-Railway', 'A-Railway', 'A-Railway', 'A-Logistics', 'A-Logistics', 'A-Logistics', 'A-Solar',
                     'A-Solar', 'A-Solar', 'A-Airlines', 'A-Airlines', 'A-Airlines', 'A-Utilities', 'A-Utilities',
                     'A-Utilities', 'A-Network Equipment', 'A-Network Equipment', 'A-Network Equipment', 'A-Wind',
                     'A-Wind', 'A-Wind', 'A-Healthcare', 'A-Healthcare', 'A-Healthcare', 'A-Water', 'A-Water',
                     'A-Water', 'A-Utilities', 'A-Utilities', 'A-Utilities', 'Tech', 'Tech', 'Tech', 'A-Balance',
                     'A-Balance', 'A-Balance', 'Balance', 'Balance', 'Balance', 'Balance', 'Balance', 'Balance',
                     'Balance', 'Balance', 'Balance'],
        'sector': ['Tail', 'Tail', 'Tail', 'Vol', 'Vol', 'Vol', 'Tail', 'Tail', 'TailRR', 'TailRR', 'TailRR', 'TailRR',
                   'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'TailSens', 'TailSens', 'TailSens', 'Tail', 'Tail', 'Tail',
                   'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail',
                   'Tail', 'Tail', 'Tail', 'TailRR', 'TailRR', 'TailRR', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail',
                   'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail',
                   'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail',
                   'Tail', 'TailRR', 'TailRR', 'TailRR', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail',
                   'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'CH', 'CH', 'CH',
                   'CH', 'CH', 'CH', 'Tech', 'Tech', 'Tech', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail',
                   'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'CH', 'CH', 'CH', 'Tail', 'CH', 'CH', 'CH', 'Tail',
                   'Tail', 'Tail', 'Tail', 'TailSens', 'TailSens', 'TailSens', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail',
                   'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'TailSens', 'TailSens',
                   'TailSens', 'TailRR', 'TailRR', 'TailRR', 'TailRR', 'TailRR', 'TailRR', 'Tail', 'Tail', 'Tail',
                   'Tech', 'Tech', 'Tech', 'Tail', 'Tail', 'Tail', 'CM', 'CM', 'CM', 'CM', 'CM', 'CM', 'CM', 'CM', 'CM',
                   'Tech', 'Tech', 'Tech', 'CM', 'CM', 'CM', 'CM', 'CM', 'CM', 'CM', 'CM', 'CM', 'CM', 'CM', 'CM', 'CM',
                   'CM', 'CM', 'CM', 'CM', 'CM', 'CM', 'CM', 'CM', 'CM', 'CM', 'CM', 'CM', 'TailSens', 'Tail', 'TailRR',
                   'TailRR', 'TailRR', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail',
                   'MA', 'MA', 'MA', 'MA', 'MA', 'MA', 'Tail', 'MA', 'MA', 'MA', 'MA', 'MA', 'MA', 'MA', 'MA', 'MA',
                   'MA', 'MA', 'Tail', 'Tail', 'Tail', 'Tech', 'Tech', 'Tech', 'Tech', 'Tech', 'Tech', 'MA', 'MA', 'MA',
                   'TailRR', 'Tail', 'MA', 'MA', 'MA', 'Tech', 'Tech', 'Tech', 'Tech', 'Tech', 'Tech', 'Tail', 'MA',
                   'MA', 'MA', 'Tech', 'Tech', 'Tech', 'TailRR', 'TailRR', 'TailRR', 'TailRR', 'TailRR', 'TailRR',
                   'Tech', 'Tech', 'Tech', 'Tech', 'Tech', 'Tech', 'Tech', 'Tech', 'Tech', 'Tech', 'Tech', 'Tech',
                   'Tail', 'Tail', 'MA', 'MA', 'MA', 'Tech', 'Tech', 'Tech', 'Tech', 'Tech', 'Tech', 'Tech', 'Tech',
                   'Tech', 'Tech', 'Tech', 'Tech', 'MA', 'MA', 'MA', 'MA', 'MA', 'MA', 'MA', 'MA', 'MA', 'MA', 'MA',
                   'MA', 'MA', 'MA', 'MA', 'TailRR', 'TailRR', 'TailRR', 'MA', 'MA', 'MA', 'MA', 'MA', 'MA', 'MA', 'MA',
                   'MA', 'MA', 'MA', 'MA', 'MA', 'MA', 'MA', 'MA', 'MA', 'MA', 'TailRR', 'TailRR', 'Tail', 'Tail',
                   'Tail', 'Tail', 'TailRR', 'TailRR', 'TailRR', 'Tail', 'Tail', 'Tail', 'TailSens', 'TailSens',
                   'TailSens', 'Tail', 'Tail', 'Tail', 'TailRR', 'TailRR', 'TailRR', 'Tech', 'Tech', 'Tech', 'Tech',
                   'Tech', 'Tech', 'Tech', 'Tech', 'Tech', 'Tail', 'Tail', 'Tail', 'CM', 'CM', 'CM', 'Tail', 'CM', 'CM',
                   'CM', 'Tail', 'Tail', 'Tail', 'CM', 'CM', 'CM', 'Tail', 'Tail', 'Tech', 'Tech', 'Tech', 'Tail',
                   'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'TailSens', 'TailSens', 'TailSens',
                   'FIN', 'FIN', 'FIN', 'FIN', 'FIN', 'FIN', 'FIN', 'FIN', 'FIN', 'FIN', 'FIN', 'FIN', 'FIN', 'FIN',
                   'FIN', 'HR', 'HR', 'HR', 'HR', 'HR', 'HR', 'TailSens', 'TailSens', 'TailSens', 'Tail', 'Tail',
                   'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'TailRR', 'TailRR', 'TailRR', 'Tail', 'Tail',
                   'Tail', 'Tail', 'Tail', 'Tail', 'CM', 'CM', 'CM', 'CM', 'CM', 'CM', 'CH', 'CH', 'CH', 'Tail', 'Tail',
                   'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'TailRR', 'TailRR',
                   'TailRR', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'Tail', 'TailRR',
                   'TailRR', 'TailRR', 'TailSens', 'TailSens', 'TailSens', 'TailSens', 'TailSens', 'TailSens', 'TailRR',
                   'TailRR', 'TailRR', 'Tail', 'Tail', 'Tail', 'TailRR', 'HR', 'HR', 'HR', 'TailSens', 'TailSens',
                   'TailSens', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia',
                   'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia',
                   'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia',
                   'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia',
                   'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia',
                   'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia',
                   'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia',
                   'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia',
                   'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia',
                   'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia',
                   'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia',
                   'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia',
                   'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia',
                   'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia',
                   'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Asia', 'Tech', 'Tech',
                   'Tech', 'Asia', 'Asia', 'Asia', 'Index', 'Index', 'Index', 'Index', 'Index', 'Index', 'Index',
                   'Index', 'Index'],
        'GICS': ['Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary', 'Index', 'Index',
                 'Index', 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials',
                 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials',
                 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials',
                 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Consumer Staples',
                 'Consumer Staples', 'Consumer Staples', 'Consumer Staples', 'Consumer Staples', 'Consumer Staples',
                 'Information Technology', 'Information Technology', 'Information Technology', 'Health Care',
                 'Health Care', 'Health Care', 'Information Technology', 'Information Technology',
                 'Information Technology', 'Information Technology', 'Information Technology', 'Consumer Staples',
                 'Consumer Staples', 'Consumer Staples', 'Consumer Staples', 'Consumer Staples', 'Consumer Staples',
                 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary',
                 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary',
                 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Staples', 'Consumer Staples',
                 'Consumer Staples', 'Consumer Staples', 'Consumer Staples', 'Consumer Staples', 'Consumer Staples',
                 'Consumer Staples', 'Consumer Staples', 'Consumer Staples', 'Information Technology',
                 'Information Technology', 'Information Technology', 'Industrials', 'Financials', 'Financials',
                 'Financials', 'Financials', 'Financials', 'Financials', 'Financials', 'Consumer Discretionary',
                 'Consumer Discretionary', 'Consumer Discretionary', 'Financials', 'Financials', 'Financials',
                 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary',
                 'Materials', 'Materials', 'Materials', 'Materials', 'Materials', 'Materials', 'Information Technology',
                 'Information Technology', 'Information Technology', 'Financials', 'Financials', 'Financials',
                 'Information Technology', 'Information Technology', 'Information Technology', 'Information Technology',
                 'Information Technology', 'Information Technology', 'Information Technology', 'Materials', 'Materials',
                 'Materials', 'Materials', 'Materials', 'Materials', 'Materials', 'Materials', 'Materials', 'Materials',
                 'Consumer Discretionary', 'Consumer Staples', 'Consumer Staples', 'Consumer Staples', 'Health Care',
                 'Health Care', 'Health Care', 'Health Care', 'Health Care', 'Health Care', 'Health Care',
                 'Health Care', 'Health Care', 'Health Care', 'Health Care', 'Industrials', 'Industrials',
                 'Industrials', 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary',
                 'Information Technology', 'Information Technology', 'Information Technology', 'Industrials',
                 'Industrials', 'Industrials', 'Information Technology', 'Information Technology',
                 'Information Technology', 'Information Technology', 'Information Technology', 'Information Technology',
                 'Information Technology', 'Information Technology', 'Information Technology', 'Consumer Staples',
                 'Consumer Staples', 'Consumer Staples', 'Energy', 'Energy', 'Energy', 'Energy', 'Energy', 'Energy',
                 'Energy', 'Energy', 'Energy', 'Industrials', 'Industrials', 'Industrials', 'Materials', 'Materials',
                 'Materials', 'Materials', 'Materials', 'Materials', 'Materials', 'Materials', 'Materials', 'Materials',
                 'Materials', 'Materials', 'Materials', 'Materials', 'Materials', 'Materials', 'Materials', 'Materials',
                 'Materials', 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials',
                 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials',
                 'Industrials', 'Industrials', 'Information Technology', 'Information Technology',
                 'Information Technology', 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials',
                 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials',
                 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials',
                 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Consumer Discretionary',
                 'Consumer Discretionary', 'Consumer Discretionary', 'Industrials', 'Industrials', 'Industrials',
                 'Information Technology', 'Information Technology', 'Information Technology', 'Industrials',
                 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials',
                 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials',
                 'Industrials', 'Information Technology', 'Information Technology', 'Information Technology',
                 'Information Technology', 'Information Technology', 'Information Technology', 'Information Technology',
                 'Information Technology', 'Information Technology', 'Information Technology', 'Consumer Discretionary',
                 'Consumer Discretionary', 'Consumer Discretionary', 'Information Technology', 'Information Technology',
                 'Information Technology', 'Information Technology', 'Information Technology', 'Information Technology',
                 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary', 'Information Technology',
                 'Information Technology', 'Information Technology', 'Information Technology', 'Industrials',
                 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary',
                 'Consumer Discretionary', 'Consumer Discretionary', 'Information Technology', 'Information Technology',
                 'Information Technology', 'Information Technology', 'Information Technology', 'Information Technology',
                 'Information Technology', 'Information Technology', 'Information Technology', 'Industrials',
                 'Industrials', 'Industrials', 'Consumer Discretionary', 'Consumer Discretionary',
                 'Consumer Discretionary', 'Industrials', 'Industrials', 'Industrials', 'Consumer Discretionary',
                 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary',
                 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary',
                 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary',
                 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary',
                 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary',
                 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary',
                 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary',
                 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary', 'Industrials',
                 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Staples',
                 'Industrials', 'Information Technology', 'Consumer Discretionary', 'Consumer Discretionary',
                 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary',
                 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary',
                 'Consumer Discretionary', 'Consumer Discretionary', 'Information Technology', 'Information Technology',
                 'Information Technology', 'Information Technology', 'Information Technology', 'Information Technology',
                 'Consumer Staples', 'Consumer Staples', 'Consumer Staples', 'Industrials', 'Industrials',
                 'Industrials', 'Consumer Staples', 'Industrials', 'Industrials', 'Industrials', 'Consumer Staples',
                 'Consumer Staples', 'Consumer Staples', 'Industrials', 'Industrials', 'Industrials', 'Industrials',
                 'Industrials', 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary',
                 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Information Technology',
                 'Information Technology', 'Consumer Discretionary', 'Consumer Discretionary', 'Financials',
                 'Financials', 'Financials', 'Financials', 'Financials', 'Financials', 'Financials', 'Financials',
                 'Financials', 'Financials', 'Financials', 'Financials', 'Financials', 'Financials', 'Financials',
                 'Financials', 'Financials', 'Financials', 'Financials', 'Financials', 'Financials', 'Financials',
                 'Financials', 'Financials', 'Financials', 'Financials', 'Financials', 'Financials', 'Financials',
                 'Financials', 'Financials', 'Financials', 'Financials', 'Financials', 'Financials', 'Financials',
                 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials',
                 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials',
                 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials',
                 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials',
                 'Industrials', 'Industrials', 'Industrials', 'Telecommunication Services',
                 'Telecommunication Services', 'Telecommunication Services', 'Telecommunication Services',
                 'Telecommunication Services', 'Telecommunication Services', 'Consumer Discretionary',
                 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary',
                 'Consumer Discretionary', 'Financials', 'Financials', 'Information Technology',
                 'Information Technology', 'Information Technology', 'Information Technology', 'Information Technology',
                 'Industrials', 'Industrials', 'Industrials', 'Information Technology', 'Information Technology',
                 'Information Technology', 'Information Technology', 'Information Technology', 'Information Technology',
                 'Health Care', 'Health Care', 'Consumer Discretionary', 'Consumer Staples', 'Consumer Discretionary',
                 'Consumer Discretionary', 'Consumer Discretionary', 'Telecommunication Services',
                 'Telecommunication Services', 'Telecommunication Services', 'Information Technology',
                 'Information Technology', 'Information Technology', 'Information Technology', 'Information Technology',
                 'Information Technology', 'Financials', 'Financials', 'Financials', 'Industrials', 'Industrials',
                 'Industrials', 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary',
                 'Industrials', 'Industrials', 'Industrials', 'Information Technology', 'Information Technology',
                 'Information Technology', 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials',
                 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Consumer Discretionary',
                 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary',
                 'Consumer Discretionary', 'Materials', 'Materials', 'Materials', 'Information Technology',
                 'Information Technology', 'Information Technology', 'Consumer Discretionary', 'Consumer Discretionary',
                 'Consumer Discretionary', 'Financials', 'Financials', 'Financials', 'Information Technology',
                 'Information Technology', 'Information Technology', 'Consumer Discretionary', 'Consumer Discretionary',
                 'Consumer Discretionary', 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials',
                 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Information Technology',
                 'Information Technology', 'Information Technology', 'Information Technology', 'Information Technology',
                 'Information Technology', 'Information Technology', 'Information Technology', 'Information Technology',
                 'Utilities', 'Utilities', 'Utilities', 'Information Technology', 'Information Technology',
                 'Information Technology', 'Information Technology', 'Information Technology', 'Information Technology',
                 'Information Technology', 'Information Technology', 'Information Technology', 'Information Technology',
                 'Information Technology', 'Information Technology', 'Industrials', 'Industrials', 'Industrials',
                 'Industrials', 'Industrials', 'Industrials', 'Financials', 'Financials', 'Financials', 'Utilities',
                 'Utilities', 'Utilities', 'Information Technology', 'Information Technology', 'Information Technology',
                 'Information Technology', 'Information Technology', 'Information Technology', 'Industrials',
                 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Energy', 'Energy',
                 'Energy', 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary',
                 'Information Technology', 'Information Technology', 'Information Technology', 'Utilities', 'Utilities',
                 'Utilities', 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary', 'Energy',
                 'Energy', 'Energy', 'Consumer Discretionary', 'Consumer Discretionary', 'Consumer Discretionary',
                 'Information Technology', 'Information Technology', 'Information Technology', 'Consumer Discretionary',
                 'Consumer Discretionary', 'Consumer Discretionary', 'Financials', 'Financials', 'Financials',
                 'Utilities', 'Utilities', 'Utilities', 'Information Technology', 'Information Technology',
                 'Information Technology', 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials',
                 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials', 'Industrials',
                 'Industrials', 'Utilities', 'Utilities', 'Utilities', 'Information Technology',
                 'Information Technology', 'Information Technology', 'Utilities', 'Utilities', 'Utilities',
                 'Health Care', 'Health Care', 'Health Care', 'Utilities', 'Utilities', 'Utilities', 'Utilities',
                 'Utilities', 'Utilities', 'Information Technology', 'Information Technology', 'Information Technology',
                 'Index', 'Index', 'Index', 'Index', 'Index', 'Index', 'Index', 'Index', 'Index', 'Index', 'Index',
                 'Index'],
        'TOPIX': ['Construction', 'Construction', 'Construction', 'Index', 'Index', 'Index', 'Construction',
                  'Construction', 'Construction', 'Construction', 'Construction', 'Construction', 'Construction',
                  'Construction', 'Construction', 'Construction', 'Construction', 'Construction', 'Construction',
                  'Construction', 'Construction', 'Construction', 'Construction', 'Construction', 'Construction',
                  'Construction', 'Construction', 'Construction', 'Construction', 'Foods', 'Foods', 'Foods', 'Foods',
                  'Foods', 'Foods', 'Information  and  Communication', 'Information  and  Communication',
                  'Information  and  Communication', 'Services', 'Services', 'Services', 'Services', 'Services',
                  'Services', 'Services', 'Services', 'Foods', 'Foods', 'Foods', 'Foods', 'Foods', 'Foods',
                  'Retail Trade', 'Retail Trade', 'Retail Trade', 'Retail Trade', 'Retail Trade', 'Other Products',
                  'Other Products', 'Other Products', 'Retail Trade', 'Wholesale Trade', 'Foods', 'Foods', 'Foods',
                  'Foods', 'Foods', 'Foods', 'Foods', 'Foods', 'Foods', 'Foods', 'Wholesale Trade', 'Wholesale Trade',
                  'Wholesale Trade', 'Wholesale Trade', 'Real Estate', 'Real Estate', 'Real Estate', 'Real Estate',
                  'Real Estate', 'Real Estate', 'Real Estate', 'Real Estate', 'Real Estate', 'Real Estate',
                  'Real Estate', 'Real Estate', 'Real Estate', 'Retail Trade', 'Retail Trade', 'Retail Trade',
                  'Retail Trade', 'Textiles  and  Apparel', 'Textiles  and  Apparel', 'Textiles  and  Apparel',
                  'Textiles  and  Apparel', 'Textiles  and  Apparel', 'Textiles  and  Apparel', 'Metal Products',
                  'Metal Products', 'Metal Products', 'Real Estate', 'Real Estate', 'Real Estate',
                  'Information  and  Communication', 'Information  and  Communication',
                  'Information  and  Communication', 'Information  and  Communication',
                  'Information  and  Communication', 'Information  and  Communication',
                  'Information  and  Communication', 'Pulp  and  Paper', 'Pulp  and  Paper', 'Pulp  and  Paper',
                  'Chemicals', 'Chemicals', 'Chemicals', 'Chemicals', 'Chemicals', 'Chemicals', 'Chemicals', 'Services',
                  'Chemicals', 'Chemicals', 'Chemicals', 'Pharmaceutical', 'Pharmaceutical', 'Pharmaceutical',
                  'Pharmaceutical', 'Pharmaceutical', 'Pharmaceutical', 'Pharmaceutical', 'Pharmaceutical',
                  'Pharmaceutical', 'Pharmaceutical', 'Pharmaceutical', 'Services', 'Services', 'Services', 'Services',
                  'Services', 'Services', 'Information  and  Communication', 'Information  and  Communication',
                  'Information  and  Communication', 'Services', 'Services', 'Services',
                  'Information  and  Communication', 'Information  and  Communication',
                  'Information  and  Communication', 'Information  and  Communication',
                  'Information  and  Communication', 'Information  and  Communication', 'Chemicals', 'Chemicals',
                  'Chemicals', 'Chemicals', 'Chemicals', 'Chemicals', 'Oil  and  Coal Products',
                  'Oil  and  Coal Products', 'Oil  and  Coal Products', 'Oil  and  Coal Products',
                  'Oil  and  Coal Products', 'Oil  and  Coal Products', 'Oil  and  Coal Products',
                  'Oil  and  Coal Products', 'Oil  and  Coal Products', 'Glass  and  Ceramics Products',
                  'Glass  and  Ceramics Products', 'Glass  and  Ceramics Products', 'Iron  and  Steel',
                  'Iron  and  Steel', 'Iron  and  Steel', 'Iron  and  Steel', 'Iron  and  Steel', 'Iron  and  Steel',
                  'Iron  and  Steel', 'Iron  and  Steel', 'Iron  and  Steel', 'Iron  and  Steel', 'Iron  and  Steel',
                  'Iron  and  Steel', 'Nonferrous Metals', 'Nonferrous Metals', 'Nonferrous Metals',
                  'Nonferrous Metals', 'Nonferrous Metals', 'Nonferrous Metals', 'Nonferrous Metals',
                  'Nonferrous Metals', 'Nonferrous Metals', 'Nonferrous Metals', 'Nonferrous Metals',
                  'Nonferrous Metals', 'Nonferrous Metals', 'Metal Products', 'Metal Products', 'Metal Products',
                  'Metal Products', 'Metal Products', 'Metal Products', 'Metal Products', 'Metal Products', 'Services',
                  'Services', 'Services', 'Services', 'Services', 'Services', 'Services', 'Machinery', 'Machinery',
                  'Machinery', 'Machinery', 'Machinery', 'Machinery', 'Machinery', 'Machinery', 'Machinery',
                  'Machinery', 'Machinery', 'Machinery', 'Construction', 'Construction', 'Construction', 'Machinery',
                  'Machinery', 'Machinery', 'Machinery', 'Machinery', 'Machinery', 'Electric Appliances',
                  'Electric Appliances', 'Electric Appliances', 'Electric Appliances', 'Electric Appliances',
                  'Electric Appliances', 'Electric Appliances', 'Electric Appliances', 'Electric Appliances',
                  'Electric Appliances', 'Machinery', 'Machinery', 'Machinery', 'Machinery', 'Electric Appliances',
                  'Electric Appliances', 'Electric Appliances', 'Electric Appliances', 'Electric Appliances',
                  'Electric Appliances', 'Electric Appliances', 'Electric Appliances', 'Electric Appliances',
                  'Electric Appliances', 'Electric Appliances', 'Electric Appliances', 'Electric Appliances',
                  'Electric Appliances', 'Electric Appliances', 'Electric Appliances', 'Electric Appliances',
                  'Electric Appliances', 'Electric Appliances', 'Electric Appliances', 'Electric Appliances',
                  'Electric Appliances', 'Electric Appliances', 'Electric Appliances', 'Electric Appliances',
                  'Electric Appliances', 'Electric Appliances', 'Electric Appliances', 'Electric Appliances',
                  'Electric Appliances', 'Electric Appliances', 'Electric Appliances', 'Electric Appliances',
                  'Transportation Equipment', 'Transportation Equipment', 'Transportation Equipment',
                  'Electric Appliances', 'Electric Appliances', 'Electric Appliances', 'Electric Appliances',
                  'Electric Appliances', 'Electric Appliances', 'Electric Appliances', 'Electric Appliances',
                  'Electric Appliances', 'Electric Appliances', 'Electric Appliances', 'Electric Appliances',
                  'Transportation Equipment', 'Transportation Equipment', 'Transportation Equipment',
                  'Transportation Equipment', 'Transportation Equipment', 'Transportation Equipment',
                  'Transportation Equipment', 'Transportation Equipment', 'Transportation Equipment',
                  'Transportation Equipment', 'Transportation Equipment', 'Transportation Equipment',
                  'Transportation Equipment', 'Transportation Equipment', 'Transportation Equipment',
                  'Transportation Equipment', 'Transportation Equipment', 'Transportation Equipment',
                  'Transportation Equipment', 'Transportation Equipment', 'Transportation Equipment',
                  'Transportation Equipment', 'Transportation Equipment', 'Transportation Equipment',
                  'Transportation Equipment', 'Transportation Equipment', 'Transportation Equipment',
                  'Transportation Equipment', 'Transportation Equipment', 'Transportation Equipment',
                  'Transportation Equipment', 'Transportation Equipment', 'Transportation Equipment',
                  'Transportation Equipment', 'Transportation Equipment', 'Transportation Equipment',
                  'Transportation Equipment', 'Transportation Equipment', 'Transportation Equipment',
                  'Transportation Equipment', 'Transportation Equipment', 'Wholesale Trade', 'Retail Trade',
                  'Retail Trade', 'Retail Trade', 'Retail Trade', 'Wholesale Trade', 'Wholesale Trade', 'Retail Trade',
                  'Retail Trade', 'Retail Trade', 'Wholesale Trade', 'Wholesale Trade', 'Wholesale Trade',
                  'Retail Trade', 'Retail Trade', 'Retail Trade', 'Precision Instruments', 'Precision Instruments',
                  'Precision Instruments', 'Precision Instruments', 'Precision Instruments', 'Precision Instruments',
                  'Electric Appliances', 'Electric Appliances', 'Electric Appliances', 'Other Products',
                  'Other Products', 'Other Products', 'Wholesale Trade', 'Wholesale Trade', 'Wholesale Trade',
                  'Wholesale Trade', 'Wholesale Trade', 'Wholesale Trade', 'Wholesale Trade', 'Retail Trade',
                  'Retail Trade', 'Retail Trade', 'Wholesale Trade', 'Wholesale Trade', 'Wholesale Trade',
                  'Wholesale Trade', 'Wholesale Trade', 'Precision Instruments', 'Precision Instruments',
                  'Precision Instruments', 'Wholesale Trade', 'Wholesale Trade', 'Wholesale Trade', 'Wholesale Trade',
                  'Wholesale Trade', 'Wholesale Trade', 'Wholesale Trade', 'Retail Trade', 'Retail Trade',
                  'Other Financing Business', 'Other Financing Business', 'Other Financing Business', 'Banks', 'Banks',
                  'Banks', 'Securities  and  Commodity Futures', 'Securities  and  Commodity Futures',
                  'Securities  and  Commodity Futures', 'Securities  and  Commodity Futures',
                  'Securities  and  Commodity Futures', 'Securities  and  Commodity Futures',
                  'Securities  and  Commodity Futures', 'Securities  and  Commodity Futures',
                  'Securities  and  Commodity Futures', 'Other Financing Business', 'Other Financing Business',
                  'Other Financing Business', 'Real Estate', 'Real Estate', 'Real Estate', 'Real Estate', 'Real Estate',
                  'Real Estate', 'Real Estate', 'Real Estate', 'Real Estate', 'Real Estate', 'Real Estate',
                  'Real Estate', 'Real Estate', 'Real Estate', 'Real Estate', 'Real Estate', 'Real Estate',
                  'Real Estate', 'Land Transportation', 'Land Transportation', 'Land Transportation',
                  'Land Transportation', 'Land Transportation', 'Land Transportation', 'Land Transportation',
                  'Land Transportation', 'Land Transportation', 'Marine Transportation', 'Marine Transportation',
                  'Marine Transportation', 'Marine Transportation', 'Marine Transportation', 'Marine Transportation',
                  'Marine Transportation', 'Marine Transportation', 'Marine Transportation', 'Air Transportation',
                  'Air Transportation', 'Air Transportation', 'Air Transportation', 'Air Transportation',
                  'Air Transportation', 'Air Transportation', 'Air Transportation', 'Air Transportation',
                  'Information  and  Communication', 'Information  and  Communication',
                  'Information  and  Communication', 'Information  and  Communication',
                  'Information  and  Communication', 'Information  and  Communication',
                  'Information  and  Communication', 'Information  and  Communication',
                  'Information  and  Communication', 'Services', 'Services', 'Services', 'Services', 'Services',
                  'Information  and  Communication', 'Information  and  Communication',
                  'Information  and  Communication', 'Information  and  Communication',
                  'Information  and  Communication', 'Real Estate', 'Real Estate', 'Real Estate',
                  'Information  and  Communication', 'Information  and  Communication',
                  'Information  and  Communication', 'Information  and  Communication',
                  'Information  and  Communication', 'Information  and  Communication', 'Services', 'Services',
                  'Wholesale Trade', 'Retail Trade', 'Retail Trade', 'Retail Trade', 'Retail Trade',
                  'Information  and  Communication', 'Information  and  Communication',
                  'Information  and  Communication', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan',
                  'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan',
                  'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan',
                  'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan',
                  'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan',
                  'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan',
                  'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan',
                  'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan',
                  'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan',
                  'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan',
                  'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan',
                  'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan',
                  'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan',
                  'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan',
                  'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan',
                  'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan',
                  'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan',
                  'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan',
                  'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan',
                  'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan',
                  'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan',
                  'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan',
                  'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan',
                  'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan',
                  'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan',
                  'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan', 'Non-Japan',
                  'Non-Japan', 'Non-Japan', 'Non-Japan', 'Index', 'Index', 'Index', 'Index', 'Index', 'Index', 'Index',
                  'Index', 'Index']
    }
    df20141231 = DataFrame(trade20141231, index=pd.DatetimeIndex(trade20141231['tradeDate'])).drop('tradeDate', 1)

    # concat with data in Access DB
    turnoverDf = pd.concat([df20141231, sqlTurnoverDf])

    # merge with FX df to get to-JPY-fx rate
    mergedDf = turnoverDf.merge(sqlFxDf, left_on='currencyCode', right_index=True).sort_index()
    # create new column which contain turnover in JPY
    mergedDf['JPYPL'] = (mergedDf['Turnover'] * mergedDf['AvgOfrate']).values

    # calculate total turnover for each side
    totalTurnover = mergedDf.truncate(after=g.endDate).groupby(["side"]).sum()['JPYPL']

    # calculate turnover for each advisor
    sumTurnoverPerAdv = mergedDf.truncate(after=g.endDate).groupby(["advisor", "side"]).sum()['JPYPL'].unstack()

    totalRatio = sumTurnoverPerAdv * 100 / totalTurnover['L']  # % TOTAL

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
    namesDf = fExposureDf.groupby(by=['processDate', 'advisor']).count()['quick']

    mfExposureDf = fExposureDf.merge(codeBetaDf, how='left', left_on='quick', right_on='code')
    sumExposureDf = mfExposureDf.groupby(['processDate', 'advisor', 'side']).sum()[
        ['RHExposure', 'YAExposure', 'LRExposure']]

    temp2 = mfExposureDf.set_index(['processDate', 'advisor', 'quick', 'side'])
    temp2 = mfExposureDf.set_index(['processDate', 'advisor', 'side'])
    temp2
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
    sqlPlDf = sql.read_sql('''SELECT processDate,advisor, side, quick, attribution, name,
                          RHAttribution AS RHAttr,
                          YAAttribution AS YAAttr,
                          LRAttribution AS LRAttr, GICS, TPX,strategy
                          FROM `t05PortfolioResponsibilities`
                          WHERE processDate >= '%s' AND processDate < '%s'
                          AND advisor <> ''
                          ;''' % (g.fromDate, g.endDate), g.con, coerce_float=True,
                           parse_dates=['processDate'])  # ,index_col = 'processDate')

    t = sqlPlDf.groupby(['processDate', 'advisor', 'side']).sum().drop(['RHAttr', 'YAAttr', 'LRAttr'],
                                                                        axis=1).unstack().reset_index().set_index('processDate')
    attr_df = t[t['advisor'] == g.reportAdvisor]['attribution']
    cs_attr_df = attr_df
    cs_attr_df.ix[g.fromDate] = 0
    cs_attr_df = cs_attr_df.cumsum()

    long_short_return = sqlPlDf.groupby(["advisor", "side"]).sum().drop(['RHAttr', 'YAAttr', 'LRAttr'],
                                                axis=1).unstack().div(sumTurnoverPerAdv, axis=0)*100

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
    # csAttr_df['Total'].head()
    netOP = DataFrame()
    netOP['L'] = attr_df['L'].sub(tExposure['L'].mul(indexReturn[g.indexMapping[g.reportAdvisor]], axis=0), axis=0).div(
            aumDf.shift(1)['Total'], axis=0)
    netOP['S'] = attr_df['S'].sub((tExposure['S'] * -1).mul(indexReturn[g.indexMapping[g.reportAdvisor]], axis=0),
                                 axis=0).div(aumDf.shift(1)['Total'], axis=0)
    netOP.ix[g.fromDate] = 0
    netOP = netOP.cumsum()

    btExposure = betaExposure[:, g.reportAdvisor].unstack().shift(1)
    betaOP = DataFrame()
    betaOP['L'] = attr_df['L'].sub(btExposure['L'].mul(indexReturn[g.indexMapping[g.reportAdvisor]], axis=0),
                                  axis=0).div(aumDf.shift(1)['Total'], axis=0)
    betaOP['S'] = attr_df['S'].sub((btExposure['S'] * -1).mul(indexReturn[g.indexMapping[g.reportAdvisor]], axis=0),
                                  axis=0).div(aumDf.shift(1)['Total'], axis=0)
    betaOP = betaOP.cumsum()

    paramCsAttrDf = dict()
    paramCsAttrDf['index'] = [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in cs_attr_df.index]
    paramCsAttrDf['columns'] = {col: cs_attr_df[col].values.tolist() for col in cs_attr_df.columns}

    render_obj = dict()
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
    render_obj['longIndexOP'] = netOP['L'].iloc[-1] * 100
    render_obj['shortIndexOP'] = netOP['S'].iloc[-1] * 100
    render_obj['longBetaOP'] = betaOP['L'].iloc[-1] * 100
    render_obj['shortBetaOP'] = betaOP['S'].iloc[-1] * 100
    render_obj['longHitRate'] = hitRateDf['LongsHR'].ix[g.reportAdvisor]
    render_obj['shortHitRate'] = hitRateDf['ShortsHR'].ix[g.reportAdvisor]
    render_obj['longReturn']   = long_short_return['attribution']['L'].ix[g.reportAdvisor]
    render_obj['shortReturn']  = long_short_return['attribution']['S'].ix[g.reportAdvisor]
    #renderObj['test'] = long_short_return

    return render_template('attrib.html', params=render_obj, csAttrDf=paramCsAttrDf, test=cs_attr_df)


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
