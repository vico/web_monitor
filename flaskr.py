# all the imports
import sqlite3
from flask import Flask, request, session, g, redirect, url_for, \
    abort, render_template, flash, Response
from contextlib import closing

import pandas as pd
from pandas import DataFrame
from pandas.io import sql
import numpy as np
import pymysql
from datetime import datetime, timedelta
from decimal import *
from flask.ext.cache import Cache

import urlparse
import pyotp
import qrcode
import StringIO

# configuration
DATABASE = '/tmp/flaskr.db'
DEBUG = True
SECRET_KEY = 'development key'
USERNAME = 'admin'
PASSWORD = 'default'
TIMEOUT = 15 * 60
NUMBER_OF_ROW_PER_PAGE = 41
NUMBER_OF_TOP_POSITIONS = 8

# create our little application :)
app = Flask(__name__)
app.config.from_object(__name__)

cache = Cache(app, config={'CACHE_TYPE': 'simple'})


def connect_db():
    return sqlite3.connect(app.config['DATABASE'])


def init_db():
    with closing(connect_db()) as db:
        with app.open_resource('schema.sql', mode='r') as f:
            db.cursor().executescript(f.read())
        db.commit()


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
                table_html += '<tr>' + ''.join(['<th>' + elements[0] + '</th>'] + [
                    '<td>' + '{:.2f}%'.format(float(h) * 100) + '</td>' for h in elements[1:4]] + [
                                                   '<td>' + ('0' if h == '' else '{:,.0f}'.format(float(h))) + '</td>' for h in
                                                   elements[4:7]] + [
                                                   '<td>' + ('0' if h == '' else '{:.1f}%'.format(float(h) * 100)) + '</td>' for h in
                                                   elements[7:]
                                                   ]) + '</tr>'
            elif table_type == 'ranking':
                table_html += '<tr>' + ''.join(['<th>' + elements[0] + '</th>'] + [
                    '<td>' + h + '</td>' for h in elements[1:-2]] + [
                                                   '<td>' + ('0' if h == '' else '{:,.0f}'.format(float(h)))+'</td>' for h in elements[-2:-1]
                                                   ] + [
                    '<td>' + h + '</td>' for h in elements[-1:]
                ]
                                               ) + '</tr>'

            total_rows += 1
        count += 1
    table_html += '</tbody></table>'
    if finalize_last_page:
        table_html += '</section>'
    return table_html, count


def add_to_page(df, current_page, remaining_row_number, table_type='summary', table_caption=''):
    return_html, r_count = break_into_page(df, start_new_page=False, finalize_last_page=False,
                                           number_of_row_to_break_first_page=remaining_row_number,
                                           table_type=table_type, table_caption=table_caption)
    if r_count < remaining_row_number:
        remaining_row_number -= r_count
    else:
        return_html, r_count = break_into_page(df, start_new_page=True, finalize_last_page=False,
                                               number_of_row_to_break_first_page=NUMBER_OF_ROW_PER_PAGE,
                                               table_type=table_type, table_caption=table_caption)
        remaining_row_number = NUMBER_OF_ROW_PER_PAGE - r_count

    return current_page + return_html, remaining_row_number


@app.before_request
def before_request():
    g.db = connect_db()
    g.con = pymysql.connect(host='127.0.0.1', user='root', passwd='root', db='hkg02p')
    g.startDate = datetime(datetime.now().year-1, 12, 31).strftime('%Y-%m-%d')
    app.logger.debug(g.startDate) #'2014-12-31'  # not include
    g.endDate = datetime.now().strftime('%Y-%m-%d')#'2015-12-01'  # not include
    g.reportAdvisor = 'AP'
    g.lineWidth = 3
    g.thinLineWidth = 2
    g.indexMapping = {
        'AP': 'TPX',
        'CS': 'TPX',
        'SM': 'TPX',
        'HA': 'TPX',
        'PK': 'TPX',
        'RW': 'TPX',
        'SJ': 'TPX',
        'TNi': 'TPX',
        'TI': 'TPX',
        'TT': 'TPX',
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
            flash('You were logged in')
            #return redirect(url_for('attrib'))
            return redirect(url_for('enable_tfa_via_app'))

    return render_template('login.html', error=error)


@app.route('/enable-tfa-via-app')
def enable_tfa_via_app():
    if request.method == 'GET':
        return render_template('enable_tfa_via_app.html')
    token = request.form['token']
    if token:
        pass


@app.route('/auth-qr-code.png')
def auth_qr_code():
    """generate a QR code with the users TOTP secret

    We do this to reduce the risk of leaking
    the secret over the wire in plaintext"""
    #FIXME: This logic should really apply site-wide
    domain = urlparse.urlparse(request.url).netloc

    secret = pyotp.random_base32()

    totp = pyotp.TOTP(secret)

    if not domain:
        domain = 'example.com'
    username = "%s@%s" % (app.config['USERNAME'], domain)

    uri = totp.provisioning_uri(username)
    qrc = qrcode.make(uri)

    stream = StringIO.StringIO()
    qrc.save(stream)
    image = stream.getvalue()
    return Response(image, mimetype='image/png')


@cache.memoize(TIMEOUT)
def get_turnover_df(from_date, end_date):
    # there is some trades on 2014/12/31 both long and short sides, which is not in database table
    sql_turnover_df = sql.read_sql('''
        SELECT aa.tradeDate, aa.code,
        aa.currencyCode, aa.side,
        ABS(Notl) AS Turnover, e.advisor, e.strategy, e.sector,
        f.value AS GICS,
        IF(g.value IS NOT NULL, g.value, 'Non-Japan') AS TOPIX,
        aa.firstTradeDate,
        aa.MktCap
        FROM (
        SELECT b.code,
        d.currencyCode,
        b.side,
        IF(orderType="B",1,-1)*b.quantity AS Qty,
        IF(orderType="B",-1,1)*b.net AS Notl,
        MAX(a.adviseDate) AS `MaxOfDate`,
        b.reconcileID,
        b.tradeDate,
        b.equityType,
        c.instrumentType,
        c.instrumentID,
        b.orderType,
        IF (z.side='L', firstTradeDateLong, firstTradeDateShort) AS firstTradeDate,
        IF(c.instrumentType <> 'EQ', 'Index', IF(h.value*j.rate < 250000000,'Micro',
                        IF(h.value*j.rate <1000000000, 'Small', IF(h.value*j.rate <5000000000, 'Mid', IF(h.value*j.rate <20000000000, 'Large', IF(h.value IS NULL, 'Micro','Mega'))) ))) AS MktCap
        FROM t08AdvisorTag a
        INNER JOIN t08Reconcile b ON a.code = b.code
        INNER JOIN t01Instrument c ON (b.equityType = c.instrumentType) AND (b.code = c.quick)
        INNER JOIN t02Currency d ON c.currencyID = d.currencyID
        INNER JOIN `t06DailyCrossRate` j ON j.priceDate = b.processDate AND j.base=d.currencyCode AND j.quote='USD'
        LEFT JOIN t05PortfolioResponsibilities z ON z.instrumentID = c.instrumentID AND z.processDate = b.processDate
        LEFT JOIN t06DailyBBStaticSnapshot h ON c.instrumentID = h.instrumentID AND h.dataType = 'CUR_MKT_CAP'
        WHERE a.adviseDate<= b.processDate
            AND b.processDate >= '%s' AND b.processDate < '%s' # Grab Analyst Trades start date
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
         ''' % (from_date, end_date), g.con, parse_dates=['tradeDate'], coerce_float=True, index_col='tradeDate')

    if datetime.strptime(from_date, '%Y-%m-%d') <= datetime(2014,12,31):
        # TODO: update exposure df for 2016, specifically MktCap information
        df20141231 = pd.read_csv('turnover20141231.csv', index_col=0, parse_dates=0)
        # concat with data in Access DB
        turnover_df = pd.concat([df20141231, sql_turnover_df])
    else:
        turnover_df = sql_turnover_df

    return turnover_df


@cache.memoize(TIMEOUT)
def get_hit_rate_df(from_date, end_date):
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
    pl_df = sql.read_sql('''SELECT processDate,advisor, side, a.quick, attribution,
                            RHAttribution AS RHAttr,
                            YAAttribution AS YAAttr,
                            LRAttribution AS LRAttr, GICS, TPX,strategy, firstTradeDateLong, firstTradeDateShort,
                            IF(c.instrumentType <> 'EQ', 'Index', IF(d.value*b.rate < 250000000,'Micro',
                                                IF(d.value*b.rate <1000000000, 'Small',
                                                IF(d.value*b.rate <5000000000, 'Mid',
                                                IF(d.value*b.rate <20000000000, 'Large',
                                                IF(d.value IS NULL, 'Micro','Mega'))) ))) AS MktCap
                FROM `t05PortfolioResponsibilities` a
                INNER JOIN `t06DailyCrossRate` b ON a.processDate=b.priceDate AND a.CCY=b.base AND b.quote='USD'
                INNER JOIN t01Instrument c ON c.instrumentID = a.instrumentID
                LEFT JOIN t06DailyBBStaticSnapshot d ON d.instrumentID = a.instrumentID AND d.dataType = 'CUR_MKT_CAP'
                WHERE processDate > '%s' AND processDate < '%s'
                AND advisor <> ''
                AND a.quick NOT LIKE "DIV%%"
                AND a.quick NOT LIKE "FX%%"
                          ;''' % (from_date, end_date), g.con, coerce_float=True, parse_dates=['processDate'])
    return pl_df


@cache.memoize(TIMEOUT)
def get_fx_df(from_date, end_date):
    fx_df = sql.read_sql('''SELECT a.base, AVG(a.rate) AS AvgOfrate
                FROM (
                SELECT t06DailyCrossRate.priceDate, t06DailyCrossRate.base, t06DailyCrossRate.quote, t06DailyCrossRate.Rate
                FROM t06DailyCrossRate
                WHERE priceDate> '%s' AND priceDate < '%s' AND QUOTE="JPY"
                ) a
                GROUP BY a.base;
                ''' % (from_date, end_date), g.con, coerce_float=True, index_col='base')
    return fx_df


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
    code_beta_df = sql.read_sql('''SELECT a.code, a.beta, a.sector
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
def get_index_return(from_date, end_date):
    index_df = sql.read_sql('''SELECT b.priceDate, a.indexCode, b.close
      FROM `t07Index` a, `t06DailyIndex` b
      WHERE a.indexID = b.indexID
      AND b.priceDate >= '%s' AND b.priceDate < '%s'
      AND a.indexCode IN ('TPX','KOSPI','TWSE','HSCEI')
      ;''' % (from_date, end_date), g.con, coerce_float=True, parse_dates=['priceDate'])
    pIndexDf = index_df.pivot('priceDate', 'indexCode', 'close')
    indexReturn = pIndexDf / pIndexDf.shift(1) - 1
    return indexReturn, pIndexDf


@cache.memoize(TIMEOUT)
def get_code_name_map():
    code_name_map = sql.read_sql('''SELECT quick, name FROM t01Instrument;''', g.con)
    return code_name_map


@app.route('/', methods=['GET'])
def attrib():
    if not session.get('logged_in'):
        #abort(401)
        return redirect(url_for('login'))
    # TODO: change all double quotes to single quote for consistence
    # TODO: verify cache invalidate
    # TODO: test Redis as cache backend
    # TODO: find a good way to reduce first access turn around time
    # TODO: add email verification, Clef as 2 factor authentication
    # TODO: add send PDFs to specified email
    # TODO: add checker view (should have pie graph to see what is missing)
    # TODO: add 1 year summary attribution page
    # TODO: switch to use scale from table
    param_adviser = request.args.get('analyst', g.reportAdvisor)
    start_date = request.args.get('startDate', g.startDate)
    end_date = request.args.get('endDate', g.endDate)
    nticks = len(pd.date_range(start_date, end_date, freq='BM'))

    code_name_map = get_code_name_map()

    hitRateDf = get_hit_rate_df(start_date, end_date)

    sqlFxDf = get_fx_df(start_date, end_date)

    turnover_df = get_turnover_df(start_date, end_date)

    # merge with FX df to get to-JPY-fx rate
    turnover_merged_df = turnover_df.merge(sqlFxDf, left_on='currencyCode', right_index=True).sort_index()
    # create new column which contain turnover in JPY
    turnover_merged_df['JPYPL'] = (turnover_merged_df['Turnover'] * turnover_merged_df['AvgOfrate']).values

    # calculate total turnover for each side
    total_turnover = turnover_merged_df.truncate(after=end_date).groupby(["side"]).sum()['JPYPL']

    # calculate turnover for each advisor
    sumTurnoverPerAdv = turnover_merged_df.truncate(after=end_date).groupby(["advisor", "side"]).sum()[
        'JPYPL'].unstack()

    sumTurnoverPerAdv = sumTurnoverPerAdv.reindex(g.indexMapping.keys())

    totalRatio = (sumTurnoverPerAdv * 100 / total_turnover['L']).fillna(0)  # % TOTAL

    aumDf = get_aum_df(start_date, end_date)

    codeBetaDf = get_code_beta()

    fExposureDf = get_exposure_df(start_date, end_date)

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
    tExposureDf = (sumExposureDf['RHExposure'
                   ].mul(aumDf['RHAUM'], axis=0) +
                   sumExposureDf['YAExposure'].mul(aumDf['YAAUM'], axis=0) +
                   sumExposureDf['LRExposure'].mul(aumDf['LRAUM'], axis=0))

    tExposureDf.columns = ['Exposure']

    sqlPlDf = get_pl_df(start_date, end_date)
    sqlPlDf = sqlPlDf.merge(code_name_map, left_on='quick', right_on='quick')

    if (fExposureDf[fExposureDf['advisor'] == param_adviser].empty and turnover_df[turnover_df['advisor'] == param_adviser]
        and sqlPlDf[sqlPlDf['advisor'] == param_adviser]):
        return render_template('empty.html', adviser=param_adviser)

    t = sqlPlDf.groupby(['processDate', 'advisor', 'side']).sum().drop(['RHAttr', 'YAAttr', 'LRAttr'],
                                                                       axis=1).unstack().reset_index().set_index(
            'processDate')
    attr_df = t[t['advisor'] == param_adviser]['attribution']
    attr_df['Total'] = attr_df['L'] + attr_df['S']
    cs_attr_df = attr_df
    cs_attr_df = cs_attr_df.cumsum().fillna(method='ffill').fillna(0)

    long_short_return = sqlPlDf.groupby(["advisor", "side"]).sum().drop(['RHAttr', 'YAAttr', 'LRAttr'],
                                                                        axis=1).unstack().div(sumTurnoverPerAdv,
                                                                                              axis=0) * 100

    indexReturn, pIndexDf = get_index_return(start_date, end_date)

    tExposure = tExposureDf[:, param_adviser].unstack().shift(1)

    exposure_avg = DataFrame(tExposureDf).reset_index()

    t = DataFrame(tExposureDf).reset_index()
    gross_exposure = t.groupby(by=['processDate', 'advisor'])[0].sum().div(aumDf['Total'], axis=0)
    t2 = t[t['side'] == 'S'].set_index(['processDate', 'advisor'])[0].div(aumDf['Total'], axis=0)
    t3 = DataFrame(
            t[t['side'] == 'S'].set_index(['processDate', 'advisor'])[0].div(aumDf['Total'], axis=0)).reset_index()
    t3[t3['advisor'] == 'Bal'] = 0
    t4 = t3.groupby(by='processDate')[0].sum().truncate(before=start_date)
    short_exposure = t2.div(t4, axis=0)

    rankLongDf = exposure_avg[(exposure_avg['side'] == 'L')].groupby(by='advisor').mean() * 100 / aumDf['Total'].mean()
    rankShortDf = exposure_avg[(exposure_avg['side'] == 'S')].groupby(by='advisor').mean() * 100 / aumDf['Total'].mean()
    rankLongDf = rankLongDf.drop(g.dropList, errors='ignore').rank(ascending=False)
    rankShortDf = rankShortDf.drop(g.dropList, errors='ignore').rank(ascending=False)

    net_op = DataFrame()

    net_op['L'] = attr_df['L'].sub(tExposure['L'].mul(indexReturn[g.indexMapping[param_adviser]], axis=0),
                                   axis=0).div(
            aumDf.shift(1)['Total'], axis=0)
    net_op['S'] = attr_df['S'].sub((tExposure['S'] * -1).mul(indexReturn[g.indexMapping[param_adviser]], axis=0),
                                   axis=0).div(aumDf.shift(1)['Total'], axis=0)
    net_op = net_op.cumsum().fillna(method='ffill').fillna(0)
    net_op['Total'] = net_op['L'] + net_op['S']

    btExposure = betaExposure[:, param_adviser].unstack().shift(1)
    beta_op = DataFrame()
    beta_op['L'] = attr_df['L'].sub(btExposure['L'].mul(indexReturn[g.indexMapping[param_adviser]], axis=0),
                                    axis=0).div(aumDf.shift(1)['Total'], axis=0)
    beta_op['S'] = attr_df['S'].sub((btExposure['S'] * -1).mul(indexReturn[g.indexMapping[param_adviser]], axis=0),
                                    axis=0).div(aumDf.shift(1)['Total'], axis=0)
    beta_op = beta_op.cumsum().fillna(method='ffill').fillna(0)
    beta_op['Total'] = beta_op['L'] + beta_op['S']

    totalFund = sqlPlDf.groupby(['processDate', 'advisor', 'side']).sum().drop(['attribution'],
                                                                               axis=1).unstack().reset_index().set_index(
            'processDate')

    csIndexReturn = pIndexDf/pIndexDf.ix[1]-1

    # calculate range for two graph so that we can make them have same 0 of y axis
    posPlBound = 1.1*abs(max([max(cs_attr_df.max().values), min(cs_attr_df.min().values), 0]))
    negPlBound = 1.1*abs(min([min(cs_attr_df.min().values), max(cs_attr_df.max().values), 0]))
    posIdxBound = 1.1*abs(max([csIndexReturn[g.indexMapping[param_adviser]].max(), 0]))
    negIdxBound = 1.1*abs(min([csIndexReturn[g.indexMapping[param_adviser]].min(), 0]))

    range1 = [0, 0]
    range2 = [0, 0]

    if posPlBound == 0 and posIdxBound == 0:
        range1 = [-negPlBound, 0]
        range2 = [-negIdxBound, 0]
    elif posPlBound == 0 and posIdxBound > 0 and negIdxBound != 0:
        range1 = [-negPlBound*posIdxBound/negIdxBound, negPlBound]
        range2 = [-negIdxBound, posIdxBound]
    elif negPlBound == 0 and negIdxBound == 0:
        range1 = [0, posPlBound]
        range2 = [0, posIdxBound]
    elif negPlBound == 0 and negIdxBound > 0 and posIdxBound != 0:
        range1 = [-posPlBound * negIdxBound / posIdxBound, posPlBound]
        range2 = [-negIdxBound, posIdxBound]
    elif posPlBound == 0 and posIdxBound >0 and negIdxBound == 0:
        range1 = [-negPlBound, negPlBound]
        range2 = [-posIdxBound, posIdxBound]
    elif negPlBound == 0 and negIdxBound >0 and posIdxBound == 0:
        range1 = [-posPlBound, posPlBound]
        range2 = [-negIdxBound, negIdxBound]
    elif posPlBound > 0 and negPlBound > 0 and posIdxBound == 0 and negIdxBound > 0:
        range1 = [-negPlBound, posPlBound]
        range2 = [-negIdxBound, posPlBound*negIdxBound/negPlBound]
    elif posPlBound > 0 and negPlBound > 0 and posIdxBound > 0 and negIdxBound > 0:
        range1 = [-negPlBound, posPlBound]
        if posIdxBound > negIdxBound:
            range2 = [-negPlBound*posIdxBound/posPlBound, posIdxBound]
        else:
            range2 = [-negIdxBound, posPlBound*negIdxBound/negPlBound]

    range2 = map(lambda x: x*100, range2)

    pl_graph = { 'data': [{
                    'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in cs_attr_df.index],
                    'y': cs_attr_df[col].values.tolist(),
                    'name': ('Long' if col == 'L' else ('Short' if col == 'S' else col)) + ' PL',
                    'line': {'width': g.lineWidth,
                             'color': "rgb(27, 93, 225)" if col == 'L' else ("rgb(214,39,40)" if col == 'S' else "rgb(0,0,0)")
                             }
                } for col in cs_attr_df.columns
                ] + [{
        'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in pIndexDf.index],
        'y': (csIndexReturn[g.indexMapping[param_adviser]]*100).values.tolist(),
        'name': g.indexMapping[param_adviser],
        'fill': 'tozeroy',
        'line': {'width': 0},
        'yaxis': 'y2'
    }],
        'layout': {
            'margin': {'t': 0, 'b': 15, 'l': 40, 'r': 40},
           'width': 750,
           'height': 240,
           'xaxis': {'tickformat': '%d %b', 'tickfont': {'size': 10}},
           'yaxis': {'tickfont': {'size': 10}, 'range': range1},
           'yaxis2': {
            'overlaying':'y',
            'side': 'right',
            'title': 'Index',
            'ticksuffix': '%',
            'showgrid': 'false',
            'tickfont': {'size': 10},
               'range': range2
   },
    'legend': {'font': {'size': 10}, 'x': 1.05}
        }
                 }


    netop_graph = [{
                       'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in net_op.index],
                       'y': (net_op[col] * 100).values.tolist(),
                       'name': ('Long Net' if col == 'L' else ('Short Net' if col == 'S' else col)) + ' O/P',
                       'line': {'width': g.lineWidth,
                                'color': "rgb(27, 93, 225)" if col == 'L' else ("rgb(214,39,40)" if col == 'S' else "rgb(0,0,0)")
                                }
                   } for col in net_op.columns
                   ]

    beta_graph = [{
                      'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in beta_op.index],
                      'y': (beta_op[col] * 100).fillna(0).values.tolist(),
                      'name': ('Long Beta' if col == 'L' else ('Short Beta' if col == 'S' else col)) + ' O/P',
                      'line': {'width': g.lineWidth,
                               'color': "rgb(27, 93, 225)" if col == 'L' else ("rgb(214,39,40)" if col == 'S' else "rgb(0,0,0)")
                               }
                  } for col in beta_op.columns
                  ]

    exposureGraphDf = tExposureDf[:, param_adviser].unstack().reindex(tExposureDf.index.levels[0]).fillna(0)
    exposure_graph = [{
                          'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in exposureGraphDf.index],
                          'y': exposureGraphDf[col].values.tolist(),
                          'name': 'Long Exposure' if col == 'L' else ('Short Exposure' if col == 'S' else col),
                          'line': {'width': g.lineWidth,
                                   'color': "rgb(27, 93, 225)" if col == 'L' else ("rgb(214,39,40)" if col == 'S' else "rgb(0,0,0)")
                                   }
                      } for col in ['L', 'S']
                      ]

    month_end = datetime(net_op.index[-1].year, net_op.index[-1].month, net_op.index[-1].daysinmonth)
    bm_index = pd.date_range(start=start_date, end=month_end, freq='BM')

    if net_op.index[-1] < bm_index[-1]:
        net_op.ix[bm_index[-1]] = np.nan
        beta_op.ix[bm_index[-1]] = np.nan

    bm_net_op = net_op.fillna(method='ffill').reindex(bm_index)
    bm_beta_op = beta_op.fillna(method='ffill').reindex(bm_index)
    bm_net_op = bm_net_op - bm_net_op.shift(1).fillna(0)
    bm_beta_op = bm_beta_op - bm_beta_op.shift(1).fillna(0)
    graph_op = DataFrame()
    graph_op['Long OP'] = bm_net_op['L'].fillna(0)
    graph_op['Long Beta OP'] = bm_beta_op['L'].fillna(0)
    graph_op['Short OP'] = bm_net_op['S'].fillna(0)
    graph_op['Short Beta OP'] = bm_beta_op['S'].fillna(0)
    graph_op = graph_op.truncate(before=datetime.strptime(start_date, '%Y-%m-%d')+timedelta(1))

    op_graph = dict()
    op_graph['index'] = [x.strftime('%Y-%m') for x in graph_op.index]
    op_graph['columns'] = {col: (graph_op[col] * 100).values.tolist() for col in graph_op.columns}

    gross_exposure_graph = [{
                                'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in
                                      gross_exposure[:, col].index],
                                'y': (gross_exposure[:, col] * 100).values.tolist(),
                                'name': col,
                                'line': {
                                    'color': "rgb(214, 39, 40)" if (col == param_adviser) else "rgb(190, 190, 190)",
                                    'width': g.lineWidth if (col == param_adviser) else g.thinLineWidth
                                }
                            } for col in gross_exposure.index.levels[1] if not col in g.dropList]

    short_exposure_graph = [{
                                'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in
                                      short_exposure[:, col].index],
                                'y': (short_exposure[:, col] * 100).values.tolist(),
                                'name': col,
                                'line': {
                                    'color': "rgb(214, 39, 40)" if (col == param_adviser) else "rgb(190, 190, 190)",
                                    'width': g.lineWidth if (col == param_adviser) else g.thinLineWidth

                                }
                            } for col in short_exposure.index.levels[1] if not col in g.dropList]

    names_graph = [{
                       'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in names_df[:, col].index],
                       'y': (names_df[:, col]).values.tolist(),
                       'name': col,
                       'line': {
                           'color': "rgb(214, 39, 40)" if (col == param_adviser) else "rgb(190, 190, 190)",
                           'width': g.lineWidth if (col == param_adviser) else g.thinLineWidth

                       }
                   } for col in names_df.index.levels[1] if not col in g.dropList]

    # attribution for each fund: number is correct
    fundScale = sqlPlDf.groupby(['advisor',
                                 'MktCap']).sum()[['RHAttr',
                                                   'YAAttr',
                                                   'LRAttr']].loc[(slice(param_adviser,
                                                                         param_adviser),slice(None)),:
                                                                                          ].reset_index().drop('advisor',1).set_index('MktCap')
    # pl for each side
    scalePl = sqlPlDf.groupby(['advisor',
                               'MktCap',
                               'side']).sum()[['attribution']].loc[(slice(param_adviser, param_adviser),
                                                                    slice(None)),:
                                                                                 ].unstack()['attribution'].reset_index().drop('advisor',
                                                                                                                               1).set_index('MktCap').fillna(0)

    # try to assign cap to each trade turnover as Micro,.., Large
    scaleTable = turnover_merged_df.truncate(after=end_date).reset_index()
    scaleTable.groupby(['advisor', 'MktCap']).sum()

    # TODO: truncate before?

    # TODO: analyst=DH&startDate=2014-12-31&endDate=2015-10-01, has incorrect value? for Long PL

    # TODO: analyst=TT&startDate=2014-12-31&endDate=2015-11-01  turnover and gics

    # TODO: ranking table not fit page well

    sizeTurnover = scaleTable.groupby(["advisor",
                                       "MktCap"]).sum()[['JPYPL']].loc[(slice(param_adviser,param_adviser),
                                                                        slice(None)),:].reset_index().drop('advisor',1).set_index('MktCap')
    sizeTurnover = sizeTurnover.merge(fundScale,
                                      left_index=True,
                                      right_index=True,
                                      how='outer').fillna(0).merge(scalePl, left_index=True, right_index=True, how='outer')
    totalTurnOver = sizeTurnover['JPYPL'].sum()
    sizeTurnover['TO'] = (sizeTurnover['JPYPL']/totalTurnOver).replace([np.nan,np.inf,-np.inf],0)

    sizeTurnover = sizeTurnover.reindex(['Micro', 'Small', 'Mid', 'Large', 'Mega', 'Index'], fill_value=0)
    totalSeries = sizeTurnover.sum()
    totalSeries.name = 'Total'
    scaleTotal = pd.DataFrame(totalSeries).T
    scaleTable = pd.concat([sizeTurnover, scaleTotal])
    scaleTable['Return'] = (scaleTable['L'] + scaleTable['S']) / scaleTable['JPYPL']
    scaleTable['Return'].fillna(0, inplace=True)
    scaleTable = scaleTable[['RHAttr', 'YAAttr', 'LRAttr', 'L', 'S', 'JPYPL', 'TO', 'Return']]

    scaleTable = scaleTable.rename(
        columns={'JPYPL': 'Turnover', 'RHAttr': 'Rockhampton', 'YAAttr': 'Yaraka', 'LRAttr': 'Longreach', 'L': 'LongPL',
                 'S': 'ShortPL', 'TO': 'TO %'})

    gicsTable = turnover_merged_df.truncate(after=end_date).groupby(["advisor", "GICS"]).sum()[['JPYPL']].loc[
                (slice(param_adviser, param_adviser), slice(None)), :].reset_index().drop('advisor', 1).set_index(
            'GICS')
    fundGics = sqlPlDf.groupby(['advisor', 'GICS']).sum()[['RHAttr', 'YAAttr', 'LRAttr']].loc[
               (slice(param_adviser, param_adviser), slice(None)), :].reset_index().drop('advisor', 1).set_index(
            'GICS')
    gicsPl = sqlPlDf.groupby(['advisor', 'GICS', 'side']).sum()[['attribution']].loc[
             (slice(param_adviser, param_adviser), slice(None)), :].unstack()['attribution'].reset_index().drop(
            'advisor', 1).set_index('GICS').fillna(0)
    gicsTable = gicsTable.merge(fundGics, left_index=True, right_index=True, how='outer').merge(gicsPl, left_index=True,
                                                                                                right_index=True,
                                                                                                how='outer').fillna(0)

    totalTurnOver = gicsTable['JPYPL'].sum()
    gicsTable['TO'] = gicsTable['JPYPL'] / totalTurnOver

    total_series = gicsTable.sum()
    total_series.name = 'Total'
    gicsTotal = pd.DataFrame(total_series).T
    gicsTable = pd.concat([gicsTable, gicsTotal])
    gicsTable['Return'] = ((gicsTable['L'] + gicsTable['S']) / gicsTable['JPYPL']).replace(['', np.nan, np.inf, -np.inf], 0)
    gicsTable = gicsTable[['RHAttr', 'YAAttr', 'LRAttr', 'L', 'S', 'JPYPL', 'TO', 'Return']]
    gicsTable = gicsTable.rename(
            columns={'JPYPL': 'Turnover', 'RHAttr': 'Rockhampton', 'YAAttr': 'Yaraka', 'LRAttr': 'Longreach',
                     'L': 'LongPL',
                     'S': 'ShortPL', 'TO': 'TO %'})

    codeBetaDf['code'] = codeBetaDf[['code']].applymap(str.upper)[
        'code']  # some code has inconsistent format like xxxx Hk instead of HK
    t = sqlPlDf.merge(codeBetaDf, left_on='quick', right_on='code', how='left')
    sectorTable = turnover_merged_df.truncate(after=end_date).groupby(["advisor", "sector"]).sum()[['JPYPL']].loc[
                  (slice(param_adviser, param_adviser), slice(None)), :].reset_index().drop('advisor', 1).set_index(
            'sector')
    fundSector = t.groupby(['advisor', 'sector']).sum()[['RHAttr', 'YAAttr', 'LRAttr']].loc[
                 (slice(param_adviser, param_adviser), slice(None)), :].reset_index().drop('advisor', 1).set_index(
            'sector')

    sectorPl = t.groupby(['advisor', 'sector', 'side']).sum()[['attribution']].loc[
               (slice(param_adviser, param_adviser), slice(None)), :].unstack()['attribution'].reset_index().drop(
            'advisor', 1).set_index('sector').fillna(0)

    sectorTable = sectorTable.merge(fundSector, left_index=True,
                                    right_index=True, how='outer').fillna(0).merge(sectorPl,
                                    left_index=True,
                                    right_index=True,
                                    how='outer').fillna(0)

    sectorTotalTurnOver = sectorTable['JPYPL'].sum()

    sectorTable['TO'] = sectorTable['JPYPL'] / sectorTotalTurnOver

    sectorSeries = sectorTable.sum()
    sectorSeries.name = 'Total'
    if 'TailSens' in sectorTable.index and 'Tail' in sectorTable.index:
        sectorTable.ix['Tail'] = sectorTable.ix['Tail'] + sectorTable.ix['TailSens']
        sectorTable.drop('TailSens', inplace=True)
    if 'Tail' in sectorTable.index and 'TailRR' in sectorTable.index:
        sectorTable.ix['Tail'] = sectorTable.ix['Tail'] + sectorTable.ix['TailRR']
        sectorTable.drop('TailRR', inplace=True)

    sectorTotal = pd.DataFrame(sectorSeries).T
    sectorTable = pd.concat([sectorTable, sectorTotal])
    sectorTable['Return'] = ((sectorTable['L'] + sectorTable['S']) / sectorTable['JPYPL']).replace([np.inf, -np.inf], 0)
    sectorTable = sectorTable[['RHAttr', 'YAAttr', 'LRAttr', 'L', 'S', 'JPYPL', 'TO', 'Return']]
    sectorTable = sectorTable.rename(
            columns={'JPYPL': 'Turnover', 'RHAttr': 'Rockhampton', 'YAAttr': 'Yaraka', 'LRAttr': 'Longreach',
                     'L': 'LongPL',
                     'S': 'ShortPL', 'TO': 'TO %'})

    firstTradeDate = np.where(sqlPlDf['side'] == 'L',sqlPlDf['firstTradeDateLong'],sqlPlDf['firstTradeDateShort'])
    top_positions = sqlPlDf[['quick',
                             'advisor',
                             'attribution',
                             'name',
                             'side',
                             'processDate',
                             'firstTradeDateLong',
                             'firstTradeDateShort'
                             ]].groupby(['advisor',
                                         'quick',
                                         'name',
                                         'side',
                                         firstTradeDate
                                         ]).sum().sort_values(by='attribution', ascending=False).ix[
        param_adviser].head(NUMBER_OF_TOP_POSITIONS)
    top_positions = top_positions.reset_index().drop('quick', axis=1)
    top_positions.index = top_positions.index + 1
    top_positions = top_positions.rename(columns={'name': 'Name', 'side': 'Side', 'attribution': 'Attribution', 'level_3': 'First Trade Date'})
    top_positions = top_positions[['Name', 'Side', 'Attribution', 'First Trade Date']]

    bottom_positions = sqlPlDf[['quick',
                                'advisor',
                                'attribution',
                                'name',
                                'side',
                                'firstTradeDateLong',
                                'firstTradeDateShort'
                                ]].groupby(['advisor',
                                            'quick',
                                            'name',
                                            'side',
                                            firstTradeDate
                                            ]).sum().sort_values(by='attribution').ix[param_adviser].head(
        NUMBER_OF_TOP_POSITIONS)
    bottom_positions = bottom_positions.reset_index().drop('quick', axis=1)
    bottom_positions.index = bottom_positions.index + 1
    bottom_positions = bottom_positions.rename(columns={'name': 'Name', 'side': 'Side', 'attribution': 'Attribution', 'level_3': 'First Trade Date'})
    bottom_positions = bottom_positions[['Name', 'Side', 'Attribution', 'First Trade Date']]

    topixTable = turnover_merged_df.truncate(after=end_date).groupby(["advisor", "TOPIX"]).sum()[['JPYPL']].loc[
                 (slice(param_adviser, param_adviser), slice(None)), :].reset_index().drop('advisor', 1).set_index(
            'TOPIX')
    fundTopix = sqlPlDf.groupby(['advisor', 'TPX']).sum()[['RHAttr', 'YAAttr', 'LRAttr']].loc[
                (slice(param_adviser, param_adviser), slice(None)), :].reset_index().drop('advisor', 1).set_index(
            'TPX')
    fundTopix = fundTopix.rename(index={'Warehousing  and  Harbor Transpo': 'Warehousing  and  Harbor Transport'})
    topixPl = sqlPlDf.groupby(['advisor', 'TPX', 'side']).sum()[['attribution']].loc[
              (slice(param_adviser, param_adviser), slice(None)), :].unstack()['attribution'].reset_index().drop(
            'advisor', 1).set_index('TPX')
    topixPl = topixPl.rename(index={'Warehousing  and  Harbor Transpo': 'Warehousing  and  Harbor Transport'})
    topixTable = topixTable.merge(fundTopix, left_index=True, right_index=True, how='outer').fillna(0).merge(topixPl.fillna(0),
                                                                                      left_index=True, right_index=True, how='outer')
    totalTurnOver = topixTable['JPYPL'].sum()
    topixTable['TO'] = topixTable['JPYPL'] / totalTurnOver

    topixSeries = topixTable.sum()
    topixSeries.name = 'Total'
    topixTotal = pd.DataFrame(topixSeries).T
    topixTable = pd.concat([topixTable, topixTotal])
    topixTable['Return'] = ((topixTable['L'] + topixTable['S'].fillna(0)) / topixTable['JPYPL']).replace(
            [np.nan, np.inf, -np.inf], 0)
    topixTable = topixTable[['RHAttr', 'YAAttr', 'LRAttr', 'L', 'S', 'JPYPL', 'TO', 'Return']]
    topixTable = topixTable.rename(
            columns={'JPYPL': 'Turnover', 'RHAttr': 'Rockhampton', 'YAAttr': 'Yaraka', 'LRAttr': 'Longreach',
                     'L': 'LongPL',
                     'S': 'ShortPL', 'TO': 'TO %'})

    strategyTable = turnover_merged_df.truncate(after=end_date).groupby(["advisor", "strategy"]).sum()[['JPYPL']].loc[
                    (slice(param_adviser, param_adviser), slice(None)), :].reset_index().drop('advisor',
                                                                                              1).set_index(
            'strategy')
    fundStrategy = sqlPlDf.groupby(['advisor', 'strategy']).sum()[['RHAttr', 'YAAttr', 'LRAttr']].loc[
                   (slice(param_adviser, param_adviser), slice(None)), :].reset_index().drop('advisor',
                                                                                             1).set_index(
            'strategy')
    fundStrategy = fundStrategy.fillna(0)
    strategyPl = sqlPlDf.groupby(['advisor', 'strategy', 'side']).sum()[['attribution']].loc[
                 (slice(param_adviser, param_adviser), slice(None)), :].unstack()['attribution'].reset_index().drop(
            'advisor', 1).set_index('strategy')
    strategyPl = strategyPl.fillna(0)
    strategyTable = strategyTable.merge(fundStrategy, left_index=True, right_index=True, how='outer').fillna(0).merge(strategyPl,
                                                                                               left_index=True,
                                                                                               right_index=True,
                                                                                               how='left').fillna(0)

    totalStrategyTurnOver = strategyTable['JPYPL'].sum()
    strategyTable['TO'] = strategyTable['JPYPL'] / totalStrategyTurnOver

    strategySeries = strategyTable.sum()
    strategySeries.name = 'Total'
    strategyTotal = pd.DataFrame(strategySeries).T
    strategyTable = pd.concat([strategyTable, strategyTotal])
    strategyTable['Return'] = ((strategyTable['L'] + strategyTable['S'].fillna(0)) / strategyTable['JPYPL']).replace(
            [np.inf, -np.inf], 0)
    strategyTable = strategyTable[['RHAttr', 'YAAttr', 'LRAttr', 'L', 'S', 'JPYPL', 'TO', 'Return']]
    strategyTable = strategyTable.rename(
            columns={'JPYPL': 'Turnover', 'RHAttr': 'Rockhampton', 'YAAttr': 'Yaraka', 'LRAttr': 'Longreach',
                     'L': 'LongPL',
                     'S': 'ShortPL', 'TO': 'TO %'})
    positionTable = turnover_merged_df.truncate(after=end_date).groupby(["advisor", "code"]).sum()[['JPYPL']].loc[
                    (slice(param_adviser, param_adviser), slice(None)), :].reset_index().drop('advisor',
                                                                                              1).set_index('code')
    positionPl = sqlPlDf.groupby(['advisor', 'quick', 'name']).sum()[['RHAttr', 'YAAttr', 'LRAttr']].loc[
                 (slice(param_adviser, param_adviser), slice(None)), :].reset_index().drop('advisor', 1).set_index(
            'quick')
    sidePl = sqlPlDf.groupby(['advisor', 'quick', 'side']).sum()[['attribution']].loc[
             (slice(param_adviser, param_adviser), slice(None)), :].unstack()['attribution'].reset_index().drop(
            'advisor', 1).set_index('quick').fillna(0)
    positionTable = positionTable.merge(positionPl, left_index=True, right_index=True, how='outer'). \
        merge(sidePl, left_index=True, right_index=True, how='left').fillna(0)

    totalPositionTurnOver = positionTable['JPYPL'].sum()
    positionTable['TO'] = positionTable['JPYPL'] / totalPositionTurnOver
    positionTable = positionTable.reset_index().set_index(['name']).sort_index()
    positionSeries = positionTable.sum()
    positionSeries.name = 'Total'
    positionTotal = pd.DataFrame(positionSeries).T

    positionTable = pd.concat([positionTable, positionTotal])
    positionTable['Return'] = ((positionTable['L'] + positionTable['S'].fillna(0)) / positionTable['JPYPL'].replace(0,
                                                                                                                    np.nan)).replace(
            [np.nan, np.inf, -np.inf], 0)
    positionTable = positionTable[['RHAttr', 'YAAttr', 'LRAttr', 'L', 'S', 'JPYPL', 'TO', 'Return']]
    positionTable = positionTable.rename(
            columns={'JPYPL': 'Turnover', 'RHAttr': 'Rockhampton', 'YAAttr': 'Yaraka', 'LRAttr': 'Longreach',
                     'L': 'LongPL',
                     'S': 'ShortPL', 'TO': 'TO %'})

    tables_html = ''
    remaining_row_number = 28
    for df in [scaleTable, gicsTable, sectorTable]:
        tables_html, remaining_row_number = add_to_page(df, tables_html, remaining_row_number)

    tables_html, remaining_row_number = add_to_page(top_positions, tables_html, remaining_row_number, 'ranking',
                                                    'Top %s Trades' % NUMBER_OF_TOP_POSITIONS)
    tables_html, remaining_row_number = add_to_page(bottom_positions, tables_html, remaining_row_number, 'ranking',
                                                    'Bottom %s Trades' % NUMBER_OF_TOP_POSITIONS)

    remaining_row_number -= 2 + 1  # 2 titles of ranking tables
    table_list = [topixTable, strategyTable, positionTable] if g.indexMapping[param_adviser] == 'TPX' else [
        strategyTable, positionTable]
    for df in table_list:
        tables_html, remaining_row_number = add_to_page(df, tables_html, remaining_row_number)

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
    render_obj['index'] = g.indexMapping[param_adviser]
    render_obj['startDate'] = start_date
    render_obj['endDate'] = end_date
    render_obj['longTurnover'] = Decimal(sumTurnoverPerAdv.fillna(0).ix[param_adviser]['L']).quantize(Decimal('1.'),
                                                                                            rounding=ROUND_HALF_UP)
    render_obj['shortTurnover'] = Decimal(sumTurnoverPerAdv.fillna(0).ix[param_adviser]['S']).quantize(Decimal('1.'),
                                                                                             rounding=ROUND_HALF_UP)
    render_obj['totalLong'] = totalRatio.ix[param_adviser]['L']
    render_obj['totalShort'] = totalRatio.ix[param_adviser]['S']
    render_obj['longPL'] = Decimal(cs_attr_df['L'].iloc[-1]).quantize(Decimal('1.'), rounding=ROUND_HALF_UP)
    render_obj['shortPL'] = Decimal(cs_attr_df['S'].iloc[-1]).quantize(Decimal('1.'), rounding=ROUND_HALF_UP)
    render_obj['longIndexOP'] = net_op['L'].iloc[-1] * 100
    render_obj['shortIndexOP'] = net_op['S'].iloc[-1] * 100
    render_obj['longBetaOP'] = beta_op['L'].iloc[-1] * 100
    render_obj['shortBetaOP'] = beta_op['S'].iloc[-1] * 100
    render_obj['longHitRate'] = hitRateDf['LongsHR'].ix[param_adviser]
    render_obj['shortHitRate'] = hitRateDf['ShortsHR'].ix[param_adviser]
    render_obj['longReturn'] = long_short_return.fillna(0)['attribution']['L'].ix[param_adviser]
    render_obj['shortReturn'] = long_short_return.fillna(0)['attribution']['S'].ix[param_adviser]
    render_obj['rhBpsLong'] = totalFund[totalFund['advisor'] == param_adviser].sum()['RHAttr']['L'] * 100
    render_obj['rhBpsShort'] = totalFund[totalFund['advisor'] == param_adviser].sum()['RHAttr']['S'] * 100
    render_obj['yaBpsLong'] = totalFund[totalFund['advisor'] == param_adviser].sum()['YAAttr']['L'] * 100
    render_obj['yaBpsShort'] = totalFund[totalFund['advisor'] == param_adviser].sum()['YAAttr']['S'] * 100
    render_obj['lrBpsLong'] = totalFund[totalFund['advisor'] == param_adviser].sum()['LRAttr']['L'] * 100
    render_obj['lrBpsShort'] = totalFund[totalFund['advisor'] == param_adviser].sum()['LRAttr']['S'] * 100
    render_obj['exposure_avg_long'] = (
        exposure_avg[(exposure_avg['advisor'] == param_adviser) & (exposure_avg['side'] == 'L')].mean() * 100 / aumDf[
            'Total'].mean()).iloc[0]
    render_obj['exposure_avg_short'] = (
        exposure_avg[(exposure_avg['advisor'] == param_adviser) & (exposure_avg['side'] == 'S')].mean() * 100 / aumDf[
            'Total'].mean()).iloc[0]
    render_obj['rank_long'] = rankLongDf.ix[param_adviser][0]
    render_obj['rank_short'] = rankShortDf.ix[param_adviser][0]
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

    return render_template('attrib.html', params=render_obj)


@app.route('/test')
def test():
    return render_template('test.html')


def getSumTurnover(from_date, end_date):
    sumTurnoverPerAdv = cache.get('sumTurnoverPerAdv')
    totalRatio = cache.get('totalRatio')
    if sumTurnoverPerAdv is None or totalRatio is None:
        sqlFxDf = get_fx_df(from_date, end_date)

        turnover_df = get_turnover_df(from_date, end_date)

        # merge with FX df to get to-JPY-fx rate
        turnover_merged_df = turnover_df.merge(sqlFxDf, left_on='currencyCode', right_index=True).sort_index()
        # create new column which contain turnover in JPY
        turnover_merged_df['JPYPL'] = (turnover_merged_df['Turnover'] * turnover_merged_df['AvgOfrate']).values

        # calculate total turnover for each side
        total_turnover = turnover_merged_df.truncate(after=end_date).groupby(["side"]).sum()['JPYPL']

        # calculate turnover for each advisor
        sumTurnoverPerAdv = turnover_merged_df.truncate(after=end_date).groupby(["advisor", "side"]).sum()[
            'JPYPL'].unstack()

        totalRatio = sumTurnoverPerAdv * 100 / total_turnover['L']

        cache.set('sumTurnoverPerAdv', sumTurnoverPerAdv, timeout=TIMEOUT)
        cache.set('totalRatio', totalRatio, timeout=TIMEOUT)

    return sumTurnoverPerAdv, totalRatio



def get_attr_df(from_date, end_date):

    sqlPlDf = cache.get('sql_pl_df')
    long_short_return = cache.get('long_short_return')

    if sqlPlDf is None:
        code_name_map = sql.read_sql('''SELECT quick, name FROM t01Instrument;''', g.con)
        sqlPlDf = get_pl_df(from_date, end_date)
        sqlPlDf = sqlPlDf.merge(code_name_map, left_on='quick', right_on='quick')

        sumTurnoverPerAdv, totalRatio = getSumTurnover(from_date, end_date)

        long_short_return = sqlPlDf.groupby(["advisor", "side"]).sum().drop(['RHAttr', 'YAAttr', 'LRAttr'],
                                                                        axis=1).unstack().div(sumTurnoverPerAdv,
                                                                                              axis=0) * 100

        cache.set('long_short_return', long_short_return)
        cache.set('sql_pl_df', sqlPlDf)

    return long_short_return, sqlPlDf


def getTurnoverValue(advisor, side):
    '''
    :param advisor: advisor abbreviation
    :param side: 'L' or 'S'
    :return: sum of turnover
    '''

    sumTurnoverPerAdv, totalRatio = getSumTurnover(g.startDate, g.endDate)

    return Decimal(sumTurnoverPerAdv.ix[advisor][side]).quantize(Decimal('1.'), rounding=ROUND_HALF_UP)


def getTurnoverRatio(advisor, side):
    sumTurnoverPerAdv, totalRatio = getSumTurnover(g.startDate, g.endDate)
    return totalRatio.ix[advisor][side]


def get_pl(advisor, side, type):
    long_short_return, sqlPlDf = get_attr_df(g.startDate, g.endDate)
    attr_df = sqlPlDf.groupby(['advisor', 'side']).sum()
    if type =='pl':
        return Decimal(attr_df.ix[(advisor, side)]['attribution']).quantize(Decimal('1.'), rounding=ROUND_HALF_UP)
    elif type == 'return':
        return long_short_return['attribution'][side].ix[advisor]
    elif type == 'rhpl':
        return attr_df.ix[(advisor, side)]['RHAttr']
    elif type == 'yapl':
        return attr_df.ix[(advisor, side)]['YAAttr']
    elif type == 'lrpl':
        return attr_df.ix[(advisor, side)]['LRAttr']


def get_avg_exposure(from_date, end_date):

    exposure_avg = cache.get('exposure_avg')
    tExposureDf = cache.get('tExposureDf')
    betaExposure = cache.get('betaExposure')
    gross_exposure = cache.get('gross_exposure')

    if exposure_avg is None:
        aumDf = get_aum_df(from_date, end_date)
        codeBetaDf = get_code_beta()

        fExposureDf = get_exposure_df(g.startDate, end_date)

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
        t = DataFrame(tExposureDf).reset_index()
        gross_exposure = t.groupby(by=['processDate', 'advisor'])[0].sum().div(aumDf['Total'], axis=0)

        exposure_avg = DataFrame(tExposureDf).reset_index()

        cache.set('tExposureDf', tExposureDf)
        cache.set('exposure_avg', exposure_avg)
        cache.set('betaExposure', betaExposure)
        cache.set('gross_exposure', gross_exposure)

    return exposure_avg, tExposureDf, betaExposure, gross_exposure


def get_exposure(adviser, side):
    exposure_avg, tExposureDf, betaExposure, gross_exposure = get_avg_exposure(g.startDate, g.endDate)
    aum_df = get_aum_df(g.startDate, g.endDate)
    return (exposure_avg[(exposure_avg['advisor'] == adviser) & (exposure_avg['side'] == side)].mean() * 100 / aum_df[
            'Total'].mean()).iloc[0]


def get_rank(adviser, side):
    exposure_avg, tExposureDf, betaExposure, gross_exposure = get_avg_exposure(g.startDate, g.endDate)
    aum_df = get_aum_df(g.startDate, g.endDate)
    rankLongDf = exposure_avg[(exposure_avg['side'] == 'L')].groupby(by='advisor').mean() * 100 / aum_df['Total'].mean()
    rankShortDf = exposure_avg[(exposure_avg['side'] == 'S')].groupby(by='advisor').mean() * 100 / aum_df['Total'].mean()
    rankLongDf = rankLongDf.drop(g.dropList, errors='ignore').rank(ascending=False)
    rankShortDf = rankShortDf.drop(g.dropList, errors='ignore').rank(ascending=False)

    if side == 'L':
        return rankLongDf.ix[adviser][0]
    elif side == 'S':
        return rankShortDf.ix[adviser][0]


def get_net_op(adviser):

    indexReturn, pIndexDf = get_index_return(g.startDate, g.endDate)
    exposure_avg, tExposureDf, betaExposure, gross_exposure = get_avg_exposure(g.startDate, g.endDate)
    aum_df = get_aum_df(g.startDate, g.endDate)
    long_short_return, sqlPlDf = get_attr_df(g.startDate, g.endDate)

    tExposure = tExposureDf[:, adviser].unstack().shift(1)

    t = sqlPlDf.groupby(['processDate', 'advisor', 'side']).sum().drop(['RHAttr', 'YAAttr', 'LRAttr'],
                                                                       axis=1).unstack().reset_index().set_index(
            'processDate')
    attr_df = t[t['advisor'] == adviser]['attribution']
    attr_df['Total'] = attr_df['L'] + attr_df['S']

    net_op = DataFrame()
    net_op['L'] = attr_df['L'].sub(tExposure['L'].mul(indexReturn[g.indexMapping[adviser]], axis=0),
                                   axis=0).div(
            aum_df.shift(1)['Total'], axis=0)
    net_op['S'] = attr_df['S'].sub((tExposure['S'] * -1).mul(indexReturn[g.indexMapping[adviser]], axis=0),
                                   axis=0).div(aum_df.shift(1)['Total'], axis=0)
    net_op.ix[g.startDate] = 0
    net_op = net_op.cumsum().fillna(0)
    net_op['Total'] = net_op['L'] + net_op['S']

    netop_graph = [{
                       'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in net_op.index],
                       'y': (net_op[col] * 100).values.tolist(),
                       'name': ('Long Net' if col == 'L' else ('Short Net' if col == 'S' else col)) + ' O/P',
                       'line': {'width': g.lineWidth}
                   } for col in net_op.columns
                   ]

    return net_op, attr_df, netop_graph


def get_beta_op(adviser):

    indexReturn, pIndexDf = get_index_return(g.startDate, g.endDate)
    exposure_avg, tExposureDf, betaExposure, gross_exposure = get_avg_exposure(g.startDate, g.endDate)
    long_short_return, sqlPlDf = get_attr_df(g.startDate, g.endDate)
    aum_df = get_aum_df(g.startDate, g.endDate)

    btExposure = betaExposure[:, adviser].unstack().shift(1)

    t = sqlPlDf.groupby(['processDate', 'advisor', 'side']).sum().drop(['RHAttr', 'YAAttr', 'LRAttr'],
                                                                       axis=1).unstack().reset_index().set_index(
            'processDate')
    attr_df = t[t['advisor'] == adviser]['attribution']
    attr_df['Total'] = attr_df['L'] + attr_df['S']

    beta_op = DataFrame()
    beta_op['L'] = attr_df['L'].sub(btExposure['L'].mul(indexReturn[g.indexMapping[adviser]], axis=0),
                                    axis=0).div(aum_df.shift(1)['Total'], axis=0)
    beta_op['S'] = attr_df['S'].sub((btExposure['S'] * -1).mul(indexReturn[g.indexMapping[adviser]], axis=0),
                                    axis=0).div(aum_df.shift(1)['Total'], axis=0)
    beta_op = beta_op.cumsum().fillna(0)
    beta_op['Total'] = beta_op['L'] + beta_op['S']

    beta_graph = [{
                      'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in beta_op.index],
                      'y': (beta_op[col] * 100).fillna(0).values.tolist(),
                      'name': ('Long Beta' if col == 'L' else ('Short Beta' if col == 'S' else col)) + ' O/P',
                      'line': {'width': g.lineWidth}
                  } for col in beta_op.columns
                  ]
    return beta_op, beta_graph


def get_pl_graph(adviser, margin_top, margin_bottom, margin_left, margin_right, graph_width, graph_height):
    net_op, attr_df, netop_graph = get_net_op(adviser)
    cs_attr_df = attr_df.cumsum().fillna(0)
    indexReturn, pIndexDf = get_index_return(g.startDate, g.endDate)
    exposure_avg, tExposureDf, betaExposure, gross_exposure = get_avg_exposure(g.startDate, g.endDate)

    tExposure = tExposureDf[:, adviser].unstack().shift(1)

    pl_graph = [{
                    'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in cs_attr_df.index],
                    'y': cs_attr_df[col].values.tolist(),
                    'name': ('Long' if col == 'L' else ('Short' if col == 'S' else col)) + ' PL',
                    'line': {'width': g.lineWidth}
                } for col in cs_attr_df.columns
                ] + [{
        'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in pIndexDf.index],
        'y': ((tExposure['L'] + tExposure['S']) * indexReturn[g.indexMapping[adviser]]).sub(
                pIndexDf[g.indexMapping[adviser]], axis=0).cumsum().fillna(0).values.tolist(),
        'name': g.indexMapping[adviser],
        'fill': 'tozeroy',
        'line': {'width': 0}
    }]

    return pl_graph


def get_exposure_graph(adviser):
    exposure_avg, tExposureDf, betaExposure, gross_exposure = get_avg_exposure(g.startDate, g.endDate)
    exposureGraphDf = tExposureDf[:, adviser].unstack().reindex(tExposureDf.index.levels[0]).fillna(0)
    exposure_graph = [{
                          'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in exposureGraphDf.index],
                          'y': exposureGraphDf[col].values.tolist(),
                          'name': 'Long Exposure' if col == 'L' else ('Short Exposure' if col == 'S' else col),
                          'line': {'width': g.lineWidth}
                      } for col in ['L', 'S']
                      ]
    return exposure_graph


def get_name_graph(adviser):
    fExposureDf = get_exposure_df(g.startDate, g.endDate)

    names_df = fExposureDf.groupby(by=['processDate', 'advisor']).count()['quick']
    names_graph = [{
                       'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in names_df[:, col].index],
                       'y': (names_df[:, col]).values.tolist(),
                       'name': col,
                       'line': {
                           'color': "rgb(214, 39, 40)" if (col == adviser) else "rgb(190, 190, 190)",
                           'width': g.lineWidth if (col == adviser) else g.thinLineWidth

                       }
                   } for col in names_df.index.levels[1] if not col in g.dropList]

    return names_graph


def get_op_graph(adviser):
    net_op, attr_df, netop_graph = get_net_op(adviser)
    beta_op, beta_graph = get_beta_op(adviser)

    bm_index = pd.date_range(start=g.startDate, end=g.endDate, freq='BM')
    bm_net_op = net_op.reindex(bm_index)
    bm_beta_op = beta_op.reindex(bm_index)
    bm_net_op = bm_net_op - bm_net_op.shift(1)
    bm_beta_op = bm_beta_op - bm_beta_op.shift(1)
    graph_op = DataFrame()
    graph_op['Long OP'] = bm_net_op['L'].fillna(0) * 100
    graph_op['Long Beta OP'] = bm_beta_op['L'].fillna(0) * 100
    graph_op['Short OP'] = bm_net_op['S'].fillna(0) * 100
    graph_op['Short Beta OP'] = bm_beta_op['S'].fillna(0) * 100

    op_graph = dict()
    op_graph['index'] = [x.strftime('%Y-%m') for x in graph_op.index]
    op_graph['columns'] = {col: (graph_op[col] * 100).values.tolist() for col in graph_op.columns}

    return op_graph


def get_gross_exposure_graph(adviser):
    exposure_avg, tExposureDf, betaExposure, gross_exposure = get_avg_exposure(g.startDate, g.endDate)
    gross_exposure_graph = [{
                                'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in
                                      gross_exposure[:, col].index],
                                'y': (gross_exposure[:, col] * 100).values.tolist(),
                                'name': col,
                                'line': {
                                    'color': "rgb(214, 39, 40)" if (col == adviser) else "rgb(190, 190, 190)",
                                    'width': g.lineWidth if (col == adviser) else g.thinLineWidth
                                }
                            } for col in gross_exposure.index.levels[1] if not col in g.dropList]
    return gross_exposure_graph


def get_short_exposure_graph(adviser):
    exposure_avg, tExposureDf, betaExposure, gross_exposure = get_avg_exposure(g.startDate, g.endDate)
    aum_df = get_aum_df(g.startDate, g.endDate)
    t = DataFrame(tExposureDf).reset_index()
    t2 = t[t['side'] == 'S'].set_index(['processDate', 'advisor'])[0].div(aum_df['Total'], axis=0)
    t3 = DataFrame(
            t[t['side'] == 'S'].set_index(['processDate', 'advisor'])[0].div(aum_df['Total'], axis=0)).reset_index()
    t3[t3['advisor'] == 'Bal'] = 0
    t4 = t3.groupby(by='processDate')[0].sum().truncate(before=g.startDate)
    short_exposure = t2.div(t4, axis=0)
    short_exposure_graph = [{
                                'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in
                                      short_exposure[:, col].index],
                                'y': (short_exposure[:, col] * 100).values.tolist(),
                                'name': col,
                                'line': {
                                    'color': "rgb(214, 39, 40)" if (col == adviser) else "rgb(190, 190, 190)",
                                    'width': g.lineWidth if (col == adviser) else g.thinLineWidth

                                }
                            } for col in short_exposure.index.levels[1] if not col in g.dropList]
    return short_exposure_graph


@app.route('/test2')
def test2():
    param_adviser = request.args.get('analyst', g.reportAdvisor)

    hitRateDf = get_hit_rate_df(g.startDate, g.endDate)

    net_op, attr_df, netop_graph = get_net_op(param_adviser)
    beta_op, beta_graph = get_beta_op(param_adviser)

    render_obj = dict()
    render_obj['analyst'] = param_adviser
    render_obj['index'] = g.indexMapping[param_adviser]
    render_obj['startDate'] = g.startDate
    render_obj['endDate'] = g.endDate
    render_obj['analyst_list'] = g.indexMapping.keys()
    render_obj['longTurnover'] = getTurnoverValue(param_adviser, 'L')
    render_obj['shortTurnover'] = getTurnoverValue(param_adviser, 'S')
    render_obj['totalLong'] = getTurnoverRatio(param_adviser, 'L')
    render_obj['totalShort'] = getTurnoverRatio(param_adviser, 'S')

    render_obj['longPL'] = get_pl(param_adviser, 'L', 'pl')
    render_obj['shortPL'] = get_pl(param_adviser, 'S', 'pl')

    render_obj['longIndexOP'] = net_op['L'].iloc[-1] * 100
    render_obj['shortIndexOP'] = net_op['S'].iloc[-1] * 100

    render_obj['longBetaOP'] = beta_op['L'].iloc[-1] * 100
    render_obj['shortBetaOP'] = beta_op['S'].iloc[-1] * 100

    render_obj['longHitRate'] = hitRateDf['LongsHR'].ix[param_adviser]
    render_obj['shortHitRate'] = hitRateDf['ShortsHR'].ix[param_adviser]

    render_obj['longReturn'] = get_pl(param_adviser, 'L', 'return')
    render_obj['shortReturn'] = get_pl(param_adviser, 'S', 'return')

    render_obj['rhBpsLong'] = get_pl(param_adviser, 'L', 'rhpl') * 100
    render_obj['rhBpsShort'] = get_pl(param_adviser, 'S', 'rhpl') * 100
    render_obj['yaBpsLong'] = get_pl(param_adviser, 'L', 'yapl') * 100
    render_obj['yaBpsShort'] = get_pl(param_adviser, 'S', 'yapl') * 100
    render_obj['lrBpsLong'] = get_pl(param_adviser, 'L', 'lrpl') * 100
    render_obj['lrBpsShort'] = get_pl(param_adviser, 'S', 'lrpl') * 100

    render_obj['exposure_avg_long'] = get_exposure(param_adviser, 'L')
    render_obj['exposure_avg_short'] = get_exposure(param_adviser, 'S')

    render_obj['rank_long'] = get_rank(param_adviser, 'L')
    render_obj['rank_short'] = get_rank(param_adviser, 'S')

    render_obj['graph_width'] = 750
    render_obj['graph_height'] = 240
    render_obj['graph_line_width'] = g.lineWidth
    render_obj['margin_left'] = 40
    render_obj['margin_top'] = 40
    render_obj['margin_bottom'] = 30
    render_obj['margin_right'] = 5
    render_obj['graph_font'] = 'Calibri'
    render_obj['graph_font_size'] = 10

    render_obj['pl_graph'] = get_pl_graph(param_adviser, render_obj['margin_top'], render_obj['margin_bottom'],
                                          render_obj['margin_left'], render_obj['margin_right'],
                                          render_obj['graph_width'], render_obj['graph_height'])
    render_obj['netop_graph'] = netop_graph
    render_obj['betaop_graph'] = beta_graph
    render_obj['exposure_graph'] = get_exposure_graph(param_adviser)
    render_obj['op_graph'] = get_op_graph(param_adviser)
    render_obj['gross_exposure_graph'] = get_gross_exposure_graph(param_adviser)
    render_obj['short_exposure_graph'] = get_short_exposure_graph(param_adviser)
    render_obj['names_graph'] = get_name_graph(param_adviser)

    return render_template('test2.html', params=render_obj)


@app.route('/logout')
def logout():
    session.pop('logged_in', None)
    flash('You were logged out')
    return redirect(url_for('login'))


# default port 5000
if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')
