from flask import request, g, render_template

import pandas as pd
from pandas import DataFrame
from pandas.io import sql
import numpy as np
import pymysql
from datetime import datetime, timedelta
from decimal import *

from . import tradehistory


@tradehistory.before_request
def before_request():
    g.con = pymysql.connect(host='127.0.0.1', user='root', passwd='root', db='hkg02p')
    # g.start_date = datetime(datetime.now().year-2, 12, 31).strftime('%Y-%m-%d')
    g.start_date = '2012-01-01'
    g.end_date = datetime.now().strftime('%Y-%m-%d')  # not include
    g.param_adviser = 'AP'
    g.lineWidth = 3
    g.thinLineWidth = 2


@tradehistory.teardown_request
def teardown_request(exception):
    con = getattr(g, 'con', None)
    if con is not None:
        con.close()


@tradehistory.route('/')
def index():
    return render_template('tradehistory/index.html')


@tradehistory.route('/check')
def check():
    quick = request.args.get('quick', '7203')

    sql_pl_df = sql.read_sql('''SELECT processDate,advisor, side, a.quick, attribution,
                RHAttribution AS RHAttr,
                YAAttribution AS YAAttr,
                LRAttribution AS LRAttr,
                IF (side = 'L', firstTradeDateLong, firstTradeDateShort) AS firstTradeDate,
                a.RHExposure*j.Beta* IF(a.side='L', 1, -1) AS BetaExposure,
                a.RHExposure* IF(a.side='L', 1, -1) AS Exposure
    FROM `t05PortfolioResponsibilities` a
    LEFT JOIN
      (SELECT a.adviseDate, a.code, a.beta
       FROM t08AdvisorTag a
       INNER JOIN (SELECT MAX(adviseDate) AS MaxOfDate, code
                   FROM `t08AdvisorTag` GROUP BY code) b ON a.adviseDate=b.MaxOfDate AND a.code=b.code) j ON a.quick = j.code
    WHERE a.processDate > '%s' AND a.processDate < '%s'
    #AND a.advisor = '%s'
    AND a.quick = '%s'
    ;''' % (g.start_date, g.end_date, g.param_adviser, quick), g.con, coerce_float=True, parse_dates=['processDate'])

    attr_df = (sql_pl_df.groupby(['firstTradeDate', 'side'])
               .sum()[['RHAttr']]
               .unstack()
               )

    attr_df.columns = attr_df.columns.get_level_values(1)

    long_count = attr_df['L'].count()
    short_count = attr_df['S'].count()
    total_count = long_count + short_count
    long_win_ratio = attr_df[attr_df > 0]['L'].count() * 1.0 / long_count
    short_win_ratio = attr_df[attr_df > 0]['S'].count() * 1.0 / short_count

    total_ratio = attr_df[attr_df > 0]['L'].count() * 1.0 + attr_df[attr_df > 0]['S'].count() / total_count

    print(long_win_ratio, short_win_ratio, total_ratio)

    df2 = sql.read_sql('''SELECT b.priceDate, b.close
                          FROM `t07Index` a, `t06DailyIndex` b
                          WHERE a.indexID = b.indexID
                          AND b.priceDate >= DATE_SUB('%s', INTERVAL 1 DAY) AND b.priceDate <= '%s'
                          AND a.indexCode = 'TPX';''' % (sql_pl_df['processDate'].min().strftime('%Y-%m-%d'),
                                                         sql_pl_df['processDate'].max().strftime('%Y-%m-%d')),
                       g.con,
                       parse_dates=['priceDate'],
                       index_col='priceDate')

    index_return = df2.pct_change().dropna()

    bexposure = sql_pl_df.set_index(['processDate', 'firstTradeDate', 'side'])[['BetaExposure']].unstack()
    bexposure.columns = bexposure.columns.get_level_values(1)
    beta_exposure = bexposure.shift(1).fillna(0)

    exposure = sql_pl_df.set_index(['processDate', 'firstTradeDate', 'side'])[['Exposure']].unstack()
    exposure.columns = exposure.columns.get_level_values(1)
    exposure = exposure.shift(1).fillna(0)

    attribution = (sql_pl_df.set_index(['processDate', 'firstTradeDate', 'side'])[['RHAttr']]
                   .unstack()
                   .fillna(0))

    attribution.columns = attribution.columns.get_level_values(1)

    alpha = (attribution.subtract(beta_exposure.mul(index_return['close'], axis='index', level=0),
                                  axis='index', level=0)
                        .dropna())

    alpha_hit = alpha.groupby(axis=0, level=1).sum()
    alpha_long_hit = alpha_hit[alpha_hit > 0]['L'].count() * 1.0 / attr_df['L'].count()
    alpha_short_hit = alpha_hit[alpha_hit > 0]['S'].count() * 1.0 / attr_df['S'].count()

    op = attribution.subtract(exposure.mul(index_return['close'], axis='index', level=0), axis='index', level=0)

    op_hit = op.dropna().groupby(axis=0, level=1).sum()
    op_long_hit_ratio = op_hit[op_hit > 0]['L'].count() * 1.0 / attr_df['L'].count()
    op_short_hit_ratio = op_hit[op_hit > 0]['S'].count() * 1.0 / attr_df['S'].count()

    render_obj = dict()
    render_obj['long_pl'] = attr_df.sum()['L']
    render_obj['short_pl'] = attr_df.sum()['S']
    render_obj['long_alpha'] = alpha.sum()['L']
    render_obj['short_alpha'] = alpha.sum()['S']
    render_obj['long_count'] = long_count
    render_obj['short_count'] = short_count
    render_obj['long_pl_win'] = long_win_ratio
    render_obj['short_pl_win'] = short_win_ratio
    render_obj['total_pl_win'] = total_ratio

    return render_template('tradehistory/result.html', params=render_obj)
