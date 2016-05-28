from flask import request, g, render_template

import pandas as pd
from pandas import DataFrame
from pandas.io import sql
import pymysql
from datetime import datetime

from . import tradehistory


@tradehistory.before_request
def before_request():
    g.con = pymysql.connect(host='localhost', user='root', passwd='root', db='hkg02p')
    # g.start_date = datetime(datetime.now().year-2, 12, 31).strftime('%Y-%m-%d')
    g.start_date = '2012-01-01'
    g.end_date = datetime.now().strftime('%Y-%m-%d')  # not include
    g.param_adviser = 'AP'
    g.lineWidth = 3
    g.markerSize = 7
    g.thinLineWidth = 2


@tradehistory.teardown_request
def teardown_request():
    con = getattr(g, 'con', None)
    if con is not None:
        con.close()


@tradehistory.route('/')
def index():
    return render_template('tradehistory/index.html')


def sum_long_short(df):
    l = df.L if 'L' in df else 0
    s = df.S if 'S' in df else 0
    return l + s


@tradehistory.route('/check')
def check():
    quick = request.args.get('quick', '7203')

    sql_pl_df = sql.read_sql('''SELECT processDate,advisor, side, a.quick, attribution,
                RHAttribution AS RHAttr,
                YAAttribution AS YAAttr,
                LRAttribution AS LRAttr,
                IF (side = 'L', firstTradeDateLong, firstTradeDateShort) AS firstTradeDate,
                a.RHExposure*j.Beta* IF(a.side='L', 1, -1) AS BetaExposure,
                a.RHExposure* IF(a.side='L', 1, -1) AS Exposure,
                a.quantity
    FROM `t05PortfolioResponsibilities` a
    INNER JOIN t01Instrument c ON a.instrumentID=c.instrumentID
    LEFT JOIN
      (SELECT a.adviseDate, a.code, a.beta
       FROM t08AdvisorTag a
       INNER JOIN (SELECT MAX(adviseDate) AS MaxOfDate, code
                   FROM `t08AdvisorTag` 
                   WHERE code = '%s') b ON a.adviseDate=b.MaxOfDate AND a.code='%s') j ON a.quick = j.code
    WHERE a.processDate > '%s' AND a.processDate < '%s'
    #AND a.advisor = '%s'
    AND c.quick = '%s'
    ;''' % (quick, quick, g.start_date,
            g.end_date, g.param_adviser, quick), g.con, coerce_float=True, parse_dates=['processDate'])

    if sql_pl_df['processDate'].count() == 0:
        return "Sorry, no RH position for this code."

    t = sql_pl_df['quantity'] - sql_pl_df['quantity'].shift(1)
    bl = DataFrame(index=sql_pl_df.index, columns=['BuySell'])
    bl[t < 0] = 'SELL'
    bl.iloc[0] = 'BUY' if (sql_pl_df['quantity'].iloc[0] > 0) else 'SELL'
    bl[t > 0] = 'BUY'
    sql_pl_df['BuySell'] = bl.dropna()['BuySell']

    attr_df = (sql_pl_df.groupby(['firstTradeDate', 'side'])
               .sum()[['RHAttr']]
               .unstack()
               )

    attr_df.columns = attr_df.columns.get_level_values(1)

    long_count = attr_df['L'].count() if 'L' in attr_df.columns else 0
    short_count = attr_df['S'].count() if 'S' in attr_df.columns else 0
    pl_long_hit = attr_df[attr_df > 0]['L'].count() * 1.0 if 'L' in attr_df.columns else 0
    pl_short_hit = attr_df[attr_df > 0]['S'].count() * 1.0 if 'S' in attr_df.columns else 0
    total_count = long_count + short_count
    long_win_ratio = pl_long_hit / long_count if long_count > 0 else 0
    short_win_ratio = pl_short_hit / short_count if short_count > 0 else 0

    total_ratio = pl_long_hit + pl_short_hit / total_count if total_count > 0 else 0

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

    bexposure = sql_pl_df.set_index(['processDate', 'firstTradeDate', 'advisor', 'side'])[['BetaExposure']].unstack()
    bexposure.columns = bexposure.columns.get_level_values(1)
    beta_exposure = bexposure.shift(1).fillna(0)

    exposure = sql_pl_df.set_index(['processDate', 'firstTradeDate', 'advisor', 'side'])[['Exposure']].unstack()
    exposure.columns = exposure.columns.get_level_values(1)
    exposure = exposure.shift(1).fillna(0)

    attribution = (sql_pl_df.set_index(['processDate', 'firstTradeDate', 'advisor', 'side'])[['RHAttr']]
                   .unstack()
                   .fillna(0))

    attribution.columns = attribution.columns.get_level_values(1)

    alpha = (attribution.subtract(beta_exposure.mul(index_return['close'], axis='index', level=0),
                                  axis='index', level=0)
                        .dropna())

    trade_alpha = (alpha.dropna()
                        .groupby(axis=0, level=1)
                        .sum()
                        .assign(Alpha=sum_long_short)[['Alpha']]
                   )

    alpha_hit = alpha.groupby(axis=0, level=1).sum()
    alpha_long_hit = alpha_hit[alpha_hit > 0]['L'].count() * 1.0 if long_count > 0 else 0
    alpha_short_hit = alpha_hit[alpha_hit > 0]['S'].count() * 1.0 if short_count > 0 else 0
    alpha_long_ratio = alpha_long_hit / long_count if long_count > 0 else 0
    alpha_short_ratio = alpha_short_hit / short_count if short_count > 0 else 0

    op = (attribution.subtract(exposure.mul(index_return['close'], axis='index', level=0), axis='index', level=0)
                     .dropna())

    trade_op = (op.dropna()
                  .groupby(axis=0, level=1)
                  .sum()
                  .assign(OP=sum_long_short)[['OP']]
                )
    op_hit = op.groupby(axis=0, level=1).sum()
    op_long_hit = op_hit[op_hit > 0]['L'].count() * 1.0 if long_count > 0 else 0
    op_short_hit = op_hit[op_hit > 0]['S'].count() * 1.0 if short_count > 0 else 0
    op_long_hit_ratio = op_long_hit / long_count if long_count > 0 else 0
    op_short_hit_ratio = op_short_hit / short_count if short_count > 0 else 0

    tbl = (sql_pl_df.groupby(['firstTradeDate', 'side'])
           .sum()[['RHAttr']]
           .sort_index(ascending=False)
           )

    day_count = sql_pl_df.groupby(['firstTradeDate', 'side']).count()['processDate']
    tbl['Days'] = day_count
    tbl['Analyst'] = sql_pl_df.groupby(['firstTradeDate', 'side']).apply(lambda subf: subf['advisor'].iloc[0])
    tbl['RHAttr'] = tbl['RHAttr'].map(lambda x: '{:.2f}%'.format(x * 100))
    tbl = (tbl.reset_index()
              .set_index('firstTradeDate')
              .merge(trade_alpha, left_index=True, right_index=True)
              .merge(trade_op, left_index=True, right_index=True)
              .sort_index(ascending=False)
           )

    tbl.index.names = ['Date']
    tbl = tbl.reset_index()

    tbl['Alpha'] = tbl['Alpha'].map(lambda x: '{:.2f}%'.format(x * 100))
    tbl['OP'] = tbl['OP'].map(lambda x: '{:.2f}%'.format(x * 100))
    tbl = tbl[['Date', 'side', 'Analyst', 'Days', 'RHAttr', 'Alpha', 'OP']]
    tbl = tbl.rename(columns={'side': 'Side', 'RHAttr': 'PL'})
    tbl_html = tbl.to_html(index=False, classes='table').replace('border="1"', 'border="0"')

    df3 = sql.read_sql('''SELECT a.priceDate, a.close
                          FROM `t06DailyPrice` a
                          INNER JOIN t01Instrument b ON b.instrumentID = a.instrumentID
                          WHERE a.priceDate >= DATE_SUB('%s', INTERVAL 1 DAY) 
                          AND a.priceDate <= '%s'
                          AND b.quick='%s';''' % (sql_pl_df['processDate'].min().strftime('%Y-%m-%d'),
                                                  sql_pl_df['processDate'].max().strftime('%Y-%m-%d'), quick
                                                  ), g.con, parse_dates=['priceDate'], index_col='priceDate')

    pricedf = (sql_pl_df.set_index('processDate')
                        .merge(df3, left_index=True, right_index=True)
                        .dropna()[['side', 'BuySell', 'close']])

    ratedf = df3.div(df2)
    ratiodf = (sql_pl_df.set_index('processDate')
                        .merge(df3.div(df2), left_index=True, right_index=True)
                        .dropna()[['side', 'BuySell', 'close']])

    long_price_graph = {'data': [{
                'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in df3.index],
                'y': df3[col].values.tolist(),
                'name':'Stock',
                'line': {'width': g.lineWidth,
                         'color': "rgb(182, 182, 182)"
                         }
            } for col in df3.columns
            ]+[{
                'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d')
                      for i in pricedf[(pricedf['side'] == 'L') & (pricedf['BuySell'] == 'BUY')].index],
                'y': pricedf[(pricedf['side'] == 'L') & (pricedf['BuySell'] == 'BUY')]['close'].values.tolist(),
                'mode': 'markers',
                'name': 'Buy Long',
                'marker': {
                    'color': 'rgb(27, 93, 225)',
                    'size': g.markerSize
                }
                }]+[{
                    'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d')
                          for i in pricedf[(pricedf['side'] == 'L') & (pricedf['BuySell'] == 'SELL')].index],
                    'y': pricedf[(pricedf['side'] == 'L') & (pricedf['BuySell'] == 'SELL')]['close'].values.tolist(),
                    'mode': 'markers',
                    'name': 'Sell Long',
                    'marker': {
                        'color': 'rgb(214,39,40)',
                        'size': g.markerSize
                    }
                }],
            'layout': {
                'margin': {'l': 40, 'r': 40},
                # 'width': 750,
                # 'height': 240,
                'legend': {'font': {'size': 10}, 'x': 1.05}
            }
    }

    long_rate_graph = {'data': [{
                'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in ratedf.index],
                'y': ratedf.fillna(method="ffill")[col].dropna().values.tolist(),
                'name':'Ratio',
                'line': {'width': g.lineWidth,
                         'color': "rgb(182, 182, 182)"
                         }
            } for col in ratedf.columns
            ]+[{
                'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d')
                      for i in ratiodf[(ratiodf['side'] == 'L') & (ratiodf['BuySell'] == 'BUY')].index],
                'y': ratiodf.fillna(method="ffill")[(ratiodf['side'] == 'L') &
                                                    (ratiodf['BuySell'] == 'BUY')]['close'].values.tolist(),
                'mode': 'markers',
                'name': 'BL Ratio',
                'marker': {
                    'color': 'rgb(27, 93, 225)',
                    'size': g.markerSize
                }
                }]+[{
                    'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d')
                          for i in ratiodf[(ratiodf['side'] == 'L') & (ratiodf['BuySell'] == 'SELL')].index],
                    'y': ratiodf.fillna(method="ffill")[(ratiodf['side'] == 'L') &
                                                        (ratiodf['BuySell'] == 'SELL')]['close'].values.tolist(),
                    'mode': 'markers',
                    'name': 'SL Ratio',
                    'marker': {
                        'color': 'rgb(214,39,40)',
                        'size': g.markerSize
                    }
                }],
            'layout': {
                'margin': {'l': 40, 'r': 40},
                # 'width': 750,
                # 'height': 240,
                'legend': {'font': {'size': 10}, 'x': 1.05}
            }
    }

    short_price_graph = {'data': [{
                'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in df3.index],
                'y': df3[col].values.tolist(),
                'name':'Stock',
                'line': {'width': g.lineWidth,
                         'color': "rgb(182, 182, 182)"
                         }
            } for col in df3.columns
            ]+[{
                'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d')
                      for i in pricedf[(pricedf['side'] == 'S') & (pricedf['BuySell'] == 'BUY')].index],
                'y': pricedf[(pricedf['side'] == 'S') & (pricedf['BuySell'] == 'BUY')]['close'].values.tolist(),
                'mode': 'markers',
                'name': 'Buy Cover',
                'marker': {
                    'color': 'rgb(27, 93, 225)',
                    'size': g.markerSize
                }
                }]+[{
                    'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d')
                          for i in pricedf[(pricedf['side'] == 'S') & (pricedf['BuySell'] == 'SELL')].index],
                    'y': pricedf[(pricedf['side'] == 'S') & (pricedf['BuySell'] == 'SELL')]['close'].values.tolist(),
                    'mode': 'markers',
                    'name': 'Sell Short',
                    'marker': {
                        'color': 'rgb(214,39,40)',
                        'size': g.markerSize
                    }
                }],
            'layout': {
                'margin': {'l': 40, 'r': 40},
                # 'width': 750,
                # 'height': 240,
                'legend': {'font': {'size': 10}, 'x': 1.05}
            }
    }

    short_rate_graph = {'data': [{
                'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in ratedf.index],
                'y': ratedf.fillna(method="ffill")[col].dropna().values.tolist(),
                'name':'Ratio',
                'line': {'width': g.lineWidth,
                         'color': "rgb(182, 182, 182)"
                         }
            } for col in df3.columns
            ]+[{
                'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d')
                      for i in ratiodf[(ratiodf['side'] == 'S') & (ratiodf['BuySell'] == 'BUY')].index],
                'y': ratiodf[(ratiodf['side'] == 'S') & (ratiodf['BuySell'] == 'BUY')]['close'].values.tolist(),
                'mode': 'markers',
                'name': 'BC Ratio',
                'marker': {
                    'color': 'rgb(27, 93, 225)',
                    'size': g.markerSize
                }
                }]+[{
                    'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d')
                          for i in ratiodf[(ratiodf['side'] == 'S') & (ratiodf['BuySell'] == 'SELL')].index],
                    'y': ratiodf[(ratiodf['side'] == 'S') & (ratiodf['BuySell'] == 'SELL')]['close'].values.tolist(),
                    'mode': 'markers',
                    'name': 'SS Ratio',
                    'marker': {
                        'color': 'rgb(214,39,40)',
                        'size': g.markerSize
                    }
                }],
            'layout': {
                'margin': {'l': 40, 'r': 40},
                # 'width': 750,
                # 'height': 240,
                'legend': {'font': {'size': 10}, 'x': 1.05}
            }
    }

    render_obj = dict()
    render_obj['long_count'] = long_count
    render_obj['short_count'] = short_count
    render_obj['long_pl'] = attr_df.sum()['L'] if 'L' in attr_df.columns else 0
    render_obj['short_pl'] = attr_df.sum()['S'] if 'S' in attr_df.columns else 0
    render_obj['long_op'] = op.sum()['L'] if 'L' in op.columns else 0
    render_obj['short_op'] = op.sum()['S'] if 'S' in op.columns else 0
    render_obj['long_alpha'] = alpha.sum()['L'] if 'L' in alpha.columns else 0
    render_obj['short_alpha'] = alpha.sum()['S'] if 'S' in alpha.columns else 0
    render_obj['long_count'] = long_count
    render_obj['short_count'] = short_count
    render_obj['long_pl_win'] = long_win_ratio
    render_obj['short_pl_win'] = short_win_ratio
    render_obj['total_pl_win'] = total_ratio
    render_obj['long_a_win'] = alpha_long_ratio
    render_obj['short_a_win'] = alpha_short_ratio
    render_obj['long_op_win'] = op_long_hit_ratio
    render_obj['short_op_win'] = op_short_hit_ratio
    render_obj['table'] = tbl_html
    render_obj['long_price_graph'] = long_price_graph
    render_obj['long_rate_graph'] = long_rate_graph
    render_obj['short_price_graph'] = short_price_graph
    render_obj['short_rate_graph'] = short_rate_graph

    return render_template('tradehistory/result.html', params=render_obj, justify='right')
