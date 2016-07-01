from flask import request, g, render_template

import pandas as pd
from pandas import DataFrame, Series
from pandas.io import sql
import numpy as np
import pymysql
from datetime import datetime, timedelta
from decimal import *
import urllib
from . import tradehistory
import re
from dateutil.parser import parse
import pymysql.cursors


@tradehistory.before_request
def before_request():
    g.con = pymysql.connect(host='192.168.1.147', user='uploader', passwd='fA6ZxopGrbdb')
        #(host='localhost', user='root', passwd='root', db='hkg02p')
        #(host='192.168.1.147', user='uploader', passwd='fA6ZxopGrbdb', db='hkg02p')
    # g.start_date = datetime(datetime.now().year-2, 12, 31).strftime('%Y-%m-%d')
    g.start_date = '2012-01-01'
    g.end_date = datetime.now().strftime('%Y-%m-%d')  # not include
    g.param_adviser = 'AP'
    g.lineWidth = 3
    g.markerSize = 7
    g.thinLineWidth = 2
    g.left_margin = 60


@tradehistory.teardown_request
def teardown_request(exception):
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


def get_ratio_df(trade_df, df2, buysell, longshort):
    ratiodf = (trade_df.sort_index()
                       .loc[pd.IndexSlice[:,'RH',buysell,longshort], ['price']]
                       .div(df2,axis=0, level=0)
                       .reset_index()[['tradeDate', 'price']]
                       .set_index('tradeDate')
              )
    return ratiodf


def get_trade_pricedf(trade_df, buysell, longshort):
    ret = (trade_df.sort_index()
                   .loc[pd.IndexSlice[:,'RH',buysell,longshort], 'price']
                   .reset_index()[['tradeDate','price']]
                   .set_index('tradeDate')
           )
    return ret


def change_fund_code(df):
    cols = 'fundCode'

    def get_fund_name(x):
        return 'RH' if x == '04F08900' else ('YA' if x == '04F08910' else ('LR' if x == 'PLOJ2010' else 'Unknown'))

    fund_name = df[cols].apply(get_fund_name)
    df = df.copy()
    df[cols] = fund_name
    return df


def get_pl_df(quick, start_date, end_date, con):
    pl_df = sql.read_sql('''
                SELECT processDate,advisor, side, a.quick, attribution,
                            RHAttribution AS RHAttr,
                            YAAttribution AS YAAttr,
                            LRAttribution AS LRAttr,
                            IF (side = 'L', firstTradeDateLong, firstTradeDateShort) AS firstTradeDate,
                            a.RHExposure*j.Beta* IF(a.side='L', 1, -1) AS RHBetaExposure,
                            a.YAExposure*j.Beta* IF(a.side='L', 1, -1) AS YABetaExposure,
                            a.LRExposure*j.Beta* IF(a.side='L', 1, -1) AS LRBetaExposure,
                            a.RHExposure* IF(a.side='L', 1, -1) AS RHExposure,
                            a.YAExposure* IF(a.side='L', 1, -1) AS YAExposure,
                            a.LRExposure* IF(a.side='L', 1, -1) AS LRExposure,
                            a.name
                FROM hkg02p.t05PortfolioResponsibilities a
                LEFT JOIN
                  (SELECT a.adviseDate, a.code, a.beta
                   FROM hkg02p.t08AdvisorTag a
                   WHERE a.adviseDate IN (SELECT MAX(adviseDate) AS MaxOfDate FROM hkg02p.t08AdvisorTag WHERE code = '%s')
                        AND a.code='%s') j ON a.quick = j.code
                WHERE a.processDate >= '%s' AND a.processDate <= '%s'
                AND a.quick = '%s'
    ;''' % (quick, quick, start_date, end_date, quick), con, coerce_float=True, parse_dates=['processDate'])

    return pl_df


def get_trade_df(stock_price_df, quick, start_date, end_date, con):
    trade_df = (sql.read_sql('''SELECT  a.tradeDate, a.fundCode, a.orderType, a.side, AVG(a.price) as price
                    FROM hkg02p.t08Reconcile a
                    WHERE a.code = '%s'
                      AND a.srcFlag='K'
                      AND a.status='A'
                      AND a.processDate >= '%s'
                      AND a.processDate <= '%s'
                      GROUP BY a.tradeDate, a.fundCode, a.orderType, a.side
                      ORDER BY reconcileID
                    ;
        ;''' % (quick, start_date, end_date), con, coerce_float=True, parse_dates=['tradeDate'])
                .pipe(change_fund_code)
                .set_index(['tradeDate', 'fundCode', 'orderType', 'side'])
                )
    adj_factor_df = stock_price_df.reindex(trade_df.index.levels[0])['adj_factor']
    return trade_df.div(adj_factor_df, axis=0, level=0)


def get_index_df(start_date, end_date, con):
    index_df = sql.read_sql('''SELECT b.priceDate,a.indexCode, b.close
        FROM hkg02p.t07Index a, hkg02p.t06DailyIndex b
        WHERE a.indexID = b.indexID
        AND b.priceDate >= DATE_SUB('%s', INTERVAL 1 DAY) AND b.priceDate <= '%s'
        AND a.indexCode IN ('TPX','KOSPI','TWSE','HSCEI');''' % (start_date, end_date), con, parse_dates=['priceDate'])

    p_index_df = index_df.pivot('priceDate', 'indexCode', 'close')
    p_index_df.fillna(method='ffill', inplace=True)
    return p_index_df


def get_stock_price_df(quick, start_date, end_date, con):
    price_df = sql.read_sql('''SELECT a.priceDate, a.close AS close, a.adj_factor
        FROM hkg02p.t06DailyPrice a
        INNER JOIN hkg02p.t01Instrument b ON b.instrumentID = a.instrumentID
        WHERE a.priceDate >= DATE_SUB('%s', INTERVAL 1 DAY)
        AND a.priceDate <= '%s'
        AND b.quick='%s';''' % (start_date, end_date, quick), con, parse_dates=['priceDate'], index_col='priceDate')

    return price_df

def get_index_name(quick,con):
    result = sql.read_sql('''SELECT b.bbgCode
        FROM hkg02p.t01Instrument a
        INNER JOIN hkg02p.t02Exchange b ON a.exchangeID=b.exchangeID
        WHERE a.quick='%s'
        ;
    ''' % quick, con).iloc[0,:].values[0]
    
    mapping = {
        'JP': 'TPX',
        'HK': 'HSCEI',
        'TW': 'TWSE',
        'KS': 'KOSPI'
    }
    return mapping[result]


def parse_text(text):
    """
    parse text into a dict of catalyst for each instrument
    """

    date_list = re.split("^\s*\n", text, flags=re.MULTILINE)
        
    return date_list


def parse_catalyst(entry, date):
    """
    parse catalyst for each instrument on some day
    """
    ret = dict()
    l = re.split('\s\s+', entry)
    if entry.find('H-20') > -1 or len(l) < 2:
        return None
    
    ret['processDate'] = date
    ret['personCode'] = l[1].split('(')[-1].split(')')[0] if l[1].find(')') > -1 else ''
    ret['code'] = l[1].split(']')[0][2:]
    ret['commentText'] = ''

    if (len(l) > 3):
        ret['catalystText'] = l[3]
        #ret['recommendation'] = l[2]
    elif l[1].find(')') > -1 and len(l[1].split(')')[1]) > 0 and len(l)>2:
        ret['catalystText'] = l[2]
        #ret['recommendation'] = l[1].split(')')[1].strip()
    elif len(l) > 2 and len(l[2]) >= 24:
        ret['catalystText'] = l[2][24:]
    else:
        ret['catalystText'] = ''
        #ret['recommendation'] = '' 

    return ret


def get_wiki_df(quick, start_date, con):

    t = sql.read_sql('''
                SELECT DISTINCT processDate, a.personCode,
                catalystText,
                REPLACE(commentText, '\n', '<br>') AS commentText
                FROM hkg02p.t01Person a, noteDB.Note N
                WHERE a.personID = N.personID AND N.code='%s'
                AND processDate >= '%s'
                #AND commentText > ''
                ORDER BY processDate DESC
    ;''' % (quick, start_date), con , parse_dates=['processDate']) 

    with g.con.cursor() as cursor:
        # Read a single record
        sqlstr = """SELECT p.page_title, t.old_text, r.rev_id, r.rev_user, r.rev_timestamp, p.page_id
                FROM wikidbTKY.page p
                INNER JOIN wikidbTKY.revision r ON p.page_id=r.rev_page 
                    AND r.rev_id = (SELECT MAX(rev_id) FROM wikidbTKY.revision WHERE rev_page=p.page_id)
                INNER JOIN wikidbTKY.text t ON t.old_id = r.rev_text_id
                #WHERE p.page_id IN (20,5701, 4616, 2831, 2826, 1756, 1751) 
                WHERE p.page_id IN (20,5701, 4616, 2831, 2826)
        """

        cursor.execute(sqlstr)
        sql_result = cursor.fetchall()

    result = "".join([row[1] for row in sql_result])

    rets = []

    for e in parse_text(result):
        wikis = [wiki for wiki in e.split('\n') if wiki.strip() != '' and wiki.find('Recommendation') == -1]
        
        if len(wikis) > 2 and wikis[0].find('[[') == -1:
            date = parse(wikis[0].strip())
            for wiki in wikis[1:]:
                w = parse_catalyst(wiki, date)
                if w != None and w['code'] == quick:
                    rets.append(w)
        elif len(wikis) <= 1 and wikis[0].find('[[') == -1:
            date = parse(wikis[0].strip())
        elif len(wikis) >= 1 and wikis[0].find('[[') > -1:
            for wiki in wikis:
                w = parse_catalyst(wiki, date)
                if w != None and w['code'] == quick:
                    rets.append(w) 

    old_wiki = (DataFrame(rets)[['processDate', 'personCode', 'catalystText', 'commentText']]
                .set_index('processDate').sort_index(ascending=False).reset_index())  if len(rets) > 0 else DataFrame()
    
    return pd.concat([t, old_wiki], ignore_index=True)


def make_table(df, wiki_df, quick, numcol, table_caption=''):
    '''
    turn DataFrame into HTML table
    '''
    count = 0
    table_html = ''
    style = 'class="table"'
    field_separator = '#'
    #df.index = df.index.map(lambda x: str(x)[:12])
    rows = df.to_csv(sep=field_separator).split('\n')
    wiki_rows = wiki_df.to_csv(sep=field_separator, header=False).split('\n')
    
    table_header = ('<table %s><thead><tr>' % style) + ''.join(
            ['<th>' + h + '</th>' for h in rows[0].split(field_separator)[1:numcol+1]]) + '</tr></thead>'

    if table_caption != '':
        table_header += '<caption>' + table_caption + '</caption><tbody>'
    else:
        table_header += '<tbody>'

    table_html += table_header

    total_rows = 1
    i = 1
    j = 0
    wiki_count = 0
    wiki_r = wiki_rows[j].split(field_separator) if j < len(wiki_rows) else []
        #print(wiki_r)
    wiki_date = datetime.strptime(wiki_r[1], '%Y-%m-%d') if len(wiki_r) > 1 else datetime.now()
    
    while i < len(rows):
        r = rows[i]
        if r != '':
            elements = r.split(field_separator)
            start_date = datetime.strptime(elements[1], '%Y-%m-%d')
            end_date = datetime.strptime(elements[numcol+1], '%Y-%m-%d')
            
            while wiki_date > end_date and j < len(wiki_rows):
                #print 'over', wiki_r, wiki_date, end_date
                j +=1
                wiki_r = wiki_rows[j].split(field_separator) if j < len(wiki_rows) else []
                wiki_date = datetime.strptime(wiki_r[1], '%Y-%m-%d') if len(wiki_r) > 1 else datetime.now()
            
            link_date = wiki_date
            while wiki_date >= start_date and wiki_date <= end_date:
                wiki_count += 1
                #print 'ok', wiki_r
                j += 1
                link_date = wiki_date
                wiki_r = wiki_rows[j].split(field_separator) if j < len(wiki_rows) else []
                wiki_date = datetime.strptime(wiki_r[1], '%Y-%m-%d') if len(wiki_r) > 1 else datetime.now()
            
            #print(start_date, end_date, wiki_count)
            if wiki_date >= start_date - timedelta(2) and wiki_date.strftime('%Y-%m-%d') != datetime.now().strftime('%Y-%m-%d'):
                link_date = wiki_date

            if wiki_count > 0:
                table_html += '<tr>' + ''.join([('<th><a href="/wiki/?quick=%s&start=%s&end=%s&open=%s#%s" onclick="var left = window.screen.width/2 -500/2; window.open(this.href, \'Wiki\', \'width=500, height=500, left=\'+left+\',top=100, resizable,location=no\'); return false;">' % 
                                             (urllib.quote(quick), g.start_date, g.end_date, elements[1].title(), link_date.strftime('%Y-%m-%d'))) + elements[1].title() + 
                                                 '</a></th>'] + 
                                           ['<td>' + h + '</td>' for h in  elements[2:numcol+1] ]) + '</tr>'
            else:
                table_html += '<tr>' + ''.join(['<th>' + elements[1].title() + '</th>'] + 
                                           ['<td>' + h + '</td>' for h in  elements[2:numcol+1] ]) + '</tr>'
            wiki_count = 0
        i += 1
    table_html += '</tbody></table>'

    return table_html

@tradehistory.route('/check')
def check():
    quick = request.args.get('quick', '7203')

    sql_pl_df = get_pl_df(quick, g.start_date, g.end_date, g.con)

    if sql_pl_df['processDate'].count() == 0:
        return "Sorry, no position for this code."

    index_name = get_index_name(quick, g.con)

    position_name = sql_pl_df.loc[0, 'name']

    attr_df = (sql_pl_df.groupby(['firstTradeDate', 'side'])
               .sum()[['RHAttr', 'YAAttr', 'LRAttr']]
               .unstack()
               )

    pl_hit = dict()
    for col in ['RHAttr', 'YAAttr', 'LRAttr']:
        pl_hit[col] = dict()
        pl_hit[col]['long_count'] = attr_df[col]['L'][attr_df[col]['L'] <> 0].count() if 'L' in attr_df[col].columns else 0
        pl_hit[col]['short_count'] = attr_df[col]['S'][attr_df[col]['S'] <> 0].count() if 'S' in attr_df[col].columns else 0
        pl_hit[col]['long_hit'] = attr_df[col][attr_df[col] > 0]['L'].count() * 1.0 if 'L' in attr_df[col].columns else 0
        pl_hit[col]['short_hit'] = attr_df[col][attr_df[col] > 0]['S'].count() * 1.0 if 'S' in attr_df[col].columns else 0
        pl_hit[col]['total_count'] = pl_hit[col]['long_count'] + pl_hit[col]['short_count']
        pl_hit[col]['long_ratio'] =  pl_hit[col]['long_hit'] / pl_hit[col]['long_count'] if pl_hit[col]['long_count'] > 0 else 0
        pl_hit[col]['short_ratio'] =  pl_hit[col]['short_hit'] / pl_hit[col]['short_count'] if pl_hit[col]['short_count'] > 0 else 0
        pl_hit[col]['total_ratio'] = (pl_hit[col]['long_hit'] + pl_hit[col]['short_hit']) / pl_hit[col]['total_count'] if pl_hit[col]['total_count'] > 0 else 0
 
    df2 = get_index_df(sql_pl_df['processDate'].min().strftime('%Y-%m-%d'),
                       sql_pl_df['processDate'].max().strftime('%Y-%m-%d'),
                       g.con)

    index_return = df2.pct_change().dropna()

    bexposure = sql_pl_df.set_index(['processDate', 'firstTradeDate', 'advisor', 'side'])[['RHBetaExposure', 'YABetaExposure', 'LRBetaExposure']].unstack()
    beta_exposure = bexposure.shift(1).fillna(0)

    exposure = sql_pl_df.set_index(['processDate', 'firstTradeDate', 'advisor', 'side'])[['RHExposure', 'YAExposure', 'LRExposure']].unstack()
    exposure = exposure.shift(1).fillna(0)

    attribution = (sql_pl_df.set_index(['processDate', 'firstTradeDate', 'advisor', 'side'])[['RHAttr', 'YAAttr', 'LRAttr']]
                        .unstack()
                        .fillna(0)
              ) 

    be = beta_exposure.mul(index_return[index_name], axis='index', level=0)
    alpha_df = dict()
    alpha_df['RH'] = attribution['RHAttr'].subtract(be['RHBetaExposure'])
    alpha_df['YA'] = attribution['YAAttr'].subtract(be['YABetaExposure'])
    alpha_df['LR'] = attribution['LRAttr'].subtract(be['LRBetaExposure']) 

    rh_alpha = (alpha_df['RH'].dropna()
                              .groupby(axis=0, level=1)
                              .sum()
                              .assign(Alpha = sum_long_short)[['Alpha']]
                )

    long_count = pl_hit['RHAttr']['long_count']
    short_count = pl_hit['RHAttr']['short_count']

    alpha = dict()
    for f in ['RH', 'YA', 'LR']:
        alpha[f] = dict()
        alpha_hit = alpha_df[f].groupby(axis=0, level=1).sum()
        alpha[f]['long_hit'] = alpha_hit[alpha_hit > 0]['L'].count() * 1.0 if 'L' in alpha_hit and pl_hit[f+'Attr']['long_count'] > 0 else 0
        alpha[f]['short_hit'] = alpha_hit[alpha_hit > 0]['S'].count() * 1.0 if 'S' in alpha_hit and pl_hit[f+'Attr']['short_count'] > 0 else 0
        alpha[f]['long_ratio'] = alpha[f]['long_hit'] / pl_hit[f+'Attr']['long_count'] if pl_hit[f+'Attr']['long_count'] > 0 else 0
        alpha[f]['short_ratio'] = alpha[f]['short_hit'] / pl_hit[f+'Attr']['short_count'] if pl_hit[f+'Attr']['short_count'] > 0 else 0 
        alpha[f]['total_ratio'] = ( (alpha[f]['long_hit']+alpha[f]['short_hit'])/pl_hit[f+'Attr']['total_count'] 
                                    if pl_hit[f+'Attr']['total_count'] > 0 else 0)

    op_df = dict()
    op_df['RH'] = attribution['RHAttr'].subtract(exposure['RHExposure'].mul(index_return[index_name], axis='index', level=0))
    op_df['YA'] = attribution['YAAttr'].subtract(exposure['YAExposure'].mul(index_return[index_name], axis='index', level=0))
    op_df['LR'] = attribution['LRAttr'].subtract(exposure['LRExposure'].mul(index_return[index_name], axis='index', level=0))

    rh_op = (op_df['RH'].dropna()
                       .groupby(axis=0,level=1)
                       .sum()
                       .assign(OP = sum_long_short)[['OP']]
           )
    
    op_hit = dict()
    for f in ['RH', 'YA', 'LR']:
        op_hit[f] = dict()
        op_hit_df = op_df[f].groupby(axis=0, level=1).sum()
        op_hit[f]['long_hit'] = (op_hit_df[op_hit_df > 0]['L'].count() * 1.0 
                                 if 'L' in op_hit_df and pl_hit[f+'Attr']['long_count'] > 0 else 0)
        op_hit[f]['short_hit'] = (op_hit_df[op_hit_df > 0]['S'].count() * 1.0 
                                  if 'S' in op_hit_df and pl_hit[f+'Attr']['short_count'] > 0 else 0)
        op_hit[f]['long_ratio'] = (op_hit[f]['long_hit'] / pl_hit[f+'Attr']['long_count'] 
                                       if  pl_hit[f+'Attr']['long_count'] > 0 else 0)
        op_hit[f]['short_ratio'] = (op_hit[f]['short_hit'] / pl_hit[f+'Attr']['short_count'] 
                                        if pl_hit[f+'Attr']['short_count'] > 0 else 0)
        op_hit[f]['total_ratio'] = ( (op_hit[f]['long_hit'] + op_hit[f]['short_hit'])/pl_hit[f+'Attr']['total_count']
                                        if pl_hit[f+'Attr']['total_count'] > 0 else 0 )

    tbl = (sql_pl_df.groupby(['firstTradeDate', 'side'])
                    .sum()[['RHAttr']]
                    .assign(Analyst=sql_pl_df.groupby(['firstTradeDate', 'side'])['advisor']
                                             .apply(lambda df: df.iloc[0])
                                             .values
                            )
           )

    day_count = sql_pl_df.groupby(['firstTradeDate', 'side']).count()['processDate']
    tbl['Days'] = day_count
    tbl['CloseDate'] = (sql_pl_df.groupby(['firstTradeDate', 'side'], sort=False)['processDate'].max())
    tbl['RHAttr'] = tbl['RHAttr'].map(lambda x: '{:.2f}%'.format(x * 100))
    tbl = (tbl.reset_index()
              .set_index('firstTradeDate')
              .merge(rh_alpha, left_index=True, right_index=True)
              .merge(rh_op, left_index=True, right_index=True)
              .sort_index(ascending=False)
           )

    tbl.index.names = ['Date']
    tbl = tbl.reset_index()
    tbl['Alpha'] = tbl['Alpha'].map(lambda x: '{:.2f}%'.format(x * 100))
    tbl['OP'] = tbl['OP'].map(lambda x: '{:.2f}%'.format(x * 100))
    tbl = tbl[['Date', 'side', 'Analyst', 'Days', 'RHAttr', 'Alpha', 'OP', 'CloseDate']]
    tbl = tbl.rename(columns={'side': 'Side', 'RHAttr': 'PL'})

    wiki_df = get_wiki_df(quick, g.start_date, g.con)
    
    tbl_html = make_table(tbl, wiki_df, quick, numcol=7)

    df3 = get_stock_price_df(quick,
                             sql_pl_df['processDate'].min().strftime('%Y-%m-%d'),
                             sql_pl_df['processDate'].max().strftime('%Y-%m-%d'),
                             g.con)

    trade_df = get_trade_df(df3, quick, g.start_date, g.end_date, g.con)

    test = trade_df.copy()
    test = (test.reset_index()[['tradeDate', 'orderType', 'side']]
                .drop_duplicates()
                .set_index('tradeDate')
                .assign(order = lambda df: df['orderType']+df['side'])[['order']]
                .groupby(level=0)
                .apply(lambda df: Series(dict(orderType= df['order'].str.cat(sep=","))))
           )
    
    order_df = DataFrame(test, index=test.index.union(wiki_df['processDate'].drop_duplicates())).fillna(method="bfill")
    
    ret_df = wiki_df.merge(order_df, left_on='processDate', right_index=True, how='left')

    ratedf = df3[['close']].div(df2[index_name], axis=0)

    long_price_graph = {'data': [{
                    'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in df3.index],
                    'y': df3['close'].values.tolist(),
                    'name':'Stock',
                    'hoverinfo': 'none',
                    'line': {'width':g.lineWidth,
                             'color': "rgb(182, 182, 182)" 
                             }
                } 
                ]+([{
                    'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') 
                          for i in get_trade_pricedf(trade_df, 'B', 'L').index],
                    'y': get_trade_pricedf(trade_df, 'B', 'L')['price'].fillna(method="ffill").values.tolist(),
                    'mode': 'markers',
                    'name': 'Buy Long',
                    'marker': {
                        'color': 'rgb(27, 93, 225)',
                        'size': g.markerSize
                    }
                    }] if 'RH' in trade_df.index.levels[1] and 
                           'B' in trade_df.index.levels[2] and 
                           'L' in trade_df.index.levels[3] else [])+([{
                    'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') 
                          for i in get_trade_pricedf(trade_df, 'S', 'L').index],
                    'y': get_trade_pricedf(trade_df, 'S', 'L')['price'].values.tolist(),
                    'mode': 'markers',
                    'name': 'Sell Long',
                    'marker': {
                        'color': 'rgb(214,39,40)',
                        'size':g.markerSize
                    }
                    }] if 'RH' in trade_df.index.levels[1] and 
                           'S' in trade_df.index.levels[2] and 
                           'L' in trade_df.index.levels[3] else [])+([{
                    'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') 
                          for i in get_trade_pricedf(trade_df, 'B', 'S').index],
                    'y': get_trade_pricedf(trade_df, 'B', 'S')['price'].fillna(method="ffill").values.tolist(),
                    'mode': 'markers',
                    'name': 'Buy Cover',
                    'marker': {
                        'color': 'rgb(121, 176, 255)',
                        'size':g.markerSize
                    }
                    }] if 'RH' in trade_df.index.levels[1] and 'B' in trade_df.index.levels[2] and 'S' in trade_df.index.levels[3] else [])+([{
                    'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') 
                          for i in get_trade_pricedf(trade_df, 'S', 'S').index],
                    'y': get_trade_pricedf(trade_df, 'S', 'S')['price'].values.tolist(),
                    'mode': 'markers',
                    'name': 'Sell Short',
                    'marker': {
                        'color': 'rgb(255,124,144)',
                        'size':g.markerSize
                    }
                    }] if 'RH' in trade_df.index.levels[1] and 'S' in trade_df.index.levels[2] and 'S' in trade_df.index.levels[3] else []),
                'layout': {
                    'margin': {'l': g.left_margin, 'r': 40},
                    #'width': 750,
                    #'height': 240,
                    'legend': {'font': {'size': 10}, 'x': 1.05}
                }
    } 

    long_rate_graph = {'data': [{
                    'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in ratedf.index],
                    'y': ratedf[col].dropna().values.tolist(),
                    'name':'Ratio',
                    'hoverinfo': 'none',
                    'line': {'width':g.lineWidth,
                             'color': "rgb(182, 182, 182)" 
                             }
                } for col in ratedf.columns
                ]+([{
                    'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') 
                          for i in get_ratio_df(trade_df, df2[index_name], 'B','L').index],
                    'y': get_ratio_df(trade_df, df2[index_name], 'B','L')['price'].fillna(method="ffill").values.tolist(),
                    'mode': 'markers',
                    'name': 'BL Ratio',
                    'marker': {
                        'color': 'rgb(27, 93, 225)',
                        'size':g.markerSize
                    }
                    }] if 'RH' in trade_df.index.levels[1] and 
                           'B' in trade_df.index.levels[2] and 
                           'L' in trade_df.index.levels[3] else [])+([{
                    'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') 
                          for i in get_ratio_df(trade_df, df2[index_name], 'S','L').index],
                    'y': get_ratio_df(trade_df, df2[index_name], 'S','L')['price'].values.tolist(),
                    'mode': 'markers',
                    'name': 'SL Ratio',
                    'marker': {
                        'color': 'rgb(214,39,40)',
                        'size':g.markerSize
                    }
                    }] if 'RH' in trade_df.index.levels[1] and 
                           'S' in trade_df.index.levels[2] and 
                           'L' in trade_df.index.levels[3] else [])+([{
                    'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') 
                          for i in get_ratio_df(trade_df, df2[index_name], 'B','S').index],
                    'y': get_ratio_df(trade_df, df2[index_name], 'B','S')['price'].fillna(method="ffill").values.tolist(),
                    'mode': 'markers',
                    'name': 'BC Ratio',
                    'marker': {
                        'color': 'rgb(121, 176, 255)',
                        'size':g.markerSize
                    }
                    }] if 'RH' in trade_df.index.levels[1] and 
                            'B' in trade_df.index.levels[2] and 
                            'S' in trade_df.index.levels[3] else [])+([{
                    'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') 
                          for i in get_ratio_df(trade_df, df2[index_name], 'S','S').index],
                    'y': get_ratio_df(trade_df, df2[index_name], 'S','S')['price'].values.tolist(),
                    'mode': 'markers',
                    'name': 'SS Ratio',
                    'marker': {
                        'color': 'rgb(255,124,144)',
                        'size':g.markerSize
                    }
                    }] if 'RH' in trade_df.index.levels[1] and 'S' in trade_df.index.levels[2] and 'S' in trade_df.index.levels[3] else []),
                'layout': {
                    'margin': {'l': g.left_margin, 'r': 40},
                    #'width': 750,
                    #'height': 240,
                    'legend': {'font': {'size': 10}, 'x': 1.05}
                }
    } 

     

    long_position = (sql_pl_df.groupby(['processDate', 'side'])[['RHExposure']]
                              .sum()
                              .loc[pd.IndexSlice[:,'L'],:]
                              .reset_index()
                              .drop('side', axis=1)
                              .set_index('processDate')
                              .reindex(index_return.index)
                              .fillna(0)
                              .rename(columns={'RHExposure': 'Long Position'})
                     )*100 if long_count > 0 else None

    short_position = (sql_pl_df.groupby(['processDate', 'side'])[['RHExposure']]
                               .sum()
                               .loc[pd.IndexSlice[:,'S'],:]
                               .reset_index()
                               .drop('side', axis=1)
                               .set_index('processDate')
                               .reindex(index_return.index)
                               .fillna(0)
                               .rename(columns={'RHExposure': 'Short Position'})
                      )*100 if short_count > 0 else None

    wiki_text = (ret_df.assign(text = lambda df: df['personCode']+ '(' + df['orderType'] +'): '+ ': '+df['catalystText'])[['processDate', 'text']]
                        .groupby('processDate')
                        .apply(lambda df: df['text'].str.cat(sep='<BR>'))
                 ) 

    wiki_g = ( (long_position.reindex(wiki_text.index)['Long Position'] if long_count > 0 else 0) + 
                 (short_position.reindex(wiki_text.index)['Short Position'] if short_count > 0 else 0) )

    # wiki_g = ( (long_position.reindex(wiki_text.index)['Long Position'] ) + 
    #              (short_position.reindex(wiki_text.index)['Short Position'] ) )
    # calculate the ratio of negaive and positive range for each graphs

    positive_pl_bound = 1.1*abs(max([max(long_position.max().values)])) if long_count > 0 else 0
    negative_pl_bound = 1.1*abs(min([min(short_position.min().values)])) if short_count > 0 else 0
     
    if long_count > 0 and short_count > 0:
         positive_pl_bound = 1.1*abs(max([max(long_position.max().values), min(short_position.min().values), 0]))
         negative_pl_bound = 1.1*abs(max([max(long_position.max().values), min(short_position.min().values), 0]))

    positive_index_bound = 1.1*abs(max([df3['close'].max()]))
    
    range1 = [-negative_pl_bound, positive_pl_bound]
    range2 = [-positive_index_bound, positive_index_bound]
    
    
    if long_count > 0 and short_count == 0:
        range1 = [0, positive_pl_bound]
        range2 = [0, positive_index_bound]
    elif long_count == 0 and short_count > 0:
        range1 = [-negative_pl_bound, negative_pl_bound]
        range2 = [-positive_index_bound, positive_index_bound]
    

    position_size_graph = {'data': ([{
                    'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in long_position.index],
                    'y': long_position[col].values.tolist(),
                    'name':'Long Position',
                    'hoverinfo': 'none',
                    'type' :'bar',
                    'marker': {
                        'color':'rgb(27, 93, 225)'
                        }
                    #'line': {'width':g.lineWidth,
                    #         'color': 'rgb(27, 93, 225)'
                    #         }
                } for col in long_position.columns
                ] if long_count > 0 else []) +([{
                    'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in short_position.index],
                    'y': short_position[col].values.tolist(),
                    'name':'Short Position',
                    'hoverinfo': 'none',
                    'type': 'bar',
                    'marker' :{
                        'color':'rgb(214,39,40)'
                        }
                    #'line': {'width':g.lineWidth,
                    #         'color': 'rgb(214,39,40)'
                    #         }
                } for col in short_position.columns
                ] if short_count > 0 else []) + ([{
                        'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in wiki_g.index],
                        'y': wiki_g.fillna(0).values.tolist(),
                        'mode': 'markers',
                        'name': 'Wiki',
                        'text': wiki_text.values.tolist(),
                        'hoverinfo': 'text',
                        'marker': {
                            'color': 'green',
                            'size' : 10
                            #'symbol': 18
                        }
                    }
                ]
                )+ ([{
                    'x': [pd.to_datetime(str(i)).strftime('%Y-%m-%d') for i in df3.index],
                    'y': df3['close'].values.tolist(),
                    'name':'Stock',
                    'hoverinfo': 'none',
                    #'fill': 'tozeroy',
                    'line': {
                        'color': 'black',
                        'width': 2
                        },
                    'yaxis': 'y2'
                } 
                ]
                    ),
                'layout': {
                    'margin': {'l': g.left_margin, 'r': 40},
                    # 'width': 750,
                    # 'height': 240,
                    'legend': {'font': {'size': 10}, 'x': 1.05},
                    'yaxis': {
                        'overlaying': 'y2',
                        'ticksuffix': '%',
                        'range' : range1
                    },
                    'yaxis2': {
                        'side': 'right',
                        'title': 'Price',
                        'tickfont': {'size': 10},
                        'range': range2
                    }
                }
    } 

    render_obj = dict()
    render_obj['name'] = position_name
    render_obj['quick'] = quick
    render_obj['table'] = tbl_html
    render_obj['long_price_graph'] = long_price_graph
    render_obj['long_rate_graph'] = long_rate_graph
    render_obj['position_size_graph'] = position_size_graph

    return render_template('tradehistory/result.html',
                           params=render_obj,
                           op_hit=op_hit,
                           alpha=alpha,
                           pl=(attr_df.sum() * 100).to_dict(),
                           rhop=(op_df['RH'].sum() * 100).to_dict(),
                           yaop=(op_df['YA'].sum() * 100).to_dict(),
                           lrop=(op_df['LR'].sum() * 100).to_dict(),
                           rhalpha=(alpha_df['RH'].sum() * 100).to_dict(),
                           yaalpha=(alpha_df['YA'].sum() * 100).to_dict(),
                           lralpha=(alpha_df['LR'].sum() * 100).to_dict(),
                           tbldata=pl_hit,
                           justify='right')
