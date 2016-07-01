from flask import request, g, render_template

import pymysql
import pymysql.cursors
from datetime import datetime
from pandas.io import sql
from pandas import DataFrame
import pandas as pd
from . import wiki
import re
from dateutil.parser import parse


@wiki.before_request
def before_request():
    con = getattr(g, 'con', None)
    if con is None:
        g.con = pymysql.connect(host='192.168.1.147', user='uploader', passwd='fA6ZxopGrbdb')
        #(host='localhost', user='root', passwd='root', db='hkg02p')
        #(host='192.168.1.147', user='uploader', passwd='fA6ZxopGrbdb', db='hkg02p')
    # g.start_date = datetime(datetime.now().year-2, 12, 31).strftime('%Y-%m-%d')
    g.start_date = '2012-01-01'
    g.end_date = datetime.now().strftime('%Y-%m-%d')  # not include
    

@wiki.teardown_request
def teardown_request(exception):
    con = getattr(g, 'con', None)
    if con is not None:
        con.close()


def get_trade_df(quick, start_date, end_date, con):
    trade_df = (sql.read_sql('''SELECT  a.tradeDate, 
                                    GROUP_CONCAT(DISTINCT CONCAT(a.orderType, a.side) SEPARATOR ',') AS `order`
                    FROM hkg02p.t08Reconcile a
                    WHERE a.code = '%s'
                      AND a.srcFlag='K'
                      AND a.status='A'
                      AND a.processDate >= '%s'
                      AND a.processDate <= '%s'
                      GROUP BY a.tradeDate
                      ORDER BY reconcileID
                    ;
        ;''' % (quick, start_date, end_date), con, coerce_float=True, parse_dates=['tradeDate'])
                .set_index(['tradeDate'])
                )
    
    return trade_df

def make_view(df, date_list):
    '''
    turn DataFrame into HTML 
    '''
    count = 0
    ret_html = ''
    style = 'class="table"'
    field_separator = '#'
    rows = df.to_csv(sep=field_separator).split('\n')
    

    for r in rows[1:]:
        if r != '':
            elements = r.split(field_separator)
            color = ('blue' if elements[1].title() in date_list and elements[5]=='BL' 
                            else ('red' if elements[1].title() in date_list and elements[5] == 'SS' else ''))

            ret_html += ('<p id=%s>' % (elements[1].title()) + '<strong><span style="color:%s;">' % color +  elements[1].title()+'</span></strong> (' + 
                             elements[2]+') (' + elements[5] + ') ' +
                             '<strong>' + elements[3].title() + '</strong><BR>' +
                             elements[4].title().decode('utf-8') +
                         '</p>'
                        )
            

    return ret_html
    

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


def get_wiki_df(quick, con):

    t = sql.read_sql('''
                SELECT DISTINCT processDate, a.personCode,catalystText,
                REPLACE(commentText, '\n', '<br>') AS commentText
                FROM hkg02p.t01Person a, noteDB.Note N
                WHERE a.personID = N.personID AND N.code='%s'
                ORDER BY processDate DESC
    ;''' % (quick), con , parse_dates=['processDate']) 

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

    wiki2014 = (DataFrame(rets)[['processDate', 'personCode', 'catalystText', 'commentText']]
                .set_index('processDate').sort_index(ascending=False).reset_index()) if len(rets) > 0 else DataFrame()
    
    return pd.concat([t, wiki2014], ignore_index=True)

@wiki.route('/')
def index():
    
    quick = request.args.get('quick','notavailable')
    start_date = request.args.get('start', g.start_date)
    end_date = request.args.get('end', g.end_date)

    wiki = get_wiki_df(quick, g.con) 

    trade_date_df = sql.read_sql('''
                SELECT IF(a.side='L', a.firstTradeDateLong, a.firstTradeDateShort) AS firstTradeDate
                FROM hkg02p.t05PortfolioResponsibilities a
                WHERE a.processDate > '%s' AND a.processDate < '%s'
                AND a.quick = '%s'
                GROUP BY IF(a.side='L', a.firstTradeDateLong, a.firstTradeDateShort)
    ;''' % (start_date, end_date, quick), g.con, coerce_float=True)

    trade_df = get_trade_df(quick, start_date, end_date, g.con)

    order_df = DataFrame(trade_df, index=trade_df.index.union(wiki['processDate'].drop_duplicates())).fillna(method="bfill")

    wiki = wiki.merge(order_df, left_on='processDate', right_index=True, how='left')

    date_list = [x[0].strftime('%Y-%m-%d') for x in trade_date_df.values.tolist()] 
    
    rent_html = make_view(wiki, date_list)
    return render_template('wiki/list.html', content=rent_html) 

