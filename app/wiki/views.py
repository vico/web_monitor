from flask import request, g, render_template

import pymysql
from datetime import datetime
from . import wiki


@wiki.before_request
def before_request():
    g.con = pymysql.connect(host='localhost', user='root', passwd='root', db='hkg02p')
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


@wiki.route('/')
def index():
    
    quick = request.args.get('quick','notavailable')
    start_date = request.args.get('startDate', g.start_date)
    end_date = request.args.get('endDate', start_date)

    with g.con.cursor() as cursor:
        sql = '''SELECT DISTINCT processDate AS Date, a.personCode AS Adv,
                catalystText,
                REPLACE(REPLACE(commentText,"\r\n","<BR>"), "\n", "<BR>") AS Comment
                FROM hkg02p.t01Person a, noteDB.Note N
                WHERE a.personID = N.personID AND N.code=%s
                AND processDate >= %s AND processDate <= %s
                #AND commentText > ''
                ORDER BY processDate DESC
                ''' 

        cursor.execute(sql, (quick, start_date, end_date))
        result = cursor.fetchall()


    ret = ''.join([ '<p>' + 
                    '<strong>' + t[0].strftime('%Y-%m-%d') + '</strong>' + 
                    ' (' + t[1] + ') ' +
                    '<strong>' + t[2] + '</strong><br>'+
                    t[3] +
                    '</p>' for t in result ])
    

    return ret 

