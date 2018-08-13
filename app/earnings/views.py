from flask import request, g, render_template, redirect, url_for, flash, current_app, Response, jsonify
import io
import os
import pandas as pd
from pandas.io import sql
import japandas as jpd
import numpy as np
import pymysql
from datetime import datetime
from . import earnings
import pymysql.cursors
import math
import pathlib
from ..models import EarningSeason, EarningExclude, EarningInclude, EarningMarketcap
from .. import db
from sqlalchemy import and_
from sqlalchemy.exc import IntegrityError
from werkzeug.utils import secure_filename
from cache import cache
from pprint import pprint
import plotly


NUMBER_OF_ROW_PER_PAGE = 42
# NUMBER_OF_TOP_POSITIONS = 8

MINIMUM_POSITION_SIZE = 0.0002

column_mapping = {
    'RH': {
        'PL': 'RHAttribution',
        'alpha': 'RHMktAlpha',
        'SincePL': 'PostRHAtt',
        'SinceAlpha': 'PostRHAlpha',
        'TotalPL': 'RHAttComb',
        'TotalAlpha': 'RHAlComb'
    },
    'YA': {
        'PL': 'YAAttribution',
        'alpha': 'YAMktAlpha',
        'SincePL': 'PostYAAtt',
        'SinceAlpha': 'PostYAAlpha',
        'TotalPL': 'YAAttComb',
        'TotalAlpha': 'YAAlComb'
    },
    'LR': {
        'PL': 'LRAttribution',
        'alpha': 'LRMktAlpha',
        'SincePL': 'PostLRAtt',
        'SinceAlpha': 'PostLRAlpha',
        'TotalPL': 'LRAttComb',
        'TotalAlpha': 'LRAlComb'
    }
}

ALLOWED_EXTENSIONS = {'xlsx'}


@earnings.before_request
def before_request():
    # g.con = pymysql.connect(host='192.168.1.147', user='uploader', passwd='fA6ZxopGrbdb')
    g.con = pymysql.connect(host='192.168.2.22', user='webUser', passwd='hgdO7w45BaFT')
    # (host='localhost', user='root', passwd='root', db='hkg02p')
    # (host='192.168.1.147', user='uploader', passwd='fA6ZxopGrbdb', db='hkg02p')
    g.lineWidth = 3
    g.markerSize = 7
    g.thinLineWidth = 2
    g.left_margin = 60
    calendar = jpd.JapaneseHolidayCalendar()
    g.cday = pd.offsets.CDay(calendar=calendar)


@earnings.teardown_request
def teardown_request(exception):
    con = getattr(g, 'con', None)
    if con is not None:
        con.close()


@earnings.route('/')
def index():
    fund = request.args.get('fund', 'RH')
    seasons = EarningSeason.query.order_by(EarningSeason.start.desc()).all()
    seasons_dict = [{'value': s.id, 'str': '{}'.format(s)
                     } for s in seasons]
    selected_season = [seasons_dict[0]['value']]

    return render_template('earnings/index.html',
                           fund=fund,
                           seasons=seasons_dict,
                           selected_season=selected_season,
                           funds=['RH', 'YA', 'LR'],
                           )


def get_heat_map(minimum, maximum, value):
    # colors = [(255, 0, 0), (250, 191, 143), (99, 190, 123)]
    colors = [(255, 105, 105), (250, 250, 250), (104, 170, 232)]

    fract_between = 0
    if value <= minimum:
        idx1 = idx2 = 0
    else:
        if value >= maximum:
            idx1 = idx2 = len(colors) - 1
        else:
            value = 2 * (value - minimum) / (maximum - minimum)
            idx1 = int(math.floor(value))
            idx2 = idx1 + 1
            fract_between = value - float(idx1)

    red = int(math.floor((colors[idx2][0] - colors[idx1][0]) * fract_between + colors[idx1][0]))
    green = int(math.floor((colors[idx2][1] - colors[idx1][1]) * fract_between + colors[idx1][1]))
    blue = int(math.floor((colors[idx2][2] - colors[idx1][2]) * fract_between + colors[idx1][2]))

    return "rgb(%s,%s,%s)" % (red, green, blue)


def generate_table_row(num, format_str, heat_map_func, min_value, max_value, style=''):
    try:
        return ('<td style="text-align: center; background-color: {}; {}">'.format(
            heat_map_func(min_value, max_value, float(num)), style
        ) + format_str.format(float(num)) + '</td>')
    except ValueError:
        return ('<td style="text-align: center; background-color: {}; {}">'.format(
            heat_map_func(min_value, max_value, (min_value + max_value) / 2.0), style
        ) + num + '</td>')


def str2float(s):
    if s:
        return float(s)
    else:
        return 0.0


def break_into_page(df, heat_map_func, is_stock_table=False, start_new_page=True, finalize_last_page=True,
                    number_of_row_to_break_first_page=NUMBER_OF_ROW_PER_PAGE, table_caption='',
                    is_multiple_season=False
                    ):
    """
    break a dataframe into many page, each page has NUMBER_OF_ROW_PER_PAGE
    return HTML format of pages and total number of rows
    :rtype: str, int
    """
    count = 1  # header
    total_count = 1
    table_html = ''
    style = 'border="1" class="dataframe borderTable"'
    is_first_page = False
    field_separator = '!'
    df.index = df.index.map(lambda x: str(x))
    rows = df.to_csv(sep=field_separator).split('\n')
    if start_new_page:
        table_html = '</section><section class="sheet padding-10mm">'
        is_first_page = True

    if not is_multiple_season:
        table_header = ('<table %s><thead><tr>' % style) + ''.join(
            ['<th style="border-bottom: 1px solid black;">' + h +
             '</th>' for h in rows[0].split(field_separator)]) + '</tr></thead>'
    elif not is_stock_table:
        table_header = ('<table %s><thead><tr>' % style) + ''.join(
            ['<th style="border-bottom: 1px solid black;">' + h +
             '</th>' for h in rows[0].split(field_separator)[:6]]) + '</tr></thead>'
    else:
        table_header = ('<table %s><thead><tr>' % style) + ''.join(
            ['<th style="border-bottom: 1px solid black;">' + h +
             '</th>' for h in rows[0].split(field_separator)[:5]]) + '</tr></thead>'

    if table_caption:
        table_header += '<caption>' + table_caption + '</caption><tbody>'
    else:
        table_header += '<tbody>'

    table_html += table_header
    left_wall_style = 'border:none; border-left: 1px solid black;'
    left_wall_style1 = ('border:none; border-left: 1px solid black;' +
                        'border-top: 1px solid black; border-bottom: 1px solid black;')
    default_style = 'border: none;'
    default_style1 = 'border: none; border-top: 1px solid black; border-bottom: 1px solid black;'
    index_style = ''
    total_style = 'border: none; border-top: 1px solid black;'
    # print('1working on {}, #rowsUntilEndPage={}'.format(rows[0], number_of_row_to_break_first_page))
    size = df.index.size

    for r in rows[1:]:
        if total_count == size:
            default_style = 'border:none; border-bottom: 1px solid black;'
            left_wall_style = left_wall_style + 'border-bottom: 1px solid black;'
            index_style = default_style
        if is_first_page and count > number_of_row_to_break_first_page and r != '':
            table_html += (
                           '</tbody></table></section><section class="sheet padding-10mm"><table %s>' % style
                          ) + table_header
            count = 1
            is_first_page = False
            number_of_row_to_break_first_page = NUMBER_OF_ROW_PER_PAGE - count
            # print('working on {}'.format(rows[0]))
        elif count >= number_of_row_to_break_first_page and r != '':
            # print('create new page in break_into_page')
            table_html += (
                                  '</tbody></table></section><section class="sheet padding-10mm"><table %s>' % style
                          ) + table_header
            count = 1
            number_of_row_to_break_first_page = NUMBER_OF_ROW_PER_PAGE - count
            # print('number_to_break_page = {}'.format(number_of_row_to_break_first_page))
            # print('2working on {}, #rowsUntilEndPage={}'.format(rows[0], number_of_row_to_break_first_page))

        table_column_title = (
                '<th style="text-align:left;white-space:nowrap; {}">' +
                '<a href="breakdown?fund={}&type={}&value={}&season={}">{}</a></th>'
        )
        if r != '':
            elements = r.split(field_separator)
            if not is_stock_table:
                if not is_multiple_season:
                    table_html += ('<tr>' +
                                   ''.join([table_column_title.format(index_style, g.fund,
                                                                      df.index.name if df.index.name is not None else '',
                                                                      elements[0],
                                                                      g.selected_season,
                                                                      elements[0])] +
                                           ['<td style="{}">{}</td>'.format(left_wall_style, int(float(h)) if h else 0)
                                            for h in elements[1:2]] +
                                           [generate_table_row(h, '{:.1%}', heat_map_func, 0, 1, left_wall_style)
                                            for h in elements[2:3]] +
                                           [generate_table_row(h, '{:.2%}', heat_map_func, -.005, .005, default_style)
                                            for h in elements[3:4]] +
                                           [generate_table_row(h, '{:.1%}', heat_map_func, 0, 1, default_style)
                                            for h in elements[4:5]] +
                                           [generate_table_row(h, '{:.2%}', heat_map_func, -.005, .005, default_style)
                                            for h in elements[5:8]] +
                                           [generate_table_row(h, '{:.1%}', heat_map_func, 0, 1, left_wall_style)
                                            for h in elements[8:9]] +
                                           [generate_table_row(h, '{:.2%}', heat_map_func, -.005, .005, default_style)
                                            for h in elements[9:10]] +
                                           [generate_table_row(h, '{:.1%}', heat_map_func, 0, 1, left_wall_style)
                                            for h in elements[10:11]] +
                                           [generate_table_row(h, '{:.2%}', heat_map_func, -.005, .005, default_style)
                                            for h in elements[11:]]
                                           ) +
                                   '</tr>')
                else:  # it is a multiple season table and not stock table
                    table_html += ('<tr>' +
                                   ''.join([table_column_title.format(index_style, g.fund,
                                                                      df.index.name if df.index.name is not None else '',
                                                                      elements[0],
                                                                      g.selected_season,
                                                                      elements[0])] +
                                           ['<td style="{}">{}</td>'.format(left_wall_style, int(float(h)) if h else 0)
                                            for h in elements[1:2]] +
                                           [generate_table_row(h, '{:.1%}', heat_map_func, 0, 1, left_wall_style)
                                            for h in elements[2:3]] +
                                           [generate_table_row(h, '{:.2%}', heat_map_func, -.005, .005, default_style)
                                            for h in elements[3:4]] +
                                           [generate_table_row(h, '{:.1%}', heat_map_func, 0, 1, default_style)
                                            for h in elements[4:5]] +
                                           [generate_table_row(h, '{:.2%}', heat_map_func, -.005, .005, default_style)
                                            for h in elements[5:6]]

                                           ) +
                                   '</tr>')
            else:  # it is a stock table
                if elements[0] == 'Total':  # this is final row
                    if not is_multiple_season:
                        table_html += (
                            '<tr>' +
                            ''.join(['<th style="text-align: left;white-space: nowrap; {}">'.format(total_style) +
                                     '</th>' for h in elements[0:3]] +
                                    ['<td style="text-align: center; background-color: {}; {}">'.format(
                                        heat_map_func(-0.003, 0.003, str2float(h)), left_wall_style1) +
                                     '{:.2%}'.format(str2float(h)) + '</td>' for h in elements[3:4]
                                     ] +
                                    ['<td style="text-align: center;background-color: {};{}">'.format(
                                        heat_map_func(-0.003, 0.003, str2float(h)), default_style1) +
                                     '{:.2%}'.format(str2float(h)) + '</td>' for h in elements[4:5]
                                     ] +
                                    ['<td style="text-align: center; background-color: {}; {}">'.format(
                                        heat_map_func(-0.003, 0.003, str2float(h)), left_wall_style1) +
                                     '{:.2%}'.format(str2float(h)) + '</td>' for h in elements[5:6]
                                     ] +
                                    [
                                        '<td style="text-align: center;background-color: {}; {}">'.format(
                                            heat_map_func(-0.003, 0.003, str2float(h)), default_style1) +
                                        '{:.2f}%'.format(str2float(h) * 100) + '</td>' for h in elements[6:7]
                                    ] +
                                    ['<td style="text-align: center; background-color: {}; {}">'.format(
                                        heat_map_func(-0.003, 0.003, str2float(h)), left_wall_style1) +
                                     '{:.2f}%'.format(str2float(h) * 100) + '</td>' for h in elements[7:8]
                                     ] +
                                    ['<td style="text-align: center; background-color: {}; {}">'.format(
                                        heat_map_func(-0.003, 0.003, str2float(h)), default_style1) +
                                     '{:.2f}%'.format(str2float(h) * 100) + '</td>' for h in elements[8:]
                                     ]
                                    ) +
                            '</tr>'
                        )
                    else:  # final of multiple season stock table
                        table_html += (
                                '<tr>' +
                                ''.join(['<th style="text-align: left;white-space: nowrap; {}">'.format(total_style) +
                                         '</th>' for h in elements[0:3]] +
                                        ['<td style="text-align: center; background-color: {}; {}">'.format(
                                            heat_map_func(-0.003, 0.003, str2float(h)), left_wall_style1) +
                                         '{:.2%}'.format(str2float(h)) + '</td>' for h in elements[3:4]
                                         ] +
                                        ['<td style="text-align: center;background-color: {};{}">'.format(
                                            heat_map_func(-0.003, 0.003, str2float(h)), default_style1) +
                                         '{:.2%}'.format(str2float(h)) + '</td>' for h in elements[4:5]
                                         ]
                                        ) +
                                '</tr>'
                        )
                else:  # not the final row
                    if not is_multiple_season:
                        table_html += (
                            '<tr>' +
                            ''.join(['<th style="text-align: left;white-space: nowrap; {}">'.format(index_style) +
                                     elements[0].title() + '</th>'] +
                                    ['<td style="text-align: center;color: {}; {}">'.format(
                                        'blue' if str2float(h) >= 0 else 'red', left_wall_style) +
                                     '<strong>{:.2%} </strong>'.format(str2float(h)) + '</td>' for h in elements[1:2]
                                     ] +
                                    ['<td style="text-align: center; {}">'.format(default_style) +  # earning date
                                     '{}'.format(datetime.strptime(h, '%Y-%m-%d').strftime('%d-%b')) + '</td>' for h in
                                     elements[2:3]
                                     ] +
                                    ['<td style="text-align: center; background-color: {}; {}">'.format(
                                        heat_map_func(-0.003, 0.003, str2float(h)), left_wall_style) +
                                     '{:.2%}'.format(str2float(h)) + '</td>' for h in elements[3:4]
                                     ] +
                                    ['<td style="text-align: center;background-color: {};{}">'.format(
                                        heat_map_func(-0.003, 0.003, str2float(h)), default_style) +
                                     '{:.2%}'.format(str2float(h)) + '</td>' for h in elements[4:5]
                                     ] +
                                    ['<td style="text-align: center; background-color: {}; {}">'.format(
                                        heat_map_func(-0.003, 0.003, str2float(h)), left_wall_style) +
                                     '{:.2%}'.format(str2float(h)) + '</td>' for h in elements[5:6]
                                     ] +
                                    [
                                        '<td style="text-align: center;background-color: {}; {}">'.format(
                                            heat_map_func(-0.003, 0.003, str2float(h)), default_style) +
                                        '{:.2f}%'.format(str2float(h) * 100) + '</td>' for h in elements[6:7]
                                    ] +
                                    ['<td style="text-align: center; background-color: {}; {}">'.format(
                                        heat_map_func(-0.003, 0.003, str2float(h)), left_wall_style) +
                                     '{:.2f}%'.format(str2float(h) * 100) + '</td>' for h in elements[7:8]
                                     ] +
                                    ['<td style="text-align: center; background-color: {}; {}">'.format(
                                        heat_map_func(-0.003, 0.003, str2float(h)), default_style) +
                                     '{:.2f}%'.format(str2float(h) * 100) + '</td>' for h in elements[8:]
                                     ]
                                    ) +
                            '</tr>'
                        )
                    else:  # this is a normal row of multiple season stock table
                        table_html += (
                                '<tr>' +
                                ''.join(['<th style="text-align: left;white-space: nowrap; {}">'.format(index_style) +
                                         elements[0].title() + '</th>'] +
                                        ['<td style="text-align: center;color: {}; {}">'.format(
                                            'blue' if str2float(h) >= 0 else 'red', left_wall_style) +
                                         '<strong>{:.2%} </strong>'.format(str2float(h)) + '</td>' for h in
                                         elements[1:2]
                                         ] +
                                        ['<td style="text-align: center; {}">'.format(default_style) +  # earning date
                                         '{}'.format(datetime.strptime(h, '%Y-%m-%d').strftime('%d %b %Y')) + '</td>' for h
                                         in
                                         elements[2:3]
                                         ] +
                                        ['<td style="text-align: center; background-color: {}; {}">'.format(
                                            heat_map_func(-0.003, 0.003, str2float(h)), left_wall_style) +
                                         '{:.2%}'.format(str2float(h)) + '</td>' for h in elements[3:4]
                                         ] +
                                        ['<td style="text-align: center;background-color: {};{}">'.format(
                                            heat_map_func(-0.003, 0.003, str2float(h)), default_style) +
                                         '{:.2%}'.format(str2float(h)) + '</td>' for h in elements[4:5]
                                         ]
                                        ) +
                                '</tr>'
                        )

            count += 1
            total_count += 1
    table_html += '</tbody></table>'
    if finalize_last_page:
        table_html += '</section>'
    # print('return count = {}'.format(number_of_row_to_break_first_page - count))
    return table_html, number_of_row_to_break_first_page - count


def add_to_page(df, current_page, remaining_row_number, is_stock_table=False, start_new_page=False, last_page=False,
                caption='', is_multiple_season=False):
    return_html = ''
    if len(df.index) + 1 < remaining_row_number or remaining_row_number >= 12:
        return_html, remaining_row_number = break_into_page(df, get_heat_map, is_stock_table,
                                                            start_new_page=start_new_page,
                                                            finalize_last_page=last_page,
                                                            number_of_row_to_break_first_page=remaining_row_number,
                                                            table_caption=caption,
                                                            is_multiple_season=is_multiple_season
                                                            )
    else:  # create new page if df does not fit remaining page
        return_html, remaining_row_number = break_into_page(df, get_heat_map, is_stock_table, start_new_page=True,
                                                            finalize_last_page=last_page,
                                                            number_of_row_to_break_first_page=NUMBER_OF_ROW_PER_PAGE,
                                                            table_caption=caption,
                                                            is_multiple_season=is_multiple_season
                                                            )

    return current_page + return_html, remaining_row_number


def get_code_name_map():
    code_name_map = sql.read_sql('''SELECT quick, name FROM hkg02p.t01Instrument WHERE instrumentType='EQ';''', g.con)
    return code_name_map


def get_jp_equity_pickle_file(season):
    pickle_name = 'data/pl_df_{}'.format(season.id)

    pickle_file = pathlib.Path(pickle_name)

    max_date = None
    prev_df = None
    if pickle_file.exists():
        prev_df = pd.read_pickle(pickle_name)
        max_date = prev_df['processDate'].max()

    return prev_df, max_date, pickle_name


def get_jp_equity_pl(con, season, include):
    """
    get and return DataFrame of PnL, make sure to keep manually updated MktCap
    :param con: db connection
    :param season: season object which contains earning season period
    :param include: list of manually added stock code
    :return: DataFrame of PnL for all names within the earning season period
    """

    # PnL numbers did change from run so we will always get latest numbers
    _, _, pickle_name = get_jp_equity_pickle_file(season)

    # update_mktcap = EarningMarketcap.query.filter(EarningMarketcap.season_id == season.id).all()

    # if we already have all season pnl data then just return the cache data.
    # if season.end and max_date and max_date.to_pydatetime().date() >= season.end:
    #     current_app.logger.debug('return old pl DataFrame')
    #     return prev_df

    query = """
        SELECT
          a.processDate,
          #IF(a.side = "L", a.firstTradeDateLong, a.firstTradeDateShort) AS firstTradeDate,
          a.instrumentID,
          a.quick,
          a.name,
          a.side,
          a.advisor,
          a.strategy,
          a.TPX,
          a.GICS,
          a.RHAttribution,
          a.YAAttribution,
          a.LRAttribution,
          a.RHMktAlpha,
          a.YAMktAlpha,
          a.LRMktAlpha,
          IF(a.side = "S", -a.RHExposure, a.RHExposure)                 AS RHPos,
          IF(a.side = "S", -a.YAExposure, a.YAExposure)                 AS YAPos,
          IF(a.side = "S", -a.LRExposure, a.LRExposure)                 AS LRPos,
          # the mktcap in excel use jpy values to decide
          IF(c.instrumentType <> 'EQ', 'Index',
            IF(h.value*g.close < 25000000000,'Micro',
            IF(h.value*g.close <100000000000, 'Small',
            IF(h.value*g.close <500000000000, 'Mid',
            IF(h.value*g.close <1000000000000, 'Large',
            IF(h.value IS NULL, 'Micro','Mega'))) ))) AS MktCap,
          h.value*g.close AS MktCapValue,
          a.quantity  # the total quantity of 3 funds
        FROM hkg02p.t05PortfolioResponsibilities a
          INNER JOIN hkg02p.t01Instrument c ON a.instrumentID = c.instrumentID 
            AND c.instrumentType = "EQ" 
            AND c.currencyID = 1
        # INNER JOIN hkg02p.t06DailyCrossRate d ON a.processDate = d.priceDate AND a.ccy = d.base AND d.quote='USD'
        LEFT JOIN hkg02p.t06DailyBBStaticSnapshot h ON c.instrumentID = h.instrumentID AND h.dataType = 'EQY_SH_OUT_REAL'
        LEFT JOIN hkg02p.t06DailyPrice g ON g.instrumentID=c.instrumentID AND g.priceDate=a.processDate
        WHERE a.processDate >= DATE_SUB('{}', INTERVAL 5 DAY)  # get back more days to get exposure for prev-businessday
              AND a.processDate <= '{}'
              # thematic name - anything where sensitivity = THEME, means it's not an earnings play
              AND (a.sensitivity <> 'THEME'"""

    # if max_date:
    #     start_date = max_date.to_pydatetime().strftime('%Y-%m-%d')
    # else:
    start_date = season.start.strftime('%Y-%m-%d')
    end_date = (season.end.strftime('%Y-%m-%d')
                if season.end is not None else datetime.today().strftime('%Y-%m-%d')
                )

    sql_values = [start_date, end_date]

    if include:
        query = query + ' OR (a.quick IN ({}))'
        sql_values += [','.join(map(lambda s: '"{}"'.format(s), include))]

    query = query + """ )
        ORDER BY a.processDate
    """

    formatted_query = query.format(*sql_values)

    # current_app.logger.debug('pl query: {}'.format(formatted_query))

    pl_df = pd.read_sql(formatted_query, con, parse_dates=['processDate'])
    # if max_date:
    #     prev_df = prev_df[prev_df['processDate'] <= max_date]
    #     pl_df = pd.concat([prev_df, pl_df[pl_df['processDate'] > max_date]])
    pl_df.to_pickle(pickle_name)

    return pl_df


def get_earning_pickle_file(season):
    pickle_name = 'data/earning_df_{}'.format(season.id)

    picke_file = pathlib.Path(pickle_name)

    existing_instruments = []
    prev_df = None
    if picke_file.exists():
        prev_df = pd.read_pickle(pickle_name)
        existing_instruments = prev_df['instrumentID'].unique().tolist()

    return prev_df, existing_instruments, pickle_name


def get_earning_df(con, season, exclude):
    """
    return earning dates for specified season incrementally without overwrite old data.
    :param con:
    :param season:
    :param exclude: list of code to exclude from earning DataFrame
    :return:
    """

    prev_df, existing_instruments, pickle_name = get_earning_pickle_file(season)

    # select a list of announcement dates for instruments we have in specified period
    # need to make sure each instrument have only one announcement date, if more than 1,
    # choose date come earlier.
    query = """
        SELECT
          b.instrumentID,
          c.quick,
          b.earningYear,
          b.period,
          concat(MIN(b.announcement_date), ' ', max(b.announcement_time)) AS datetime,
          concat(MIN(b.announcement_date), ' ', max(b.announcement_time)) AS orig_datetime
        FROM hkg02p.t09BBEarningAnnouncement b
          INNER JOIN hkg02p.t01Instrument c ON b.instrumentID = c.instrumentID
                                               AND c.instrumentType = "EQ"
          # INNER JOIN hkg02p.t05PortfolioResponsibilities d ON b.instrumentID=d.instrumentID
          #    AND d.processDate=b.announcement_date AND d.quantity > 0
        WHERE b.announcement_date >= DATE_SUB('{}', INTERVAL 5 DAY)
              AND b.announcement_date <= '{}'
              AND b.instrumentID IN (SELECT DISTINCT z.instrumentID
                                     FROM hkg02p.t05PortfolioResponsibilities z
                                     WHERE z.processDate >= '{}'
                                           AND z.processDate <= '{}'
                                           AND z.CCY = 'JPY')
                                                      """

    start_date = season.start.strftime('%Y-%m-%d')
    end_date = (season.end.strftime('%Y-%m-%d')
                if season.end is not None else datetime.today().strftime('%Y-%m-%d')
                )

    sql_values = [start_date, end_date, start_date, end_date]

    if existing_instruments:
        query = query + ' AND b.instrumentID NOT IN ({}) '
        sql_values += [','.join(map(lambda s: '{}'.format(s), existing_instruments))]

    if exclude:
        query = query + ' AND c.quick NOT IN ({}) '
        sql_values += [','.join(map(lambda s: '"{}"'.format(s), exclude))]

    query = query + """    
        GROUP BY b.instrumentID, b.announcement_date
        ORDER BY b.announcement_date
    """

    earning_df = pd.read_sql(query.format(*sql_values),
                             con, parse_dates={'datetime': '%Y-%m-%d %H:%M:%S'},
                             index_col='datetime')

    # print(query.format(*sql_values))

    if prev_df is not None and prev_df[~prev_df['quick'].isin(exclude)]:
        prev_df = prev_df[~prev_df['quick'].isin(exclude)]  # filter exclude out in existing earning df
    else:
        prev_df = 0

    if not earning_df.empty:  # adjust earning date for new data got from Database
        earning_df.index = earning_df.index + pd.to_timedelta([9] * earning_df.index.size, 'h')
        earning_df.index = earning_df.index + g.cday * 0  # there is no vectorized implementation for CDay yet
        # that will cause a PerformanceWarning
        earning_df.index = earning_df.index.normalize()  # .sort_values()
        earning_df.index.name = 'datetime'

    if existing_instruments and not earning_df.empty:  # we have some earning data to append
        earning_df = pd.concat([prev_df, earning_df])
        earning_df = earning_df.sort_index()
        earning_df.to_pickle(pickle_name)
    elif existing_instruments and earning_df.empty:
        earning_df = prev_df
    elif not existing_instruments and not earning_df.empty:  # first time data
        earning_df.to_pickle(pickle_name)

    # print(earning_df)
    if not earning_df.empty:
        earning_df = earning_df.sort_index()[season.start:]  # remove all earnings before season.start

    return earning_df


def filter_after_earning(df):
    df = df.copy()
    # filter trades after earning announcement date
    df = df[df['processDate'] > df['datetime']]
    return df


def make_raw_df(con, season, include, exclude):

    xlsx_filepath = 'data/xlsx_{}'.format(season.id)
    xlsx_file = pathlib.Path(xlsx_filepath)

    if xlsx_file.exists():
        current_app.logger.info('read raw_df of xlsx')
        raw_df = pd.read_pickle(xlsx_filepath)
        return raw_df

    earning_df = get_earning_df(con, season, exclude)[['instrumentID', 'earningYear', 'period', 'orig_datetime']]
    pl_df = get_jp_equity_pl(con, season, include)

    # calculate sum of pnl, alpha for each (instrument, advisor) pair count from the day after earning date
    pos_attr = (
        pl_df
            .merge(earning_df.reset_index(), how='left',
                   left_on=['instrumentID'],
                   right_on=['instrumentID']
                   )
            .dropna()
            .pipe(filter_after_earning)
            .groupby(['instrumentID', 'advisor', 'side'])
            .sum()[['RHAttribution', 'YAAttribution', 'LRAttribution', 'RHMktAlpha', 'YAMktAlpha', 'LRMktAlpha']]
            .fillna(0)  # for position which is closed on earning date we have NaN for PL, fill those with 0
            .reset_index()
            .rename(columns={'RHAttribution': 'PostRHAtt', 'YAAttribution': 'PostYAAtt', 'LRAttribution': 'PostLRAtt',
                             'RHMktAlpha': 'PostRHAlpha', 'YAMktAlpha': 'PostYAAlpha', 'LRMktAlpha': 'PostLRAlpha'
                             })
    )

    # get exposures one day before earning date for each instrument
    earning_date_pnl = (
        earning_df
            .reset_index()
            .merge(pl_df, how='inner',
                   left_on=['datetime', 'instrumentID'],
                   right_on=['processDate', 'instrumentID'])
            .drop(['RHPos', 'YAPos', 'LRPos', 'processDate'], axis=1)
            .assign(prevDate=lambda df: df['datetime'] - g.cday)
        # get yes-business-day exposure
            .merge(pl_df[['processDate', 'instrumentID', 'RHPos', 'YAPos', 'LRPos']],
                   how='inner',
                   left_on=['prevDate', 'instrumentID'],
                   right_on=['processDate', 'instrumentID'])
    )
    # updated the logic to remove names which have position of 0 on its earning date.
    # (except earning dates which is announced after market close)
    # Tetsu request (2018-04-13)
    earning_date_pnl['orig_datetime'] = pd.to_datetime(earning_date_pnl['orig_datetime'])
    earning_date_pnl = (earning_date_pnl[(earning_date_pnl['quantity'] != 0) |
                        ((earning_date_pnl['quantity'] == 0) &
                        (earning_date_pnl['datetime'] > earning_date_pnl['orig_datetime']))
                        ])
    raw_df = (earning_date_pnl
              .merge(pos_attr, how='left',
                     left_on=['instrumentID', 'advisor', 'side'],
                     right_on=['instrumentID', 'advisor', 'side'])
              .fillna(0)  # for position which is closed on earning date we have NaN for PL, fill those with 0
              .assign(RHAttComb=lambda df: df['RHAttribution'] + df['PostRHAtt'])
              .assign(YAAttComb=lambda df: df['YAAttribution'] + df['PostYAAtt'])
              .assign(LRAttComb=lambda df: df['LRAttribution'] + df['PostLRAtt'])
              .assign(RHAlComb=lambda df: df['RHMktAlpha'] + df['PostRHAlpha'])
              .assign(YAAlComb=lambda df: df['YAMktAlpha'] + df['PostYAAlpha'])
              .assign(LRAlComb=lambda df: df['LRMktAlpha'] + df['PostLRAlpha'])
              )

    return raw_df


def calculate_group(s, fund):
    """
    a function to summarize data for each group passed to it as s
    :param s: a group of data
    :param fund: fund string like RH, YA, LR
    :return: Series of summarized result
    """
    earnings_count = len(s[(s[column_mapping[fund]['PL']] + s[column_mapping[fund]['alpha']] != 0) &
                           (np.abs(s['{}Pos'.format(fund)]) >= MINIMUM_POSITION_SIZE)
                           ])

    if earnings_count > 0:
        win = len(
            s[(s[column_mapping[fund]['PL']] > 0) & (np.abs(s['{}Pos'.format(fund)]) >= MINIMUM_POSITION_SIZE)]
        ) * 1.0 / earnings_count
    else:
        win = np.nan
    pnl = s[column_mapping[fund]['PL']].sum()
    if earnings_count > 0:
        alpha_win = len(s[s[column_mapping[fund]['alpha']] > 0]) * 1.0 / earnings_count
    else:
        alpha_win = np.nan
    alpha_sum = s[column_mapping[fund]['alpha']].sum()
    since_pl = s[column_mapping[fund]['SincePL']].sum()
    since_alpha = s[column_mapping[fund]['SinceAlpha']].sum()
    if len(s[s[column_mapping[fund]['TotalPL']] != 0]) > 0 and earnings_count > 0:
        total_win = (
                len(s[(s[column_mapping[fund]['TotalPL']] > 0) &
                      (np.abs(s['{}Pos'.format(fund)]) >= MINIMUM_POSITION_SIZE)]
                    ) * 1.0 / earnings_count
        )
    else:
        total_win = np.nan
    total_pl = s[column_mapping[fund]['TotalPL']].sum()
    if earnings_count > 0:
        total_alpha_win = (len(
            s[(s[column_mapping[fund]['TotalAlpha']] > 0) & (np.abs(s['{}Pos'.format(fund)]) >= MINIMUM_POSITION_SIZE)]
        ) * 1.0 /
                           earnings_count)
    else:
        total_alpha_win = np.nan
    total_alpha = s[column_mapping[fund]['TotalAlpha']].sum()

    return pd.Series([earnings_count, win, pnl, alpha_win, alpha_sum, since_pl, since_alpha, total_win,
                      total_pl, total_alpha_win, total_alpha
                      ],
                     index=['Earnings', 'Win%', 'PL', 'Al%', 'Alpha', 'Since PL', 'Since Alpha', 'Total Win%',
                            'Total PL', 'Total Al%', 'Total Alpha'
                            ]
                     )


def get_pickle_name(season):
    if type(season) is list:
        id_str = '_'.join(['{}'.format(i_str) for i_str in sorted([s.id for s in season])])
        path_name = 'data/raw_df_{}'.format(id_str)
    else:
        path_name = 'data/raw_df_{}'.format(season.id)
    return path_name


def get_raw_df_pickle(season):
    pickle_name = get_pickle_name(season)

    picke_file = pathlib.Path(pickle_name)

    raw_df = None
    if picke_file.exists():
        raw_df = pd.read_pickle(pickle_name)

    return raw_df


def get_excel_df_pickle(season):
    pickle_name = 'xlsx_{}'.format(season.id)
    xlsx_filepath = 'data/{}'.format(pickle_name)
    xlsx_file = pathlib.Path(xlsx_filepath)
    raw_df = None
    if xlsx_file.exists():
        current_app.logger.info('get raw_df of xlsx pickle')
        raw_df = pd.read_pickle(xlsx_filepath)
    return raw_df


def make_combined_raw_df(seasons):
    """
    return combined raw_df of earning seasons list
    :param seasons: list of earning season
    :return: DataFrame of combined earning seasons or None if there is one season not available
    """
    raw_dfs = []
    # pickle_name = get_pickle_name(seasons)
    for season in seasons:
        if not season:
            current_app.logger.critical('Currently there is no data for season {}.'.format(season))
            return None

        raw_dfs.append(make_raw_df(g.con, season,
                                   [include.code for include in season.includes.all()],
                                   [exclude.code for exclude in season.excludes.all()]
                                   )
                       )
    raw_df = pd.concat(raw_dfs)
    return raw_df


def checking_for_valid_seasons(selected_seasons):
    if selected_seasons:
        ss = []
        for season in selected_seasons:
            if season is None or len(season) == 0:
                return None, "There is invalid season selected."
            ss.append(int(season))
        return ss, 'ok'
    else:
        return None, 'No season selected'


@earnings.route('/analyse')
def analyse():
    fund = request.args.get('fund', 'RH')
    selected_seasons = request.args.getlist('season')

    selected_seasons, error_msg = checking_for_valid_seasons(selected_seasons)

    if selected_seasons is None:
        return error_msg

    seasons = []
    for s_id in selected_seasons:
        seasons.append(EarningSeason.query.filter(EarningSeason.id == s_id).first())
    # season = EarningSeason.query.filter(EarningSeason.id == selected_season).first()

    all_seasons = EarningSeason.query.all()

    g.fund = fund
    code_name_map = get_code_name_map()
    # calculate sum of pnl, alpha for each (instrument, advisor) pair count from the day after earning date

    is_multiple_season = (len(selected_seasons) > 1)
    g.selected_season = selected_seasons if is_multiple_season else selected_seasons[0]

    pickle_name = get_pickle_name(seasons)

    raw_df = make_combined_raw_df(seasons)
    raw_df.to_pickle(pickle_name)  # write to pickle file for later editing
    pd.set_option('display.max_columns', 40)
    # print(raw_df)
    filtered_df = raw_df[(raw_df[column_mapping[fund]['PL']] + raw_df[column_mapping[fund]['alpha']] != 0)
                         & (np.abs(raw_df['{}Pos'.format(fund)]) >= MINIMUM_POSITION_SIZE)
                         ]
    # print(filtered_df[filtered_df['quick'] == '3904'].iloc[0])
    number_earnings = len(filtered_df)

    winners = len(filtered_df[filtered_df[column_mapping[fund]['PL']] > 0])
    pnl = filtered_df[column_mapping[fund]['PL']].sum()
    alpha_win = len(filtered_df[filtered_df[column_mapping[fund]['alpha']] > 0])

    alpha = filtered_df[column_mapping[fund]['alpha']].sum()
    since_pl = filtered_df[column_mapping[fund]['SincePL']].sum()
    since_alpha = filtered_df[column_mapping[fund]['SinceAlpha']].sum()
    total_win = len(filtered_df[filtered_df[column_mapping[fund]['TotalPL']] > 0])
    total_pl = filtered_df[column_mapping[fund]['TotalPL']].sum()
    total_alpha_win = len(filtered_df[filtered_df[column_mapping[fund]['TotalAlpha']] > 0])
    total_alpha = filtered_df[column_mapping[fund]['TotalAlpha']].sum()
    if number_earnings > 0:
        win_pct = winners * 1.0 / number_earnings
        alpha_pct = alpha_win * 1.0 / number_earnings
        total_win_pct = total_win * 1.0 / number_earnings
        total_alpha_pct = total_alpha_win * 1.0 / number_earnings
    else:
        win_pct = alpha_pct = total_alpha_pct = total_win_pct = 0

    summary = {
        'OnDate': {
            'Earnings': number_earnings,
            'Winners': winners,
            'win_pct': win_pct,
            'PnL': pnl,
            'alpha_win': alpha_win,
            'alpha_pct': alpha_pct,
            'alpha': alpha
        },
        'Since': {
            'PnL': since_pl,
            'alpha': since_alpha
        },
        'Total': {
            'Winners': total_win,
            'win_pct': total_win_pct,
            'PnL': total_pl,
            'alpha_win': total_alpha_win,
            'alpha_pct': total_alpha_pct,
            'alpha': total_alpha
        }
    }

    ls_table = (raw_df.groupby('side')
                .apply(calculate_group, (fund))
                .rename(index={'L': 'Long', 'S': 'Short'})
                )

    cap_table = (
        raw_df.groupby('MktCap')
            .apply(calculate_group, (fund))
            .reindex(['Micro', 'Small', 'Mid', 'Large', 'Mega'])
    )

    adv_table = raw_df.groupby('advisor').apply(calculate_group, (fund)).dropna()

    tpx_table = raw_df.groupby('TPX').apply(calculate_group, (fund)).fillna(0)

    gics_table = raw_df.groupby('GICS').apply(calculate_group, (fund)).dropna()

    tables_html = ''
    remaining_row_number = 28
    for df in [ls_table, cap_table, adv_table, tpx_table, gics_table]:
        tables_html, remaining_row_number = add_to_page(df, tables_html, remaining_row_number,
                                                        is_multiple_season=is_multiple_season)

    ranking_df = (
        (filtered_df
            .groupby(['quick', 'side'])
            .sum()
        [['{}Attribution'.format(fund), '{}MktAlpha'.format(fund), 'Post{}Att'.format(fund),
          'Post{}Alpha'.format(fund), '{}AttComb'.format(fund), '{}AlComb'.format(fund)
          ]]
            ).reset_index()
            .merge(raw_df.groupby(['quick', 'side', 'datetime']).sum()['{}Pos'.format(fund)].reset_index(),
                   how='inner',
                   left_on=['quick', 'side'],
                   right_on=['quick', 'side']
                   )
            .merge(code_name_map, how='inner', left_on=['quick'], right_on=['quick'])
            .assign(combo_name=lambda df: df['name'] + ' (' + df['quick'] + ')')

    )

    attr_sortted = ranking_df.sort_values('{}AttComb'.format(fund), ascending=False)
    top2df = attr_sortted.head(2)
    bottom2 = attr_sortted.tail(2)
    top2 = {
        'first': {
            'attr': top2df.iloc[0]['{}AttComb'.format(fund)] if not top2df.empty else 0,
            'name': top2df.iloc[0].combo_name if not top2df.empty else ''
        },
        'second': {
            'attr': top2df.iloc[1]['{}AttComb'.format(fund)] if len(top2df.index) > 1 else 0,
            'name': top2df.iloc[1].combo_name if len(top2df.index) > 1 else ''
        }
    }

    worse2 = {
        'first': {
            'attr': bottom2.iloc[1]['{}AttComb'.format(fund)] if len(bottom2.index) > 1 else 0,
            'name': bottom2.iloc[1].combo_name if len(bottom2.index) > 1 else ''
        },
        'second': {
            'attr': bottom2.iloc[0]['{}AttComb'.format(fund)] if not bottom2.empty else 0,
            'name': bottom2.iloc[0].combo_name if not bottom2.empty else ''
        }
    }

    def mk_stock_table(df):
        ret = (
            (df
                .groupby(['quick', 'side'])
                .sum()
            [['{}Attribution'.format(fund), '{}MktAlpha'.format(fund), 'Post{}Att'.format(fund),
              'Post{}Alpha'.format(fund), '{}AttComb'.format(fund), '{}AlComb'.format(fund)
              ]]
                ).reset_index()
                .merge(raw_df.groupby(['quick', 'side', 'datetime']).sum()['{}Pos'.format(fund)].reset_index(),
                       how='inner',
                       left_on=['quick', 'side'],
                       right_on=['quick', 'side']
                       )
                .merge(code_name_map, how='inner', left_on=['quick'], right_on=['quick'])
                .assign(combo_name=lambda df: df['name'].str[:25] + ' (' + df['quick'] + ', ' + df['side'] + ')')
                .reset_index()
                .sort_values(by=['datetime', 'quick'])
                .drop(['name', 'quick', 'side'], axis=1)
                .rename(columns={'combo_name': 'Name',
                                 '{}Pos'.format(fund): 'OpenPos', 'datetime': 'Date',
                                 '{}Attribution'.format(fund): 'E.DatePL',
                                 '{}MktAlpha'.format(fund): 'E.DateAlpha',
                                 'Post{}Att'.format(fund): 'SincePL',
                                 'Post{}Alpha'.format(fund): 'SinceAlpha',
                                 '{}AttComb'.format(fund): 'TotalPL',
                                 '{}AlComb'.format(fund): 'TotalAlpha'
                                 })
                .set_index('Name')

        )
        total_serie = ret.sum(numeric_only=True)
        total_serie.name = 'Total'
        total_row = pd.DataFrame(total_serie).T
        total_row['OpenPos'] = np.nan
        return pd.concat([ret, total_row])[['OpenPos', 'Date', 'E.DatePL', 'E.DateAlpha',
                                            'SincePL', 'SinceAlpha', 'TotalPL', 'TotalAlpha']]

    non_rh_stock_table = None
    if fund != 'RH':
        non_rh_df = filtered_df[
            (filtered_df[column_mapping['RH']['PL']] + filtered_df[column_mapping['RH']['alpha']] == 0) |
            (np.abs(filtered_df['{}Pos'.format('RH')]) < MINIMUM_POSITION_SIZE)
            ]

        non_rh_stock_table = mk_stock_table(non_rh_df)
        include_rh_df = filtered_df[
            (filtered_df[column_mapping['RH']['PL']] + filtered_df[column_mapping['RH']['alpha']] != 0) &
            (np.abs(filtered_df['{}Pos'.format('RH')]) >= MINIMUM_POSITION_SIZE)
            ]
        stock_table = mk_stock_table(include_rh_df)

    else:
        stock_table = mk_stock_table(filtered_df)

    tables_html2 = ''

    current_app.logger.debug('is_multiple_season={}'.format(is_multiple_season))
    remaining_row_number2 = NUMBER_OF_ROW_PER_PAGE
    if non_rh_stock_table is not None and not non_rh_stock_table.empty:
        tables_html2, remaining_row_number2 = add_to_page(non_rh_stock_table, tables_html2, remaining_row_number2,
                                                          is_stock_table=True, start_new_page=True,
                                                          caption='Non Rockhampton Names',
                                                          is_multiple_season=is_multiple_season)
        tables_html2, remaining_row_number2 = add_to_page(stock_table, tables_html2, remaining_row_number2,
                                                          is_stock_table=True, start_new_page=False,
                                                          caption='All Remain Names',
                                                          is_multiple_season=is_multiple_season)
    else:
        tables_html2, remaining_row_number2 = add_to_page(stock_table, tables_html2, remaining_row_number2,
                                                          is_stock_table=True, start_new_page=True,
                                                          caption='',
                                                          is_multiple_season=is_multiple_season)
    # print('initial remaining={}'.format(remaining_row_number2))

    # print('after stock_table remaining={}'.format(remaining_row_number2))
    cap_ls = (raw_df.groupby(['side', 'MktCap'])
              .apply(calculate_group, (fund))
              .reindex(['Micro', 'Small', 'Mid', 'Large', 'Mega'], level=1)
              ).dropna()
    cap_ls.index = [' '.join(['Long' if idx[0] == 'L' else 'Short', idx[1]]) for idx in cap_ls.index.values]
    cap_ls.index.name = 'MktCap-side'

    adv_ls = raw_df.groupby(['side', 'advisor']).apply(calculate_group, (fund)).dropna()
    adv_ls.index = [' '.join(['Long' if idx[0] == 'L' else 'Short', idx[1]]) for idx in adv_ls.index.values]
    adv_ls.index.name = 'advisor-side'

    tpx_ls = raw_df.groupby(['side', 'TPX']).apply(calculate_group, (fund)).fillna(0)
    tpx_ls.index = [' '.join(['L' if idx[0] == 'L' else 'S', idx[1]]) for idx in tpx_ls.index.values]
    tpx_ls.index.name = 'TPX-side'

    gics_ls = raw_df.groupby(['side', 'GICS']).apply(calculate_group, (fund)).dropna()
    gics_ls.index = [' '.join(['L' if idx[0] == 'L' else 'S', idx[1]]) for idx in gics_ls.index.values]
    gics_ls.index.name = 'GICS-side'

    for df in [cap_ls, adv_ls, gics_ls]:
        # print('after {} remaining={}'.format(df.index.name, remaining_row_number2))
        tables_html2, remaining_row_number2 = add_to_page(df, tables_html2, remaining_row_number2,
                                                          is_multiple_season=is_multiple_season)

    # print('before gics_ls remaining={}'.format(remaining_row_number2))
    tables_html2, remaining_row_number2 = add_to_page(tpx_ls, tables_html2, remaining_row_number2, last_page=True,
                                                      is_multiple_season=is_multiple_season)

    graph_link = '{}'.format(url_for('earnings.graph', season=selected_seasons, fund=fund))

    ret_page = render_template('earnings/result.html',
                               fund=fund,
                               # start_date=start_date,
                               # end_date=end_date,
                               seasons=all_seasons,
                               selected_season=g.selected_season,
                               graph_link=graph_link,
                               summary=summary,
                               tables_html1=tables_html,
                               # stock_table=stock_table,
                               tables_html2=tables_html2,
                               top2=top2,
                               worse2=worse2,
                               funds=['RH', 'YA', 'LR'],
                               is_multiple_season=is_multiple_season,
                               )
    # if view_file is not None:
    #     view_file.write_text(ret_page, encoding='utf8')
    return ret_page


def earning_df_for_season(selected_seasons):
    if selected_seasons:
        selected_seasons = list(map(lambda x: int(x), selected_seasons))
    else:
        return 'No season selected'

    seasons = [EarningSeason.query.filter(EarningSeason.id == s_id).first() for s_id in selected_seasons]

    is_multiple_season = (len(selected_seasons) > 1)
    g.selected_season = selected_seasons if is_multiple_season else selected_seasons[0]

    earning_df = (
        get_earning_df(g.con, seasons[0], [exclude.code for exclude in seasons[0].excludes.all()])
        .sort_index(ascending=False)
        .reset_index()
        .rename(columns={'orig_datetime': 'AnnceDateTime',
                         'datetime': 'EarningsDate'
                         })
        [['quick', 'earningYear', 'period', 'AnnceDateTime', 'EarningsDate']]
    )
    return earning_df


@earnings.route('/newnames', methods=['GET'])
def new_names():
    selected_seasons = request.args.getlist('season')

    earning_df = earning_df_for_season(selected_seasons)

    earning_table = earning_df.to_html(index=False, classes='table')
    return render_template('earnings/earnings_dates.html', table=earning_table)


@earnings.route('/earning_date_excel', methods=['GET'])
def earning_date_excel():
    selected_seasons = request.args.getlist('season')

    earning_df = earning_df_for_season(selected_seasons)
    earning_df[['quick', 'earningYear']] = earning_df[['quick', 'earningYear']].astype(np.int32)

    def add_sheet(sheet_name, data_pd, writer):
        number_of_rows = data_pd.index.size
        data_pd.to_excel(writer, sheet_name=sheet_name, index=False, startrow=0, startcol=0)
        workbook = writer.book
        # # Add a number format for cells with money.
        # number_fmt = workbook.add_format({'num_format': '#0.00', 'font_name': 'Calibri', 'font_size': 11})
        # number_fmt1 = workbook.add_format({'num_format': '#0.000000', 'font_name': 'Calibri', 'font_size': 11})
        date_fmt = workbook.add_format({'num_format': 'd-m-yy'})
        fml_cell_fmt = workbook.add_format({
            'fg_color': '#FFFF00'
        })
        worksheet = writer.sheets[sheet_name]
        # # Monthly columns
        worksheet.set_column('A:A', 5.14)
        worksheet.set_column('B:B', 10.71)
        worksheet.set_column('C:C', 6.29)
        worksheet.set_column('D:D', 17.71)
        worksheet.set_column('E:E', 11.43)

    output = io.BytesIO()
    # import xlsxwriter
    with pd.ExcelWriter(output, engine='xlsxwriter', datetime_format='dd-m-yy', date_format='dd-m-yy') as writer:
        add_sheet('Raw', earning_df, writer)
        writer.save()

    xlsx_data = output.getvalue()
    return Response(
        xlsx_data,
        mimetype='application/vnd.ms-excel',
        headers={'Content-disposition': 'attachment; filename=earning_dates.xlsx'}
    )


def create_edit_link_table(df, heatmap_func, fund='', season=0, pivot_type='', value=''):
    """

    """
    table_html = ''
    style = 'border="1" class="dataframe borderTable"'

    field_separator = '!'
    df.index = df.index.map(lambda x: str(x))
    rows = df.to_csv(sep=field_separator).split('\n')
    table_header = ('<table %s><thead><tr>' % style) + ''.join(
        ['<th style="border-bottom: 1px solid black;">' + h +
         '</th>' for h in rows[0].split(field_separator)]) + '</tr></thead>'

    table_header += '<tbody>'

    table_html += table_header
    left_wall_style = 'border:none; border-left: 1px solid black;'
    default_style = 'border: none;'

    # size = df.index.size

    for r in rows[1:]:

        table_column_title = (
                '<th style="text-align:left;white-space:nowrap; {}">' +
                '<a href="edit?id={}&fund={}&season={}&pivot={}&value={}">{}</a></th>'
        )
        if r != '':
            elements = r.split(field_separator)

            table_html += ('<tr>' +
                           ''.join([table_column_title.format(default_style, elements[0], fund, season,
                                                              pivot_type, value,
                                                              elements[0]
                                                              )
                                    ] +
                                   ['<td style="{}">{}</td>'.format(left_wall_style, h)
                                    # datetime, quick code, name, side, strategy, TPX, GICS
                                    for h in elements[1:8]] +
                                   [generate_table_row(h, '{:.2%}', heatmap_func, -.005, .005, default_style)
                                    for h in elements[8:9]]
                                   ) +
                           '</tr>')

    table_html += '</tbody></table>'

    return table_html


@earnings.route('/breakdown')
def breakdown():
    fund = request.args.get('fund', 'RH')
    pivot_type = request.args.get('type')
    value = request.args.get('value')
    query_selected_season = request.args.get('season')
    if query_selected_season is None or len(query_selected_season.strip()) == 0:
        return "Please select a season"
    selected_season = int(query_selected_season)
    season = EarningSeason.query.filter(EarningSeason.id == selected_season).first()

    error_message = ''
    if not pivot_type or not value:
        error_message = 'no pivot type or value specified'

    if 'side' == pivot_type:
        value = 'L' if value == 'Long' else 'S'

    raw_df = get_raw_df_pickle(season)

    if raw_df is None:
        error_message = error_message + '<br>cannot get raw data'

    if error_message:
        return render_template('main/error_message.html', error_message=error_message)

    filtered_df = raw_df[(raw_df[column_mapping[fund]['PL']] + raw_df[column_mapping[fund]['alpha']] != 0)
                         & (np.abs(raw_df['{}Pos'.format(fund)]) >= MINIMUM_POSITION_SIZE)
                         ]

    if '-side' in pivot_type:
        pivot_type = pivot_type.split('-side')[0]
        values = value.split(' ', 1)
        side_value = values[0]
        if side_value == 'Long':
            side_value = 'L'
        elif side_value == 'Short':
            side_value = 'S'

        # pprint(value.split(values[0] + ' '))
        # type_value = value.split(values[0] + ' ')[1].lstrip()
        type_value = values[1]

        # print('{}={}, {}={}'.format(pivot_type, type_value, 'side', side_value))

        ret = (filtered_df[(filtered_df[pivot_type] == type_value) & (filtered_df['side'] == side_value)]
        [['datetime', 'quick', 'name', 'side', 'strategy', 'TPX', 'GICS', '{}Attribution'.format(fund)]]
        )
    else:
        ret = filtered_df[filtered_df[pivot_type] == value][['datetime', 'quick', 'name', 'side', 'strategy', 'TPX',
                                                             'GICS', '{}Attribution'.format(fund)]]

    edit_link_table = create_edit_link_table(ret, get_heat_map, fund, selected_season, pivot_type, value)
    return render_template('earnings/breakdown.html',
                           table=edit_link_table,
                           type=pivot_type,
                           value=value,
                           fund=fund,
                           season=selected_season,
                           )


def get_raw_df(season):
    """
    get raw_df DataFrame from file composed by parameters
    :param fund: fund of the raw_df
    :param season: season to get data
    :return: DataFrame or None
    """

    pickle_name = get_pickle_name(season)
    raw_df = get_excel_df_pickle(season)
    if raw_df is None:
        raw_df = get_raw_df_pickle(season)

    return pickle_name, raw_df


@earnings.route('/edit', methods=['GET', 'POST'])
def edit():
    if request.method == 'POST':  # get form data and update pickle file accordingly
        id = int(request.form['id'])  # id of record in DataFrame
        fund = request.form['fund']
        selected_season = int(request.form['season'])
        season = EarningSeason.query.filter(EarningSeason.id == selected_season).first()
        pivot_type = request.form['pivot']
        pivot_value = request.form['value']

        pickle_name, raw_df = get_raw_df(season)
        error_message = ''

        if raw_df is None:
            error_message = error_message + '<br>cannot get raw data'

        if error_message:
            return render_template('main/error_message.html', error_message=error_message)

        # update data here:
        edit_entry = raw_df.iloc[id]
        old_earning_date = edit_entry['datetime'].to_pydatetime()  # pandas._libs.tslib.Timestamp
        old_mktcap = edit_entry['MktCap']
        # raw_df.iloc[id, 0] = pd.Timestamp(request.form['earningDate'])
        # raw_df.iloc[id, 17] = request.form['mktcap']

        changed = False
        if old_mktcap != request.form['mktcap']:
            current_app.logger.debug('old_mktcap={}, new={}'.format(old_mktcap, request.form['mktcap']))

            pl_df, existing_instruments, pl_pickle_file = get_jp_equity_pickle_file(season)
            idx1 = pl_df['instrumentID'] == edit_entry['instrumentID']
            pl_df.loc[idx1, 'MktCap'] = request.form['mktcap']
            pl_df.to_pickle(pl_pickle_file)
            raw_df.iloc[id, 17] = request.form['mktcap']
            changed = True

        if old_earning_date != datetime.strptime(request.form['earningDate'], '%Y-%m-%d'):
            earning_df, max_date, earning_pickle_file = get_earning_pickle_file(season)
            reset_earning = earning_df.reset_index()
            idx_earning = reset_earning['instrumentID'] == edit_entry['instrumentID']
            reset_earning.loc[idx_earning, 'datetime'] = datetime.strptime(request.form['earningDate'], '%Y-%m-%d')
            earning_df = reset_earning.set_index('datetime')
            earning_df.to_pickle(earning_pickle_file)
            raw_df.iloc[id, 0] = pd.Timestamp(request.form['earningDate'])
            changed = True

        if changed:
            raw_df.to_pickle(pickle_name)

        flash('Entry is updated', category='message')

        return redirect(url_for('earnings.breakdown',
                                fund=fund,
                                type=pivot_type,
                                value=pivot_value,
                                season=selected_season))
    else:
        id = int(request.args.get('id'))  # id of record in DataFrame
        fund = request.args.get('fund')
        selected_season = int(request.args.get('season'))
        pivot_type = request.args.get('pivot')
        pivot_value = request.args.get('value')

        season = EarningSeason.query.filter(EarningSeason.id == selected_season).first()

        pickle_name, raw_df = get_raw_df(season)

        error_message = ''

        if raw_df is None:
            error_message = error_message + '<br>cannot get raw data'

        if error_message:
            return render_template('main/error_message.html', error_message=error_message)

        current_app.logger.debug(raw_df.iloc[id].tolist()[17])

        return render_template('earnings/edit.html', record=raw_df.iloc[id].tolist(), id=id, season=selected_season,
                               fund=fund, pivot_type=pivot_type, pivot_value=pivot_value
                               )


def get_date_from_str(date_str):
    try:
        ret_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        ret_date = None

    return ret_date


@earnings.route('/seasons', methods=['GET'])
def seasons_list():
    seasons = EarningSeason.query.all()
    return render_template('earnings/seasons_list.html', seasons=seasons)


@earnings.route('/seasons_edit/<int:id>', methods=['GET', 'POST'])
def seasons_edit(id):
    season = EarningSeason.query.get_or_404(id)

    excludes = season.excludes.order_by(EarningExclude.exclude_date).all()
    includes = season.includes.order_by(EarningInclude.include_date).all()

    if request.method == 'POST':
        new_start = get_date_from_str(request.form.get('start'))
        new_end = get_date_from_str(request.form.get('end'))
        new_year = request.form.get('year')
        new_quarter = request.form.get('quarter')

        season.start = new_start
        season.end = new_end
        season.year = new_year
        season.quarter = new_quarter
        db.session.add(season)

        try:
            db.session.commit()
        except IntegrityError:
            db.session.rollback()
            raise

        return redirect(url_for('earnings.index'))

    return render_template('earnings/seasons_edit.html', season=season,
                           excludes1=excludes,
                           includes1=includes
                           )


@earnings.route('/seasons_new', methods=['GET', 'POST'])
def seasons_new():
    if request.method == 'POST':
        new_start = get_date_from_str(request.form.get('start'))
        new_end = get_date_from_str(request.form.get('end'))
        new_year = request.form.get('year')
        new_quarter = request.form.get('quarter')
        season = EarningSeason(start=new_start, end=new_end, year=new_year, quarter=new_quarter)
        db.session.add(season)
        try:
            db.session.commit()
        except IntegrityError:
            current_app.logger.critical('some error occured.')
            db.session.rollback()

        return redirect(url_for('earnings.index'))
    return render_template('earnings/seasons_new.html')


@earnings.route('/seasons_del/<int:id>', methods=['GET'])
def seasons_del(id):
    season = EarningSeason.query.filter(EarningSeason.id == id).first()
    db.session.delete(season)
    try:
        db.session.commit()
    except IntegrityError as e:
        current_app.logger.critical('some error occured: {}'.format(e))
        db.session.rollback()

    return redirect(url_for('earnings.index'))


@earnings.route('/exception_new', methods=['GET', 'POST'])
def exception_new():
    if request.method == 'POST':
        code = request.form.get('code')
        date = get_date_from_str(request.form.get('date'))
        season_id = request.form.get('season_id', None)
        type = request.form.get('type')
        id = request.form.get('id', None)

        if season_id:
            season_id = int(season_id)

        if id:  # for editing exist exclude/include
            id = int(id)
            if type == 'exclude':
                update_target = EarningExclude.query.get_or_404(id)
                update_target.exclude_date = date
            elif type == 'include':
                update_target = EarningInclude.query.get_or_404(id)
                update_target.include_date = date

            else:
                raise ValueError

            update_target.code = code
            db.session.add(update_target)
            try:
                db.session.commit()
                flash('The entry is updated successfully!')
            except IntegrityError:
                current_app.logger.critical('some error occured when updating.')
                db.session.rollback()
        else:  # for creating new exclude/include
            if type == 'exclude':
                ex = EarningExclude(code=code, season_id=season_id, exclude_date=date)
            elif type == 'include':
                ex = EarningInclude(code=code, season_id=season_id, include_date=date)
            else:
                raise ValueError

            db.session.add(ex)
            try:
                db.session.commit()
                id = ex.id
                flash('The entry is inserted successfully!')
            except IntegrityError:
                current_app.logger.critical('some error occured.')
                db.session.rollback()

        return redirect(url_for('earnings.seasons_edit', id=season_id))
    season_id = request.args.get('season_id')
    ex_type = request.args.get('type')
    return render_template('earnings/excludeinclude_new.html',
                           season_id=season_id, type=ex_type,
                           today=datetime.today().strftime('%Y-%m-%d')
                           )


@earnings.route('/exception_edit', methods=['GET'])
def exception_edit():
    id = request.args.get('id')
    ex_type = request.args.get('type')
    today = ''
    if ex_type == 'include':
        ex = EarningInclude.query.get_or_404(id)
        if ex.include_date is not None:
            today = ex.include_date.strftime('%Y-%m-%d')
    else:
        ex = EarningExclude.query.get_or_404(id)
        if ex.exclude_date is not None:
            today = ex.exclude_date.strftime('%Y-%m-%d')

    return render_template('earnings/excludeinclude_new.html',
                           season_id=ex.season_id, type=ex_type,
                           today=today,
                           code=ex.code,
                           id=ex.id
                           )


@earnings.route('/exception_delete', methods=['POST'])
def exception_delete():
    id = request.form.get('id')
    ex_type = request.form.get('type')

    if id is None or len(id) == 0:
        return jsonify(message='Invalid record id.')

    id = int(id)

    if ex_type == 'include':
        ex = EarningInclude.query.get_or_404(id)
    else:
        ex = EarningExclude.query.get_or_404(id)

    db.session.delete(ex)
    try:
        db.session.commit()
        current_app.logger.info('Successfully removed {} with id: {} type = {}'.format(ex.code, ex.id, ex_type))
        return jsonify(message='The entry is removed successfully!')
    except IntegrityError:
        current_app.logger.critical('some error occured.')
        db.session.rollback()
        return jsonify(message='Deletion failed.')


@earnings.route('/graph', methods=['GET'])
def graph():
    # TODO: generate raw_df when it is not ready
    fund = request.args.get('fund')
    selected_seasons = request.args.getlist('season')

    selected_seasons, error_msg = checking_for_valid_seasons(selected_seasons)

    if selected_seasons is None:
        return error_msg

    seasons = [EarningSeason.query.filter(EarningSeason.id == s_id).first() for s_id in selected_seasons]

    raw_df = make_combined_raw_df(seasons)

    error_message = ''

    if raw_df is None:
        error_message = error_message + '<br>cannot get raw data'

    if error_message:
        return render_template('main/error_message.html', error_message=error_message)

    grouped = raw_df.groupby('datetime')

    def win_lose(s, column):
        win = len(s[s[column] > 0])
        lose = -len(s[s[column] < 0])
        return pd.Series([win, lose], index=['win', 'lose'])

    name_map = {
        'RH': 'Rockhampton',
        'YA': 'Yaraka',
        'LR': 'Longreach'
    }

    winlose = grouped.apply(win_lose, ('{}Attribution'.format(fund)))
    winlose = winlose[(winlose['win'] != 0) | (winlose['lose'] != 0)]

    pl_data = raw_df.groupby('datetime').sum()['{}Attribution'.format(fund)]
    if len(seasons) > 1:
        # graph_title = ', '.join(['{} {}Q'.format(season.year, season.quarter) for season in seasons])
        graph_title = '{} - Earnings Season Hit Rate  {} {}Q - {} {}Q'.format(
            name_map[fund],
            seasons[-1].year, seasons[-1].quarter, seasons[0].year, seasons[0].quarter)
        pl_graph_title = '{} - Earnings Season T+1 PL  {} {}Q - {} {}Q'.format(
            name_map[fund],
            seasons[-1].year, seasons[-1].quarter, seasons[0].year, seasons[0].quarter)
    else:
        graph_title = '{} {} {}Q Earnings Season'.format(name_map[fund], seasons[0].year, seasons[0].quarter)
        pl_graph_title = 'Earnings Positions Daily PL ({})'.format(name_map[fund])

    winlose_graph_x = winlose.index.strftime('%Y-%m-%d').tolist()
    annotations = []
    winlose_dict = {col: winlose[col].values.tolist() for col in winlose.columns}
    import json
    for i in range(len(winlose_graph_x)):
        for col in winlose.columns:
            y_value = winlose_dict[col][i]
            if y_value != 0:
                annotations.append({
                    'x': winlose_graph_x[i],
                    'y': y_value if col == 'win' else y_value - 1.6,
                    'text': '{}'.format(y_value),
                    'yanchor': 'bottom',
                    'showarrow': False
                })

    from pprint import pprint
    pprint(annotations)
    winlose_graph = {'data': [{
            'x': winlose_graph_x,
            'y': winlose_dict[col],
            'width': 0.5,
            'type': 'bar',
            'name': 'Correct Call On Earnings' if col == 'win' else 'Incorrect Call On Earnings',
            'marker': {
                'color': 'rgb(47, 85, 151)' if col == 'win' else 'rgb(193, 111, 111)'
            }
        } for col in winlose.columns
        ],
        'layout': {
            'title': graph_title,
            'barmode': 'relative',
            'xaxis': {
                #             'tickvals': x,
                #             'ticktext': x,
                'type': 'category'
            },
            'yaxis': {
              'autotick': 'false'
            },
            'legend': {
                'orientation': 'h',
                'xanchor': 'auto',
                'x': 0.5,
                'y': -0.4
            },
            'annotations': annotations
        }
    }

    pl_data = pl_data[pl_data != 0]
    x_axis = pl_data.index.strftime('%Y-%m-%d').tolist()
    pl_graph = {'data': [{
            'x': x_axis,
            'y': (pl_data * 100).values.tolist(),
            'width': 0.5,
            'type': 'bar',
            'name': 'PnL',
            'marker': {
                'color': 'rgb(0, 0, 0)'
            }
        }] + ([{
                'x': x_axis,
                'y': (pl_data.cumsum() * 100).values.tolist(),
                'name': 'Cumulative PnL',
                'line': {'color': 'rgb(0,0,255)'},
                'yaxis': 'y2'
        }] if len(seasons) > 1 else []),
        'layout': {
            'title': pl_graph_title,
            'yaxis': {
                'ticksuffix': '%'
            },
            'xaxis': {
                #             'tickvals': x,
                #             'ticktext': x,
                'type': 'category'
            },
            'yaxis2': {
                'ticksuffix': '%',
                'overlaying': 'y',
                'side': 'right',
               #  'linecolor': 'rgb(0,0,255)'
                'tickfont':  {
                #'family='Old Standard TT, serif',
                #'size=14,
                 'color': 'rgb(0,0,255)'
                },
            },
            'legend': {
                'orientation': 'h',
                'xanchor': 'auto',
                'x': 0.5,
                'y': -0.4
                }

        }
    }

    graph_json = json.dumps(winlose_graph, cls=plotly.utils.PlotlyJSONEncoder)

    return render_template('earnings/graph.html',
                           season=selected_seasons if len(selected_seasons) > 1 else selected_seasons[0],
                           fund=fund,
                           winlose_graph=graph_json,
                           pl_graph=pl_graph
                           )


@earnings.route('/excel', methods=['GET'])
def get_excel():

    str_selected_season = request.args.get('season')
    if str_selected_season is None or len(str_selected_season) == 0:
        return "Invalid season selected"

    selected_season = int(str_selected_season)

    season = EarningSeason.query.filter(EarningSeason.id == selected_season).first()

    if season is None or not season:
        return "Invalid season selected"

    raw_df = make_combined_raw_df([season])

    def add_sheet(sheet_name, data_pd, writer):
        number_of_rows = data_pd.index.size
        data_pd.to_excel(writer, sheet_name=sheet_name, index=False, startrow=1, startcol=3)
        workbook = writer.book
        # # Add a number format for cells with money.
        # number_fmt = workbook.add_format({'num_format': '#0.00', 'font_name': 'Calibri', 'font_size': 11})
        # number_fmt1 = workbook.add_format({'num_format': '#0.000000', 'font_name': 'Calibri', 'font_size': 11})
        date_fmt = workbook.add_format({'num_format': 'd-m-yy'})
        fml_cell_fmt = workbook.add_format({
            'fg_color': '#FFFF00'
        })
        quick_code_fmt = workbook.add_format({'num_format': '####'})
        worksheet = writer.sheets[sheet_name]
        # # Monthly columns
        worksheet.set_column('A:C', 4.14)
        worksheet.set_column('D:D', 16, date_fmt)
        worksheet.set_column('E3:E{}'.format(3 + number_of_rows - 1), 7.29, quick_code_fmt)
        worksheet.set_column('F:F', 35)
        worksheet.set_column('I:I', 12)
        worksheet.set_column('J:J', 31.14)
        worksheet.set_column('K:K', 26)
        worksheet.set_column('L:Z', 17.29)

        worksheet.write('A2', 'RH', fml_cell_fmt)
        worksheet.write('B2', 'YA', fml_cell_fmt)
        worksheet.write('C2', 'LR', fml_cell_fmt)
        for c in range(number_of_rows):
            i = c+3
            worksheet.write_formula('A{}'.format(i),
                                    '=IF(OR(L{}+O{}=0,ABS(X{})<0.02%),0,MAX(A$2:A{})+1)'.format(i, i, i, i-1),
                                    fml_cell_fmt
                                    )
            worksheet.write_formula('B{}'.format(i),
                                    '=IF(A{}>0,A{},IF(ABS(Y{})>0.02%,MAX(D$1,B{}:B$3)+1,0))'.format(i, i, i, i-1),
                                    fml_cell_fmt
                                    )
            worksheet.write_formula('C{}'.format(i),
                                    '=IF(A{}>0,A{},IF(ABS(Z{})>0.02%,MAX(D$1,C{}:C$3)+1,0))'.format(i, i, i, i-1),
                                    fml_cell_fmt
                                    )
        worksheet.write_formula('D1', '=MAX(A3:A5002)', quick_code_fmt)

    current_app.logger.debug(raw_df.columns)
    excel_df = raw_df[['datetime', 'quick', 'name', 'side', 'advisor', 'strategy', 'TPX', 'GICS',
                       'RHAttribution', 'YAAttribution', 'LRAttribution',
                       'RHMktAlpha', 'YAMktAlpha', 'LRMktAlpha',
                       'PostRHAtt', 'PostYAAtt', 'PostLRAtt',
                       'PostRHAlpha', 'PostYAAlpha', 'PostLRAlpha',
                       'RHPos', 'YAPos', 'LRPos',
                       'RHAttComb', 'YAAttComb', 'LRAttComb',
                       'RHAlComb', 'YAAlComb', 'LRAlComb',
                       'MktCap'
                       ]]
    new_columns = {
        'datetime': 'EarningsDate'
    }
    excel_df = excel_df.rename(columns=new_columns)
    excel_df[['quick']] = excel_df[['quick']].astype(np.int32)
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine='xlsxwriter', datetime_format='dd-m-yy', date_format='dd-m-yy') as writer:
        add_sheet('Raw', excel_df, writer)
        writer.save()

    xlsx_data = output.getvalue()
    return Response(
        xlsx_data,
        mimetype='application/vnd.ms-excel',
        headers={'Content-disposition': 'attachment; filename=raw_df.xlsx'}
    )


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def read_xlsx(file_path):
    current_app.logger.debug(file_path)
    mktcap_mapping = {
        250: 'Micro',
        1000: 'Small',
        5000: 'Mid',
        10000: 'Large',
        10001: 'Mega'
    }
    ret = (pd.read_excel('file://localhost{}/{}'.format(os.getcwd(), file_path),
                         sheet_name='Raw',
                         skiprows=[0],
                         na_values=['x'],
                         usecols=[i + 3 for i in range(30)]
                         )
           .dropna(axis=0, how='all')
           .rename(columns={'EarningsDate': 'datetime',
                            'PostRHAlph': 'PostRHAlpha',
                            'PostYAAlph': 'PostYAAlpha',
                            'PostLRAlph': 'PostLRAlpha',
                            'YAAlcomb': 'YAAlComb'
                            }
                   )
           .assign(MktCap=lambda df: df['Cap'].map(mktcap_mapping))

           )
    current_app.logger.debug(ret.tail())
    ret['quick'] = ret['quick'].map(lambda x: '{}'.format(int(x)))
    return ret


@earnings.route('/upload', methods=['POST'])
def upload():
    if request.method == 'POST':
        selected_season = request.form.get('season_id')
        # check if the post request has the file part
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file']
        # if user does not select file, browser also
        # submit a empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            current_app.logger.debug('upload_folder={}'.format(current_app.config['UPLOAD_FOLDER']))
            file_path = os.path.join(current_app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)
            xlsx_pd = read_xlsx(file_path)
            xlsx_pd.to_pickle('data/xlsx_{}'.format(selected_season))
            flash('File is uploaded successfully.')
            return redirect(url_for('earnings.index',
                                    filename=filename))

