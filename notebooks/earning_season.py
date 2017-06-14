import pandas as pd
import japandas as jpd


def get_earning_df(con, start_date, end_date):

    calendar = jpd.JapaneseHolidayCalendar()
    cday = pd.offsets.CDay(calendar=calendar)

    # select a list of announcement dates for instruments we have in specified period
    # need to make sure each instrument have only one announcement date, if more than 1,
    # choose date come earlier.
    earning_df = pd.read_sql("""
        SELECT
          b.instrumentID,
          b.earningYear,
          b.period,
          CONCAT(b.announcement_date, ' ', b.announcement_time) AS datetime
        FROM hkg02p.t09BBEarningAnnouncement b
          INNER JOIN (
                       SELECT
                         b.instrumentID,
                         MIN(b.announcement_date) AS announcement_date
                       FROM hkg02p.t09BBEarningAnnouncement b
                         INNER JOIN hkg02p.t01Instrument c ON b.instrumentID = c.instrumentID AND c.instrumentType = "EQ"
                       WHERE b.announcement_date >= '{}' AND b.announcement_date <= '{}'
                             AND b.instrumentID IN (SELECT DISTINCT z.instrumentID
                                                    FROM hkg02p.t05PortfolioResponsibilities z
                                                    WHERE z.processDate >= '{}' AND z.processDate <= '{}' 
                                                      AND z.CCY = 'JPY')
                             AND c.quick NOT IN
                                 ("1728", "1867", "2384", "4987", "5973", "6744", "6870", "8031", "8793", "9003", "9044", "9048", "9501", "9503", "9505", "9508", "9722")
                       GROUP BY b.instrumentID
                       ORDER BY b.announcement_date
                     ) c ON b.instrumentID = c.instrumentID AND b.announcement_date = c.announcement_date
        ORDER BY b.announcement_date
    """.format(start_date, end_date, start_date, end_date), con, parse_dates={'datetime': '%Y-%m-%d %H:%M:%S'},
                             index_col='datetime')

    # print("""
    #     SELECT
    #       b.instrumentID,
    #       b.earningYear,
    #       b.period,
    #       CONCAT(b.announcement_date, ' ', b.announcement_time) AS datetime
    #     FROM hkg02p.t09BBEarningAnnouncement b
    #       INNER JOIN hkg02p.t01Instrument c ON b.instrumentID = c.instrumentID AND c.instrumentType = "EQ"
    #     WHERE b.announcement_date >= '{}' AND b.announcement_date <= '{}'
    #           AND b.instrumentID IN (SELECT DISTINCT z.instrumentID
    #                                  FROM hkg02p.t05PortfolioResponsibilities z
    #                                  WHERE z.processDate >= '{}' AND z.processDate <= '{}' AND z.CCY = 'JPY')
    #           AND c.quick NOT IN
    #               ("1728", "1867", "2384", "4987", "5973", "6744", "6870", "8031", "8793", "9003", "9044", "9048", "9501", "9503", "9505", "9508", "9722")
    #     ORDER BY b.announcement_date;
    # """.format(start_date, end_date, start_date, end_date))

    earning_df.index = earning_df.index + pd.Timedelta(hours=9)
    earning_df.index = earning_df.index + cday * 0
    earning_df.index = earning_df.index.normalize()  # .sort_values()
    earning_df = earning_df.sort_index()

    return earning_df


def get_jp_equity_pl(con, start_date, end_date):
    pl_df = pd.read_sql("""
        SELECT
          a.processDate,
          IF(a.side = "L", a.firstTradeDateLong, a.firstTradeDateShort) AS firstTradeDate,
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
          IF(a.side = "S", -a.LRExposure, a.LRExposure)                 AS LRPos
        FROM hkg02p.t05PortfolioResponsibilities a
          INNER JOIN hkg02p.t01Instrument c ON a.instrumentID = c.instrumentID 
            AND c.instrumentType = "EQ" 
            AND c.currencyID = 1
          INNER JOIN (
                       SELECT
                         a.instrumentID,
                         a.processDate,
                         SUM(a.quantity) AS quantity
                       FROM hkg02p.t05PortfolioPosition a
                       WHERE a.processDate >= '{}' AND a.processDate <= '{}' AND a.equityType = 'EQ'
                       GROUP BY a.instrumentID, a.processDate
                     ) d ON a.instrumentID = d.instrumentID AND a.processDate = d.processDate
        WHERE a.processDate >= '{}' AND a.processDate <= '{}'
              # thematic name - anything where sensitivity = THEME, means it's not an earnings play
              AND (a.sensitivity <> 'THEME' OR (a.quick IN ("7532") AND a.processDate >= '2017-05-09'))
              # filter out position we did not have
              AND a.processDate <> IF(a.side = "L", a.firstTradeDateLong, a.firstTradeDateShort)
        ORDER BY a.processDate
    """.format(start_date, end_date, start_date, end_date), con, parse_dates=['processDate'])

    return pl_df
