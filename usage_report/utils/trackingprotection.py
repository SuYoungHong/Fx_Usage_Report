import datetime

# from pyspark.sql.functions import col, lit, mean, split
import pyspark.sql.functions as F


def pct_tracking_protection(data,
                            date,
                            period=7,
                            country_list=None):
    """ Calculate proportion of users in WAU that have a
        tracking protection = on session/window (at least 1)
        Parameters:
        data: spark df, main summary
        date: string, with the format 'yyyyMMdd'
        period: int, period to check proportion for, 7 for WAU
        country_list: a list of country names in string

        Returns:
        a spark df with the following columns
        - columns: | submission_date_s3 | country | pct_TP |
    """
    enddate = datetime.datetime.strptime(date, '%Y%m%d')
    begin = enddate - datetime.timedelta(days=period)
    begin = begin.strftime('%Y%m%d')

    data_all = data.drop('country')\
                   .select('submission_date_s3',
                           'client_id',
                           F.col('histogram_parent_tracking_protection_enabled.1').alias('TP_on'),
                           F.lit('All').alias('country'))

    if country_list:
        data_countries = (
          data.filter(F.col('country').isin(country_list))
              .select('submission_date_s3',
                      'client_id',
                      F.col('histogram_parent_tracking_protection_enabled.1').alias('TP_on'),
                      'country'))
        data_all = data_all.union(data_countries)

    def get_number_of_users(df, count_name):
        return df.groupBy('country')\
                 .agg(F.countDistinct('client_id').alias(count_name))

    WAU = get_number_of_users(
                      data_all.filter("""submission_date_s3 <= '{}'
                                         and submission_date_s3 > '{}'
                                      """.format(date, begin)),
                      'WAU')
    WAU_TP = get_number_of_users(
                      data_all.filter("""submission_date_s3 <= '{}'
                                         and submission_date_s3 > '{}'
                                      """.format(date, begin))
                              .filter(F.col('TP_on') > 0),
                      'WAU_TP')

    join_df = WAU.join(WAU_TP, 'country', 'left')\
                 .withColumn("pct_TP", (100.0 * F.col("WAU_TP") / F.col("WAU")))\
                 .select(F.lit(date).alias('submission_date_s3'),
                         'country',
                         F.coalesce('pct_TP', F.lit(0)).alias('pct_TP'))
    return join_df

def pct_etp(data,
            date,
            period=7,
            country_list=None):
    """ Calculate proportion of users in WAU that have a
        extended tracking protection = on for a session (at least 1)
        Parameters:
        data: spark df, main summary
        date: string, with the format 'yyyyMMdd'
        period: int, period to check proportion for, 7 for WAU
        country_list: a list of country names in string

        Returns:
        a spark df with the following columns
        - columns: | submission_date_s3 | country | pct_ETP |
    """
    enddate = datetime.datetime.strptime(date, '%Y%m%d')
    begin = enddate - datetime.timedelta(days=period)
    begin = begin.strftime('%Y%m%d')

    data_all = data.drop('country')\
                   .select('submission_date_s3',
                           'client_id',
                           F.col('histogram_parent_cookie_behavior.4').alias('ETP_on'),
                           F.lit('All').alias('country'))

    if country_list:
        data_countries = (
          data.filter(F.col('country').isin(country_list))
              .select('submission_date_s3',
                      'client_id',
                      F.col('histogram_parent_cookie_behavior.4').alias('ETP_on'),
                      'country'))
        data_all = data_all.union(data_countries)

    def get_number_of_users(df, count_name):
        return df.groupBy('country')\
                 .agg(F.countDistinct('client_id').alias(count_name))

    WAU = get_number_of_users(
                      data_all.filter("""submission_date_s3 <= '{}'
                                         and submission_date_s3 > '{}'
                                      """.format(date, begin)),
                      'WAU')
    WAU_ETP = get_number_of_users(
                      data_all.filter("""submission_date_s3 <= '{}'
                                         and submission_date_s3 > '{}'
                                      """.format(date, begin))
                              .filter(F.col('ETP_on') > 0),
                      'WAU_ETP')

    join_df = WAU.join(WAU_ETP, 'country', 'left')\
                 .withColumn("pct_ETP", (100.0 * F.col("WAU_ETP") / F.col("WAU")))\
                 .select(F.lit(date).alias('submission_date_s3'),
                         'country',
                         F.coalesce('pct_ETP', F.lit(0)).alias('pct_ETP'))
    return join_df
