# https://towardsdatascience.com/why-and-how-to-use-pandas-with-large-data-9594dda2ea4c

# df_chunk = pd.read_csv(r'../input/data.csv', chunksize=1000000)

import pandas as pd

import psycopg2 as pg
import pandas.io.sql as psql

# get connected to the database
connection = pg.connect("dbname='postgres' user='postgres' host='localhost' port='5433' password='admin'")
# connection = pg.connect("jdbc:postgresql://localhost:5433/postgres")


def get_analytic_window_traffic_difference(checked_detector_id):
    query = f"""
    (
        WITH ordered_series AS (
          SELECT starttime, count FROM traffic WHERE detector_id={checked_detector_id} ORDER BY starttime
          ) SELECT
              starttime,
              count,
              ABS(count - (LAG(count, 1) OVER ( ORDER BY starttime ))) traffic_diff
            FROM ordered_series
      )
    """

    return pd.read_sql_query(query, con=connection)


def get_traffic_data(checked_detector_id):
    query = f"SELECT * FROM traffic WHERE detector_id={checked_detector_id} ORDER BY starttime"
    return pd.read_sql_query(query, con=connection)


def get_analytic_window_traffic_diff(checked_detector_id):
    traffic_data_df = get_traffic_data(checked_detector_id)
    traffic_data_df['prev_count'] = traffic_data_df['count'].shift(-1)
    traffic_data_df['count_diff'] = (traffic_data_df['count'] - traffic_data_df['prev_count']).abs()
    return traffic_data_df


def get_traffic_diff_quantile(traffic_data_df, quantile):
    return traffic_data_df['count_diff'].quantile(quantile)


def get_thresholds(traffic_diff_df):
    percentile_25 = get_traffic_diff_quantile(traffic_diff_df, 0.25)
    percentile_75 = get_traffic_diff_quantile(traffic_diff_df, 0.75)
    iqr = percentile_75-percentile_25
    neg_outlier_threshold = percentile_25 - 1.5*iqr
    pos_outlier_threshold = percentile_75 + 1.5*iqr
    return neg_outlier_threshold, pos_outlier_threshold


def check_traffic_diff_for_anomaly(detector_id):
    traffic_diff_df = get_analytic_window_traffic_diff(detector_id)
    neg_outlier_threshold, pos_outlier_threshold = get_thresholds(traffic_diff_df)

    for index, row in traffic_diff_df.iterrows():
        if row['count_diff'] > pos_outlier_threshold:
            print('Noticed POSITIVE OUTLIER anomaly in detector {} at: {}. Difference between this reading and previous is {}'.format(detector_id, row['starttime'], row['count_diff']))
        elif row['count_diff'] < neg_outlier_threshold:
            print('Noticed NEGATIVE OUTLIER anomaly in detector {} at: {}. Difference between this reading and previous is {}'.format(detector_id, row['starttime'], row['count_diff']))


if __name__ == '__main__':
    check_traffic_diff_for_anomaly(1)
    # print(res)
