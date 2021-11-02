from followers_daily import followers_daily
from followers import follower_statistics
import pandas as pd
from common.common_functions import *


def agg_df(df, total_followers):

    df['month_year'] = pd.to_datetime(df['start_date']).dt.to_period('M')
    monthly_followers = df.groupby(['month_year'])['organicFollowerGain'].sum()
    aggregate_df = monthly_followers.to_frame()
    aggregate_df['reverse_count'] = aggregate_df.organicFollowerGain.values[::-1]
    aggregate_df['reverse_cum_sum'] = aggregate_df['reverse_count'].cumsum()
    aggregate_df['cum_sum'] = aggregate_df.reverse_cum_sum.values[::-1]
    aggregate_df['followers'] = total_followers
    aggregate_df['monthly_followers'] = aggregate_df['followers'] - aggregate_df['cum_sum']
    aggregate_df['month_end_followers'] = aggregate_df['monthly_followers'] + aggregate_df['organicFollowerGain']
    fin_agg_df = aggregate_df.drop(columns=['reverse_count', 'reverse_cum_sum'])

    return fin_agg_df


if __name__ == '__main__':

    try:
        followers_daily_df = followers_daily(start_epoch, end_epoch)
        follower_stats = follower_statistics()

        all_followers = sum([follower_stats['elements'][0]['followerCountsByAssociationType'][i]['followerCounts']['organicFollowerCount'] for i in range(len(follower_stats['elements'][0]['followerCountsByAssociationType']))])
        followers_month_end = agg_df(followers_daily_df, all_followers)

        writer = pd.ExcelWriter(get_filepath('followers_month_end.xlsx'), engine='xlsxwriter')
        followers_month_end.to_excel(writer, sheet_name='followers_month_end')
        writer.save()
    except Exception as e:
        print(e)
    else:
        log = "Successfully written output to: " + get_filepath('followers_month_end.csv')
        print(log)
        # logOutput(get_filepath('logging_info.txt'), log)
