import json
import requests
import pandas as pd
import numpy as np
from datetime import date
from common.auth import read_creds, headers2
from common.common_functions import *

# Need to import credentials
creds = read_creds()
access_token = creds['access_token']
headers_v2 = headers2(access_token)

# Import organization code for API
params = read_configuration_file()
organization_code = params['organization_code']


def followers_daily(start_epoch, end_epoch):
    response = requests.get(
        'https://api.linkedin.com/v2/organizationalEntityFollowerStatistics?q=organizationalEntity&organizationalEntity=urn%3Ali%3Aorganization%3A'+organization_code+'&timeIntervals=(timeRange:(start:' + str(start_epoch) + ',end:' + str(end_epoch) + '),timeGranularityType:DAY)',
        headers=headers_v2)
    data = json.loads(response.text)

    organicFollowerGain = [data['elements'][i]['followerGains']['organicFollowerGain'] for i in
                           range(len(data['elements']))]

    paidFollowerGain = [data['elements'][i]['followerGains']['paidFollowerGain'] for i in range(len(data['elements']))]

    start = [date.fromtimestamp(x / 1000) for x in
             [data['elements'][i]['timeRange']['start'] for i in range(len(data['elements']))]]

    end = [date.fromtimestamp(x / 1000) for x in
           [data['elements'][i]['timeRange']['end'] for i in range(len(data['elements']))]]

    total_gain_array = np.array([organicFollowerGain, paidFollowerGain])
    total_gain = np.sum(total_gain_array, 0)

    dic = {'start_date': start,
           'end_date': end,
           'organicFollowerGain': organicFollowerGain,
           'paidFollowerGain': paidFollowerGain,
           'TotalGain': total_gain}

    followers_daily_df = pd.DataFrame(dic)

    return followers_daily_df


if __name__ == '__main__':

    try:
        followers_daily_df = followers_daily(start_epoch, end_epoch)
        write_to_csv(followers_daily_df, get_filepath('followers_daily_df.csv'))
    except Exception as e:
        print(e)
    else:
        log = "Successfully written output to: " + get_filepath('followers_daily_df.csv')
        print(log)
        # logOutput(get_filepath('logging_info.txt'), log)
