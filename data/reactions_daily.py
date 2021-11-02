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


def reactions_daily(start_epoch, end_epoch):
    response = requests.get(
        'https://api.linkedin.com/v2/organizationalEntityShareStatistics?q=organizationalEntity&organizationalEntity=urn%3Ali%3Aorganization%3A'+organization_code+'&timeIntervals=(timeRange:(start:' + str(start_epoch) + ',end:' + str(end_epoch) + '),timeGranularityType:DAY)',
        headers=headers_v2)
    data = json.loads(response.text)

    uniqueImpressionsCount = [data['elements'][i]['totalShareStatistics']['uniqueImpressionsCount'] for i in
                              range(len(data['elements']))]

    shareCount = [data['elements'][i]['totalShareStatistics']['shareCount'] for i in range(len(data['elements']))]

    engagement = ["{:.2%}".format(data['elements'][i]['totalShareStatistics']['engagement']) for i in
                  range(len(data['elements']))]

    clickCount = [data['elements'][i]['totalShareStatistics']['clickCount'] for i in range(len(data['elements']))]

    likeCount = [data['elements'][i]['totalShareStatistics']['likeCount'] for i in range(len(data['elements']))]

    impressionCount = [data['elements'][i]['totalShareStatistics']['impressionCount'] for i in
                       range(len(data['elements']))]

    commentCount = [data['elements'][i]['totalShareStatistics']['commentCount'] for i in range(len(data['elements']))]

    start = [date.fromtimestamp(x / 1000) for x in
             [data['elements'][i]['timeRange']['start'] for i in range(len(data['elements']))]]

    end = [date.fromtimestamp(x / 1000) for x in
           [data['elements'][i]['timeRange']['end'] for i in range(len(data['elements']))]]

    total_reaction_array = np.array([clickCount, likeCount, commentCount, shareCount])
    total_reaction = np.sum(total_reaction_array, 0)

    dic = {'start_date': start,
           'end_date': end,
           'uniqueImpressionsCount': uniqueImpressionsCount,
           'shareCount': shareCount,
           'engagement': engagement,
           'clickCount': clickCount,
           'likeCount': likeCount,
           'impressionCount': impressionCount,
           'commentCount': commentCount,
           'TotalReaction': total_reaction}

    reactions_daily_df = pd.DataFrame(dic)

    return reactions_daily_df


if __name__ == '__main__':

    try:
        reactions_daily_df = reactions_daily(start_epoch, end_epoch)
        write_to_csv(reactions_daily_df, get_filepath('reactions_daily_df.csv'))
    except Exception as e:
        print(e)
    else:
        log = "Successfully written output to: " + get_filepath('reactions_daily_df.csv')
        print(log)
