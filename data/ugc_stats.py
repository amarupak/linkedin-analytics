import json
import requests
import pandas as pd
from datetime import date

from common.auth import read_creds, read_configuration_file, headers1, headers2
from common.common_functions import get_filepath

# Need to import credentials
creds = read_creds()
access_token = creds['access_token']
headers_v1 = headers1(access_token)
headers_v2 = headers2(access_token)

# Import number of posts to retrieve
params = read_configuration_file()
post_count = params['post_count']
organization_code = params['organization_code']


# Get basic UGC posts and shares data
def ugc_data():
    response = requests.get(
        'https://api.linkedin.com/v2/ugcPosts?q=authors&authors=List(urn%3Ali%3Aorganization%3A'+organization_code+')&sortBy=CREATED&count=' + post_count,
        headers=headers_v2)
    x = json.loads(response.text)

    if 'elements' in x.keys():

        lifecycleState = [x['elements'][i]['lifecycleState'] for i in range(len(x['elements']))]

        ugc_text = [x['elements'][i]['specificContent']['com.linkedin.ugc.ShareContent']['shareCommentary']['text'] for
                    i in range(len(x['elements']))]

        ugc_id = [x['elements'][i]['id'] for i in range(len(x['elements']))]

        post_type = [a + 's' for a in
                     [sub.split(':')[2] for sub in [x['elements'][i]['id'] for i in range(len(x['elements']))]]]

        cleaned_id = [int(sub.split(':')[3]) for sub in [x['elements'][i]['id'] for i in range(len(x['elements']))]]

        lastModified = [date.fromtimestamp(x / 1000) for x in
                        [x['elements'][i]['created']['time'] for i in range(len(x['elements']))]]

        time_stamp = []
        for i in range(len(x['elements'])):
            if 'firstPublishedAt' in x['elements'][i]:
                time_stamp.append(x['elements'][i]['firstPublishedAt'])
            else:
                time_stamp.append(x['elements'][i]['created']['time'])

        CreatedDate = [date.fromtimestamp(x / 1000) for x in time_stamp]

        dic = {'lifecycleState': lifecycleState,
               'CreatedDate': CreatedDate,
               'id': ugc_id,
               'post_type': post_type,
               'reference': cleaned_id,
               'text': ugc_text}

        df = pd.DataFrame(dic)

        ugc_data_df = df[df['lifecycleState'].str.contains("PUBLISHED")]

        ugc_data_df = ugc_data_df.reset_index(drop=True)

        return ugc_data_df

    else:
        return "Token might be expired"


# Get social ststistics for the above posts
def share_statistics(ugc_data_df):
    share_stats_api = 'https://api.linkedin.com/v2/organizationalEntityShareStatistics?q=organizationalEntity&organizationalEntity=urn:li:organization:'+organization_code+'&'

    ugc_data_df['share_stats_api'] = share_stats_api + ugc_data_df['post_type'] + '[0]' + '=' + ugc_data_df['id']

    share_stats_api_list = ugc_data_df['share_stats_api'].tolist()

    share_stats = []
    for api in share_stats_api_list:
        res = requests.get(api, headers=headers_v1)
        if res.status_code == 200:
            share_stats.append(res.json())
        else:
            pass

    post_type_id = []
    for i in range(len(share_stats)):
        if 'ugcPost' in share_stats[i]['elements'][0]:
            post_type_id.append(share_stats[i]['elements'][0]['ugcPost'])
        else:
            post_type_id.append(share_stats[i]['elements'][0]['share'])

    shareCount = [share_stats[i]['elements'][0]['totalShareStatistics']['shareCount'] for i in range(len(share_stats))]

    likeCount = [share_stats[i]['elements'][0]['totalShareStatistics']['likeCount'] for i in range(len(share_stats))]

    engagement = ["{:.2%}".format(share_stats[i]['elements'][0]['totalShareStatistics']['engagement']) for i in
                  range(len(share_stats))]

    clickCount = [share_stats[i]['elements'][0]['totalShareStatistics']['clickCount'] for i in range(len(share_stats))]

    impressionCount = [share_stats[i]['elements'][0]['totalShareStatistics']['impressionCount'] for i in
                       range(len(share_stats))]

    commentCount = [share_stats[i]['elements'][0]['totalShareStatistics']['commentCount'] for i in
                    range(len(share_stats))]

    dic = {'id': post_type_id,
           'shareCount': shareCount,
           'likeCount': likeCount,
           'engagement': engagement,
           'clickCount': clickCount,
           'impressionCount': impressionCount,
           'commentCount': commentCount}

    stats = pd.DataFrame(dic)

    return stats


# def comments(z):
#     num_comments = len(z['elements'])
#     nested_comments=[]
#     for i in range(len(z['elements'])):
#       if 'commentsSummary' in z['elements'][i]:
#         nested_comments.append(z['elements'][i]['commentsSummary']['aggregatedTotalComments'])

#     return sum(nested_comments, num_comments)

# def likes_count(y):
#     return len(y['elements'])


# Get activityid for the stats
def activity_data(stats_df):
    activity_api = 'https://api.linkedin.com/v2/socialActions/'

    stats_df['activity_api'] = activity_api + stats_df['id']

    activity_api_list = stats_df['activity_api'].tolist()

    activity = []
    for api in activity_api_list:
        res = requests.get(api, headers=headers_v1)
        if res.status_code == 200:
            activity.append(res.json())
        else:
            pass

    activity_id = [activity[i]['target'] for i in range(len(activity))]

    stats_df['activity_id'] = activity_id

    return stats_df


# Get actors for likes
def get_like_actors(stats_df):
    stats_df['activity_api'] = 'https://api.linkedin.com/v2/socialActions/' + stats_df['id'] + '/likes?count=200'

    likes_activity_api_list = stats_df['activity_api'].tolist()

    data = []
    for api in likes_activity_api_list:
        res = requests.get(api, headers=headers_v1)
        if res.status_code == 200:
            data.append(res.json())
        else:
            pass

    activity_stats = []
    for i in range(len(data)):
        for j in range(len(data[i]['elements'])):
            activity_stats.append(data[i]['elements'][j])

    actor = [activity_stats[i]['actor'] for i in range(len(activity_stats))]

    actor_id = [sub.split(':')[3] for sub in [activity_stats[i]['actor'] for i in range(len(activity_stats))]]

    URN = [activity_stats[i]['$URN'] for i in range(len(activity_stats))]

    time_stamp = [date.fromtimestamp(x / 1000) for x in
                  [activity_stats[i]['lastModified']['time'] for i in range(len(activity_stats))]]

    _object = [activity_stats[i]['object'] for i in range(len(activity_stats))]

    dic = {'actor': actor,
           'actor_id': actor_id,
           'time_stamp': time_stamp,
           '$URN': URN,
           'object_ref': _object}

    actors = pd.DataFrame(dic)

    return actors


# Get profiles for the actors - used for both likes and comments
def get_profile(actors):
    actors['actors_info'] = 'https://api.linkedin.com/v2/people/(id:' + actors['actor_id'] + ')'

    actors_info_api = actors['actors_info'].unique()

    profile_api_list = actors_info_api.tolist()

    data = []
    for api in profile_api_list:
        res = requests.get(api, headers=headers_v2)
        if res.status_code == 200:
            data.append(res.json())
        else:
            pass

    actor_id = [data[i]['id'] for i in range(len(data))]

    first_name = [data[i]['localizedFirstName'] if 'localizedFirstName' in data[i] else '' for i in range(len(data))]

    last_name = [data[i]['localizedLastName'] if 'localizedLastName' in data[i] else '' for i in range(len(data))]

    headline = [data[i]['localizedHeadline'] if 'localizedHeadline' in data[i] else '' for i in range(len(data))]

    company = ["BCI" if i.find('BCI') != -1 else "external" for i in headline]

    dic = {'id': actor_id,
           'first_name': first_name,
           'last_name': last_name,
           'localizedHeadline': headline,
           'company': company}

    profile = pd.DataFrame(dic)

    return profile


# Get actors for comments
def get_comment_actors(stats_df):
    stats_df['activity_api'] = 'https://api.linkedin.com/v2/socialActions/' + stats_df['id'] + '/comments?count=200'

    social_activity_api_list = stats_df['activity_api'].tolist()

    data = []
    for api in social_activity_api_list:
        res = requests.get(api, headers=headers_v1)
        if res.status_code == 200:
            data.append(res.json())
        else:
            pass

    activity_stats = []
    for i in range(len(data)):
        for j in range(len(data[i]['elements'])):
            activity_stats.append(data[i]['elements'][j])

    actor = [activity_stats[i]['actor'] for i in range(len(activity_stats))]

    actor_id = [sub.split(':')[3] for sub in [activity_stats[i]['actor'] for i in range(len(activity_stats))]]

    URN = [activity_stats[i]['$URN'] for i in range(len(activity_stats))]

    time_stamp = [date.fromtimestamp(x / 1000) for x in
                  [activity_stats[i]['lastModified']['time'] for i in range(len(activity_stats))]]

    _object = [activity_stats[i]['object'] for i in range(len(activity_stats))]

    dic = {'actor': actor,
           'actor_id': actor_id,
           'time_stamp': time_stamp,
           '$URN': URN,
           'object_ref': _object}

    comment_actors = pd.DataFrame(dic)

    return comment_actors


# Get actors current company
def current_company(df):
    if pd.isna(df['localizedHeadline']) and df['actor'] == 'urn:li:organization:'+organization_code:
        return 'BCI'
    else:
        return df['company']


# Calculate external likes and comments by object_ref
def actors_profile_transformations(df):
    df.company.fillna('external', inplace=True)
    non_bci_df = df[df['company'] != 'BCI']
    agg_df = non_bci_df[['object_ref', 'company']].groupby(['object_ref']).agg('count')
    return agg_df


# Cleaning external data
def external_activity_transformations(external_activity_df):
    external_activity_df.columns = ['external_likes', 'external_comments']
    external_activity_df.external_comments.fillna(0, inplace=True)
    external_activity_df.external_likes.fillna(0, inplace=True)
    return external_activity_df


# Final transformations and dataframe
def final_df(ext_df):
    ext_df['ExternalLikes'] = (ext_df['external_likes'] / ext_df['likeCount'])
    ext_df['ExternalComments'] = (ext_df['external_comments'] / ext_df['commentCount'])
    final_df = ext_df.drop(columns=['lifecycleState', 'post_type', 'share_stats_api', 'activity_id', 'activity_api'])
    final_df.ExternalLikes.fillna(0, inplace=True)
    final_df.ExternalComments.fillna(0, inplace=True)
    final_df['ExternalLikes'] = pd.Series(["{0:.2f}%".format(val * 100) for val in final_df['ExternalLikes']],
                                          index=final_df.index)
    final_df['ExternalComments'] = pd.Series(["{0:.2f}%".format(val * 100) for val in final_df['ExternalComments']],
                                             index=final_df.index)
    return final_df


if __name__ == '__main__':

    # ##################################################UGC data########################################################
    try:
        ugc_data_df = ugc_data()
        share_statistics_df = share_statistics(ugc_data_df)
        stats_df = pd.merge(ugc_data_df, share_statistics_df, on=['id'])
        ugc_posts_shares_df = activity_data(stats_df)

        like_actors = get_like_actors(stats_df)
        like_profiles = get_profile(like_actors)

        like_actors_profiles_df = pd.merge(like_actors, like_profiles, how='left', left_on='actor_id', right_on='id')
        like_actors_profiles_df['company'] = like_actors_profiles_df.apply(current_company, axis=1)
        likes_df = actors_profile_transformations(like_actors_profiles_df)

        comment_actors = get_comment_actors(stats_df)
        comment_profiles = get_profile(comment_actors)

        comm_actors_profile_df = pd.merge(comment_actors, comment_profiles, how='left', left_on='actor_id',
                                          right_on='id')
        comm_actors_profile_df['company'] = comm_actors_profile_df.apply(current_company, axis=1)
        comments_df = actors_profile_transformations(comm_actors_profile_df)

        external_activity = pd.merge(likes_df, comments_df, on=['object_ref'], how='outer',
                                     suffixes=('_likes', '_comments'))
        external_activity_df = external_activity_transformations(external_activity)
        ext_df = pd.merge(ugc_posts_shares_df, external_activity_df, left_on='activity_id', right_on='object_ref',
                          how='left')

        ugc_share_statistics_df = final_df(ext_df)

        writer = pd.ExcelWriter(get_filepath('ugc_share_statistics_df.xlsx'), engine='xlsxwriter')
        ugc_share_statistics_df.to_excel(writer, sheet_name='ugc_share_statistics')
        writer.save()
    except Exception as e:
        print(e)
    else:
        log = "Successfully written output to: " + get_filepath('ugc_share_statistics_df.xlsx')
        print(log)

    # ##############################################Likes and comments Profiles#####################################
    # try:
    #     # Get subset of ugc_posts_shares_df
    #     ugc = ugc_posts_shares_df[['id', 'activity_id']]
    #
    #     # Join UGC and like_actors
    #     ugc_likes = pd.merge(ugc, like_actors, how='left', left_on='activity_id', right_on='object_ref')
    #
    #     # Join like_actors and like_actors_details
    #     ugc_likes_data = pd.merge(ugc_likes, like_profiles, how='left', left_on='actor_id', right_on='id')
    #     ugc_likes_data['action_type'] = "Like"
    #
    #     # Final UGC_likes dataframe
    #     ugc_likes_stats = ugc_likes_data.drop(columns=['actor', '$URN', 'object_ref', 'actors_info'])
    #
    #     # -----------------------------------------------------------------------------------------------------------
    #
    #     # Join UGC and comment_actors
    #     ugc_comments = pd.merge(ugc, comment_actors, how='left', left_on='activity_id', right_on='object_ref')
    #
    #     # Join comment_actors and like_actors_details
    #     ugc_comments_data = pd.merge(ugc_comments, comment_profiles, how='left', left_on='actor_id', right_on='id')
    #     ugc_comments_data['action_type'] = "Comment"
    #
    #     # Final UGC_comments dataframe
    #     ugc_comments_stats = ugc_comments_data.drop(columns=['actor', '$URN', 'object_ref', 'actors_info'])
    #
    #     profiles_writer = pd.ExcelWriter(get_filepath('ugc_profiles.xlsx'), engine='xlsxwriter')
    #
    #     # write each dataframe to individual sheets
    #     ugc_likes_stats.to_excel(profiles_writer, sheet_name='ugc_likes_stats')
    #     ugc_comments_stats.to_excel(profiles_writer, sheet_name='ugc_comments_stats')
    #     profiles_writer.save()
    # except Exception as e:
    #     print(e)
    # else:
    #     log = "Successfully written output to: " + get_filepath('ugc_profiles.xlsx')
    #     print(log)
