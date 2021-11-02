import json
import requests
import pandas as pd
from common.auth import read_creds, headers1
from common.common_functions import *

# Need to import credentials
creds = read_creds()
access_token = creds['access_token']
headers_v1 = headers1(access_token)

# Import organization code for API
params = read_configuration_file()
organization_code = params['organization_code']


def follower_statistics():
    response = requests.get(
        'https://api.linkedin.com/v2/organizationalEntityFollowerStatistics?q=organizationalEntity&organizationalEntity=urn:li:organization:'+organization_code,
        headers=headers_v1)
    follower_stats = json.loads(response.text)

    return follower_stats


def followersby_country(follower_stats):
    country_detail = follower_stats['elements'][0]['followerCountsByCountry']

    response_head = requests.get('https://api.linkedin.com/v2/countries', headers=headers_v1)
    country = json.loads(response_head.text)

    country_id = [country['elements'][i]['$URN'] for i in range(len(country['elements']))]
    country_value = [country['elements'][i]['name']['value'] for i in range(len(country['elements']))]

    country_detail_id = [country_detail[i]['country'] for i in range(len(country_detail))]
    organicFollowerCount = [country_detail[i]['followerCounts']['organicFollowerCount'] for i in
                            range(len(country_detail))]
    paidFollowerCount = [country_detail[i]['followerCounts']['paidFollowerCount'] for i in range(len(country_detail))]

    dic1 = {'country_id': country_id,
            'country': country_value}

    dic2 = {'country_id': country_detail_id,
            'organicFollowerCount': organicFollowerCount,
            'paidFollowerCount': paidFollowerCount}

    country_value_df = pd.DataFrame(dic1)

    country_details_df = pd.DataFrame(dic2)

    country_df = pd.merge(country_value_df, country_details_df, on='country_id')

    return country_df


def followersby_region(follower_stats):
    region_detail = follower_stats['elements'][0]['followerCountsByRegion']

    response_head = requests.get('https://api.linkedin.com/v2/regions', headers=headers_v1)
    region = json.loads(response_head.text)

    region_id = [region['elements'][i]['$URN'] for i in range(len(region['elements']))]
    region_value = [region['elements'][i]['name']['value'] for i in range(len(region['elements']))]

    region_detail_id = [region_detail[i]['region'] for i in range(len(region_detail))]
    organicFollowerCount = [region_detail[i]['followerCounts']['organicFollowerCount'] for i in
                            range(len(region_detail))]
    paidFollowerCount = [region_detail[i]['followerCounts']['paidFollowerCount'] for i in range(len(region_detail))]

    dic1 = {'region_id': region_id,
            'region': region_value}

    dic2 = {'region_id': region_detail_id,
            'organicFollowerCount': organicFollowerCount,
            'paidFollowerCount': paidFollowerCount}

    region_value_df = pd.DataFrame(dic1)

    region_details_df = pd.DataFrame(dic2)

    region_df = pd.merge(region_value_df, region_details_df, on='region_id')

    return region_df


def followersby_seniority(follower_stats):
    seniority_detail = follower_stats['elements'][0]['followerCountsBySeniority']

    response_head = requests.get('https://api.linkedin.com/v2/seniorities', headers=headers_v1)
    seniority = json.loads(response_head.text)

    seniority_id = [seniority['elements'][i]['$URN'] for i in range(len(seniority['elements']))]
    seniority_value = [seniority['elements'][i]['name']['localized']['en_US'] for i in
                       range(len(seniority['elements']))]

    seniority_detail_id = [seniority_detail[i]['seniority'] for i in range(len(seniority_detail))]
    organicFollowerCount = [seniority_detail[i]['followerCounts']['organicFollowerCount'] for i in
                            range(len(seniority_detail))]
    paidFollowerCount = [seniority_detail[i]['followerCounts']['paidFollowerCount'] for i in
                         range(len(seniority_detail))]

    dic1 = {'seniority_id': seniority_id,
            'seniority': seniority_value}

    dic2 = {'seniority_id': seniority_detail_id,
            'organicFollowerCount': organicFollowerCount,
            'paidFollowerCount': paidFollowerCount}

    seniority_value_df = pd.DataFrame(dic1)

    seniority_details_df = pd.DataFrame(dic2)

    seniority_df = pd.merge(seniority_value_df, seniority_details_df, on='seniority_id')

    return seniority_df


def followersby_industry(follower_stats):
    industry_detail = follower_stats['elements'][0]['followerCountsByIndustry']

    response_head = requests.get('https://api.linkedin.com/v2/industries', headers=headers_v1)
    industry = json.loads(response_head.text)

    industry_id = [industry['elements'][i]['$URN'] for i in range(len(industry['elements']))]
    industry_value = [industry['elements'][i]['name']['localized']['en_US'] for i in range(len(industry['elements']))]

    industry_detail_id = [industry_detail[i]['industry'] for i in range(len(industry_detail))]
    organicFollowerCount = [industry_detail[i]['followerCounts']['organicFollowerCount'] for i in
                            range(len(industry_detail))]
    paidFollowerCount = [industry_detail[i]['followerCounts']['paidFollowerCount'] for i in range(len(industry_detail))]

    dic1 = {'industry_id': industry_id,
            'industry': industry_value}

    dic2 = {'industry_id': industry_detail_id,
            'organicFollowerCount': organicFollowerCount,
            'paidFollowerCount': paidFollowerCount}

    industry_value_df = pd.DataFrame(dic1)

    industry_details_df = pd.DataFrame(dic2)

    industry_df = pd.merge(industry_value_df, industry_details_df, on='industry_id')

    return industry_df


def followersby_function(follower_stats):
    function_detail = follower_stats['elements'][0]['followerCountsByFunction']

    response_head = requests.get('https://api.linkedin.com/v2/functions', headers=headers_v1)
    function = json.loads(response_head.text)

    function_id = [function['elements'][i]['$URN'] for i in range(len(function['elements']))]
    function_value = [function['elements'][i]['name']['localized']['en_US'] for i in range(len(function['elements']))]

    function_detail_id = [function_detail[i]['function'] for i in range(len(function_detail))]
    organicFollowerCount = [function_detail[i]['followerCounts']['organicFollowerCount'] for i in
                            range(len(function_detail))]
    paidFollowerCount = [function_detail[i]['followerCounts']['paidFollowerCount'] for i in range(len(function_detail))]

    dic1 = {'function_id': function_id,
            'function': function_value}

    dic2 = {'function_id': function_detail_id,
            'organicFollowerCount': organicFollowerCount,
            'paidFollowerCount': paidFollowerCount}

    function_value_df = pd.DataFrame(dic1)

    function_details_df = pd.DataFrame(dic2)

    function_df = pd.merge(function_value_df, function_details_df, on='function_id')

    return function_df


def followersby_staff_count(follower_stats):
    staff_count_detail = follower_stats['elements'][0]['followerCountsByStaffCountRange']

    staff_count_detail_id = [staff_count_detail[i]['staffCountRange'] for i in range(len(staff_count_detail))]
    organicFollowerCount = [staff_count_detail[i]['followerCounts']['organicFollowerCount'] for i in
                            range(len(staff_count_detail))]
    paidFollowerCount = [staff_count_detail[i]['followerCounts']['paidFollowerCount'] for i in
                         range(len(staff_count_detail))]

    dic = {'staff_count_id': staff_count_detail_id,
           'organicFollowerCount': organicFollowerCount,
           'paidFollowerCount': paidFollowerCount}

    staff_count_df = pd.DataFrame(dic)

    return staff_count_df


if __name__ == '__main__':

    try:
        # call main function to get all the followers data
        follower_stats = follower_statistics()

        # Followers by country
        followers_country = followersby_country(follower_stats)

        # Followers by region
        followers_region = followersby_region(follower_stats)

        # Followers by seniority
        followers_seniority = followersby_seniority(follower_stats)

        # Followers by industry
        followers_industry = followersby_industry(follower_stats)

        # Followers by function
        followers_function = followersby_function(follower_stats)

        # Followers by staff count range
        followers_staff_count = followersby_staff_count(follower_stats)

        # Create a Pandas Excel writer using XlsxWriter as the engine.
        writer = pd.ExcelWriter(get_filepath('followers_by.xlsx'), engine='xlsxwriter')

        # Write each dataframe to a different worksheet.
        followers_country.to_excel(writer, sheet_name='followers_country')
        followers_region.to_excel(writer, sheet_name='followers_region')
        followers_seniority.to_excel(writer, sheet_name='followers_seniority')
        followers_industry.to_excel(writer, sheet_name='followers_industry')
        followers_function.to_excel(writer, sheet_name='followers_function')
        followers_staff_count.to_excel(writer, sheet_name='followers_staff_count')

        # Close the Pandas Excel writer and output the Excel file.
        writer.save()
    except Exception as e:
        print(e)
    else:
        log = "Successfully written output to: " + get_filepath('followers_by.xlsx')
        print(log)
        # logOutput(get_filepath('logging_info.txt'), log)
