import json
import os


def read_configuration_file(filename="config.json"):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    filepath = os.path.join(dir_path, filename)
    with open(filepath) as f:
        config = json.load(f)
    return config


def read_creds(filename="credentials.json"):
    """
    Store API credentials in a safe place.
    If you use Git, make sure to add the file to .gitignore
    """
    credentials = read_configuration_file(filename)
    return credentials


def headers1(access_token):
    """
    Make the headers to attach to the API call.
    """
    headers_v1 = {
        'Authorization': f'Bearer {access_token}'
    }
    return headers_v1


def headers2(access_token):
    """
    Make the headers with v2 protocol to attach to the API call.
    """
    headers_v2 = {
        'Authorization': f'Bearer {access_token}',
        'X-Restli-Protocol-Version': '2.0.0'
    }
    return headers_v2
