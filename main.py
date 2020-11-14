import json
from requests import get
import csv


def get_organisations(region, orientation):
    """This function sending request to dop.edu.ru API.
    :param region: region index in internal API. Can be None.
    :param orientation: orientation index in internal API. Can be None.
    :returns organisations: iterator over organisations, where each element is tuple of name, full name and site url."""
    region = f'region={region}' if region else ''
    orientation = f'&orientation={orientation}' if orientation else ''
    count = 1000
    # uncomment for count detection
    # count = json.loads(get(
    #    f'http://dop.edu.ru/organization/list?{region}{orientation}&page=1&perPage=1'
    # ).content.decode())['data']['count']
    result = json.loads(get(
        f'http://dop.edu.ru/organization/list{region}{orientation}&page=1&perPage={count}'
    ).content.decode())
    return map(lambda x: (x['name'], x['full_name'], x['site_url']), result['data']['list'])


def save_to_csv(iterator, file_name, delimiter=','):
    pass


if __name__ == '__main__':
    print(*get_organisations(42, 3))