import json
from requests import get
import csv
from bs4 import BeautifulSoup


def get_site_info(url):
    """:param url: organization site.
    :returns info: list of fields 'is_site_working', 
    'is_site_belonging_to_organization', 'site_title', 'site_description', 
    'site_keywords', 'social_links', 'followers'."""
    is_working, is_belonging, title, description, keywords, links, followers = [None] * 7
    if url:
        res = get(url)
        is_working = res.status_code == 200
        if is_working:
            page = BeautifulSoup(res.content.decode(), features="html.parser")
            title = page.title.string
            description = page.find('meta', {'name': 'description'}).content
            keywords = page.find('meta', {'name': 'keywords'}).content
            is_belonging = len(url.replace('http://', '').replace('https://', '').split('/')) < 4
    return is_working, is_belonging, title, description, keywords, links, followers


def get_organisations(region, orientation):
    """This function sending request to dop.edu.ru API.
    :param region: region index in internal API. Can be int, None or str.
    :param orientation: orientation index in internal API. Can be int, None or str.
    :returns organisations: iterator over organisations, 
    where each element is tuple of name, full name and site url."""
    region = f'region={region}&' if region else ''
    orientation = f'orientation={orientation}&' if orientation else ''
    count = 2000
    # uncomment for count detection
    # count = json.loads(get(
    #    f'http://dop.edu.ru/organization/list?{region}{orientation}page=1&perPage=1'
    # ).content.decode())['data']['count']
    result = json.loads(get(
        f'http://dop.edu.ru/organization/list?{region}{orientation}page=1&perPage={count}'
    ).content.decode())
    return map(
        lambda x: (x['name'], x['full_name'], x['region_id'], x['site_url'], 
                   *get_site_info(x['site_url'])), 
        result['data']['list']
    )


def save_to_csv(iterator, file_name, title, delimiter=','):
    """":param iterator: iterator over organisations.
    :param file_name: name of file where table will be saved.
    :param delimiter: delemiter for CSV table.
    :param title: table title, first row."""
    with open(file_name, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file, delimiter=delimiter, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(title)
        for row in iterator:
            writer.writerow(row)


if __name__ == '__main__':
    orientation_codes = '3,6'  # "Техническая" and "Естественнонаучная"
    save_to_csv(
        get_organisations(None, orientation_codes), 'output.csv', 
        ('name', 'full_name', 'region_id', 'site_url', 'is_site_working', 
         'is_site_belonging_to_organization', 'site_title', 'site_description', 
         'site_keywords', 'social_links', 'followers')
    )
