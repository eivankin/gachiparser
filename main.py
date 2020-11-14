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
        f'http://dop.edu.ru/organization/list?{region}{orientation}&page=1&perPage={count}'
    ).content.decode())
    return map(lambda x: (x['name'], x['full_name'], x['site_url']), result['data']['list'])


def save_to_csv(iterator, file_name, delimiter=',', title=('name', 'full_name', 'site_url')):
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
    save_to_csv(get_organisations(42, 3), 'output.csv')
