from time import time
import json
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from requests import get
import re
import csv


result = []
count = 0
ENCS = ('utf-8', 'windows-1251', 'windows-1252')
MAX_RETRIES = 10

def finish_checking(item):
    global count
    result.append(item)
    count += 1
    print(f'Total parsed: {count:03d}/{len(orgs)}', end='\r')


async def get_one(org, session):
    item = {col: org[col] for col in ('name', 'full_name', 'inn', 'ogrn', 'origin_address', 
                                      'phone', 'email', 'region_id', 'site_url')}
    url = org['site_url']
    del org
    try:
        async with session.get(url) as response:
            page_content = await response.read()
            default_enc = response.headers.get('Content-Type', '').split('=')
            default_enc = default_enc[1] if len(default_enc) > 1 else ''
            for enc in ENCS + (default_enc, ):
                try:
                    page_content = page_content.decode(enc)
                    break
                except (UnicodeDecodeError, LookupError):
                    continue
            else:
                item['is_site_working'] = response.status == 200 and len(page_content) > 300
                finish_checking(item)
                return
            is_working = (bool(response) and response.status == 200 and len(page_content) > 200 and 
                          'страница не найдена' not in page_content.lower())
            item['is_site_working'] = is_working
            for key, value in zip(('site_title', 'site_description', 'site_keywords', 'social_links'), 
                                  get_item(page_content, url, is_working)):
                item[key] = value
            item['is_site_belonging_to_organization'] = (item['is_site_working'] and 
                                                    len(url.replace('http://', '').replace('https://', '').split('/')) < 4)
    except (aiohttp.client_exceptions.ClientConnectorError, aiohttp.client_exceptions.ServerDisconnectedError, 
            aiohttp.client_exceptions.InvalidURL, aiohttp.client_exceptions.ClientPayloadError, asyncio.TimeoutError):
        item['is_site_working'] = False
    finish_checking(item)


async def bound_fetch(sm, org, session):
    for _ in range(MAX_RETRIES):
        try:
            async with sm:
                await get_one(org, session)
                break
        except aiohttp.client_exceptions.ClientOSError:
            continue
    else:
        print(f'Cannot connect to {org["url"]}: out of retries. Skipping...')


async def run(orgs):
    tasks = []
    sm = asyncio.Semaphore(50)
    async with aiohttp.ClientSession() as session:
        for org in orgs:
            url = org['site_url']
            org['site_url'] = url if not url or url.startswith('http') else 'http://' + url
            task = asyncio.ensure_future(bound_fetch(sm, org, session))
            tasks.append(task)
        await asyncio.gather(*tasks)


def get_content(tag):
    return tag.content if tag and tag.content else ''


def get_item(page_content, url, is_working):
    title, description, keywords, links = [None] * 4
    if is_working:
        page = BeautifulSoup(page_content, 'html.parser')
        title = page.title.string.strip() if page.title and page.title.string else None
        description = get_content(page.find('meta', {'name': 'description'})).strip()
        keywords = get_content(page.find('meta', {'name': 'keywords'})).strip()
        links_list = {}
        for link in page.find_all('a'):
            if link.get('href'):
                h = link.get('href')
                link_type = None
                if 'vk.com' in h:
                    link_type = 'vk'
                elif 'facebook.com' in h:
                    link_type = 'fb'
                elif 'ok.ru' in h:
                    link_type = 'ok'
                elif 'instagram.com' in h:
                    link_type = 'ig'
                if link_type:
                    match = re.search('(?P<url>https?://[^\s]+)', h)
                    if match:
                        h = match.group('url')
                        links_list[link_type] = h
        links = json.dumps(links_list)
    return title, description, keywords, links


def save_to_csv(iterator, file_name, title, delimiter=','):
    """":param iterator: iterator over organisations.
    :param file_name: name of file where table will be saved.
    :param delimiter: delemiter for CSV table.
    :param title: table title, first row."""
    with open(file_name, 'w', newline='', encoding='utf8') as csv_file:
        writer = csv.DictWriter(csv_file, title, delimiter=delimiter, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        for row in iterator:
            writer.writerow(row)


if __name__ == '__main__':
    start_time = time()
    orientation = 'orientation=3,6&'
    print('Loading organizations list...')
    orgs = get(
        f'http://dop.edu.ru/organization/list?{orientation}institution_type=188&status=1&page=1&perPage=2000'
    ).json()['data']['list']
    print(f'Succesfully loaded {len(orgs)} objects')
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(orgs))
    loop.run_until_complete(future)
    save_to_csv(result, 'result.csv', 
        ('name', 'full_name', 'inn', 'ogrn', 'origin_address', 'phone', 'email', 
         'region_id', 'site_url', 'is_site_working', 'is_site_belonging_to_organization',
         'site_title', 'site_description', 'site_keywords', 'social_links')
    )
    elapsed_time = int(time() - start_time)
    print(f'Elapsed time: {elapsed_time // 3600:02d}:{elapsed_time % 3600 // 60:02d}:{elapsed_time % 60:02d}')
    print('Results saved to result.csv')
