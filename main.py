from time import time
import json
import asyncio
import aiohttp
import bs4
from requests import get
import re
import csv


result = []
count = 0
ENCS = ('utf-8', 'windows-1251', 'windows-1252')
MAX_RETRIES = 10

def finish_checking(item: dict) -> None:
    """:param item: dictionary with organisation info, will be row of table."""
    global count
    result.append(item)
    count += 1
    print(f'Total parsed: {count:03d}/{len(orgs)}', end='\r')


def repair_url(url: str) -> list:
    """:param url: url to repair.
    :returns possible_urls: list of possible urls, can be empty."""
    possible_urls = []
    url = url.replace('http//', 'http://')
    if url:
        if len(url.split('.')) < 2:
            url += '.ucoz.ru'
        match = re.search('(?P<url>https?://[^\s]+)', url)
        if match:
            g = match.group('url')
            possible_urls += [g, g.replace('https', 'http') if g.startswith('https') else g.replace('http', 'https')]
        else:
            possible_urls += ['http://' + url, 'https://' + url]
    return possible_urls


async def get_one(org: dict, session: aiohttp.ClientSession) -> None:
    """:param org: organization dictionary from JSON response of dop.edu.ru."""
    item = {col: org[col] for col in ('name', 'full_name', 'inn', 'ogrn', 'origin_address', 
                                      'phone', 'email', 'region_id', 'site_url')}
    site_url = org['site_url']
    del org
    for url in repair_url(site_url):
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
                                      get_item(page_content, is_working)):
                    item[key] = value
                item['is_site_belonging_to_organization'] = (item['is_site_working'] and 
                                                        len(url.replace('http://', '').replace('https://', '').split('/')) < 4)
                break
        except (aiohttp.client_exceptions.ClientConnectorError, aiohttp.client_exceptions.ServerDisconnectedError, 
                aiohttp.client_exceptions.InvalidURL, aiohttp.client_exceptions.ClientPayloadError, asyncio.TimeoutError):
            item['is_site_working'] = False
    finish_checking(item)


async def bound_fetch(sm: asyncio.Semaphore, org: dict, session: aiohttp.ClientSession) -> None:
    for _ in range(MAX_RETRIES):
        try:
            async with sm:
                await get_one(org, session)
                break
        except aiohttp.client_exceptions.ClientOSError:
            continue
    else:
        print(f'Cannot connect to {org["site_url"]}: out of retries. Skipping...')


async def run(orgs: list) -> None:
    tasks = []
    sm = asyncio.Semaphore(50)
    async with aiohttp.ClientSession() as session:
        for org in orgs:
            url = org['site_url']
            task = asyncio.ensure_future(bound_fetch(sm, org, session))
            tasks.append(task)
        await asyncio.gather(*tasks)


def get_content(tag: bs4.element.Tag) -> str:
    """:param tag: parsed tag object.
    :returns string: tag string if tag exists, else empty string."""
    return tag.content if tag and tag.content else ''


def get_item(page_content: str, is_working: bool) -> tuple:
    """:param page_content: page text for parsing.
    :param is_working: is site working.
    :returns title, description, keywords, links: Returns None if this parameter equals to False."""
    title, description, keywords, links = [None] * 4
    if is_working:
        page = bs4.BeautifulSoup(page_content, 'html.parser')
        title = page.title.string.strip() if page.title and page.title.string else None
        description = get_content(page.find('meta', {'name': 'description'})).strip()
        keywords = get_content(page.find('meta', {'name': 'keywords'})).strip()
        links_list = {}
        for link in page.find_all('a'):
            if link.get('href'):
                h = link.get('href')
                link_type = None
                if 'vk.com' in h and all(x not in h for x in ('away', 'album', 'feed', 'videos', 'photo', 'wall')):
                    link_type = 'vk'
                elif 'facebook.com' in h:
                    link_type = 'fb'
                elif '/ok.ru' in h or h.startswith('ok.ru'):
                    link_type = 'ok'
                elif 'instagram.com' in h:
                    link_type = 'ig'
                elif 'twitter.com' in h:
                    link_type = 'tw'
                if link_type:
                    match = re.search('(?P<url>https?://[^\s]+)', h)
                    if match:
                        h = match.group('url')
                        links_list[link_type] = h
        links = json.dumps(links_list)
    return title, description, keywords, links


def save_to_csv(rows: list, file_name: str, title, delimiter=',') -> None:
    """":param rows: iterator over organisations.
    :param file_name: name of file where table will be saved.
    :param delimiter: delemiter for CSV table.
    :param title: table title, first row."""
    with open(file_name, 'w', newline='', encoding='utf8') as csv_file:
        writer = csv.DictWriter(csv_file, title, delimiter=delimiter, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


if __name__ == '__main__':
    start_time = time()
    orientation = 'orientation=3,6&'  # "Техническая" и "Естественнонаучная"
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
    print(f'Elapsed time: {elapsed_time // 3600}:{elapsed_time % 3600 // 60:02d}:{elapsed_time % 60:02d}')
    print('Results saved to result.csv')
