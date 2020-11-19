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


async def get_one(url, session):
    global count
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
                result.append({'is_working': response.status == 200 and len(page_content) > 300})
                return
            item = get_item(
                page_content, url, (bool(response) and response.status == 200 and len(page_content) > 200 and 
                                    'страница не найдена' not in page_content.lower())
                )
            item['is_belonging_to_organization'] = (item['is_working'] and 
                                                    len(url.replace('http://', '').replace('https://', '').split('/')) < 4)
    except (aiohttp.client_exceptions.ClientConnectorError, aiohttp.client_exceptions.ServerDisconnectedError, 
            aiohttp.client_exceptions.InvalidURL, aiohttp.client_exceptions.ClientPayloadError):
        item = {'is_working': False}
    result.append(item)
    count += 1
    print(f'Total checked: {str(count).rjust(3, "0")}/{len(urls)}', end='\r')


async def bound_fetch(sm, url, session):
    while True:
        try:
            async with sm:
                await get_one(url, session)
                break
        except aiohttp.client_exceptions.ClientOSError:
            continue


async def run(urls):
    tasks = []
    sm = asyncio.Semaphore(50)
    async with aiohttp.ClientSession() as session:
        for url in urls:
            url = url if url.startswith('http') else 'http://' + url
            
            task = asyncio.ensure_future(bound_fetch(sm, url, session))
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
    return {'is_working': is_working,
            'title': title,
            'description': description,
            'keywords': keywords,
            'links': links}


if __name__ == '__main__':
    start_time = time()
    orientation = 'orientation=3,6&'
    urls = list(filter(lambda x: x, map(lambda y: y['site_url'], get(
        f'http://dop.edu.ru/organization/list?{orientation}institution_type=188&status=1&page=1&perPage=2000'
    ).json()['data']['list'])))
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(urls))
    loop.run_until_complete(future)
    elapsed_time = time() - start_time
    with open('result.csv', 'w') as f:
        f.write(json.dumps(result))
    print(f'Elapsed time: {int(elapsed_time // 3600)}:{int(elapsed_time // 60)}:{elapsed_time % 60:.2f}')
