import json
from requests import get
import csv
from bs4 import BeautifulSoup
import re
from tqdm import tqdm
from urllib.parse import urljoin
import aiohttp
import asyncio


ENCS = ('utf-8', 'windows-1251', 'windows-1252')


def repair_encoding(s, default_enc):
    """:param s: string with unknow encoding.
    :param default_enc: auto-detected encoding.
    :returns new_s: string in with correct encoding."""
    if s and not re.match(r'[а-яА-Я0-9a-zA-Z]+', s):
        for enc in ENCS:
            try:
                tmp = s.encode(default_enc).decode(enc)
            except UnicodeDecodeError:
                tmp = ''
            except UnicodeEncodeError:
                for enc_2 in ENCS:
                    try:
                        tmp = s.encode().decode().encode(enc_2).decode(enc)
                        break
                    except (UnicodeEncodeError, UnicodeDecodeError):
                        pass
                else:
                    tmp = ''
            match = re.match(r'[а-яА-Я0-9a-zA-Z \"]+', tmp)
            if match and len(match.group(0)) > 3:
                s = tmp
                break
        else:
            return ''
    return s


def get_content(tag):
    return tag.content if tag and tag.content else ''


def get_links(page, find_contacts=True):
    links_list, followers_list, contact_link = {}, {}, ''
    for link in page.find_all('a'):
        if link.get('href'):
            h = link.get('href')
            if find_contacts and ('contacts' in h.lower() or 'контакты' in str(link.string).lower()):
                contact_link = h
            link_type = get_type(h)
            if link_type:
                match = re.search('(?P<url>https?://[^\s]+)', h)
                if match:
                    h = match.group('url')
                    links_list[link_type] = h
                    if link_type != 'ig':
                        followers_list[link_type] = loop.run_until_complete(get_followers(h, link_type))
    return links_list, followers_list, contact_link


def get_type(url):
    """:param url: url to check.
    :returns type: type of link, code of social media or None."""
    if 'vk.com' in url:
        return 'vk'
    if 'facebook.com' in url:
        return 'fb'
    if 'ok.ru' in url:
        return 'ok'
    if 'instagram.com' in url:
        return 'ig'
    return None


async def get_followers(url, link_type):
    """:param url: link to social media.
    :param link_type: code of social media.
    :returns followers_count"""
    
    url = url.replace('/vk.com', '/m.vk.com')
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as res:
            page = BeautifulSoup(await res.text(), features='html.parser')
            count = ''
            if link_type == 'vk':
                count = page.find('em', {'class': 'pm_counter'})
            elif link_type == 'fb':
                try:
                    count = page.find(
                        'div', {'class': '_4-u2 _6590 _3xaf _4-u8'}).find_all(
                            'div', {'class': '_2pi9 _2pi2'})[1].find(
                                'div', {'class': '_4bl9'})
                except (AttributeError, IndexError):
                    pass
            elif link_type == 'ok':
                count = page.find('span', {'id': 'groupMembersCntEl'})
            return ''.join(filter(str.isdigit, count.string)) if count and count.string else None


async def get_site_info(url):
    """:param url: organization site.
    :returns info: list of fields 'is_site_working', 
    'is_site_belonging_to_organization', 'site_title', 'site_description', 
    'site_keywords', 'social_links', 'followers'."""
    is_working, is_belonging, title, description, keywords, links, followers = [None] * 7
    if url:
        url = url if url.startswith('http') else 'http://' + url
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as res:
                text = await res.text()
                is_working = (bool(res) and res.status == 200 and len(text) > 200 and 
                              'страница не найдена' not in text.lower())
                if is_working:
                    page = BeautifulSoup(await res.text(), features='html.parser')
                    title = page.title.string.strip() if page.title and page.title.string else None
                    description = get_content(page.find('meta', {'name': 'description'})).strip()
                    keywords = get_content(page.find('meta', {'name': 'keywords'})).strip()
                    is_belonging = len(url.replace('http://', '').replace('https://', '').split('/')) < 4
                    # if res.encoding.lower() not in ('utf-8', 'windows-1251'):
                    #    title, description, keywords = map(
                    #    lambda x: repair_encoding(x, res.encoding), [title, description, keywords]
                    # )
                    links_list, followers_list, contact_link = {}, {}, ''  # get_links(page)
                    # if not links_list and contact_link:
                    #    contact_link = urljoin(url, contact_link)
                    #    async with session.get(contact_link) as res_2:
                    #        p = BeautifulSoup(await res_2.text(), features='html.parser')
                    #        links_list, followers_list, contact_link = get_links(p, False)
                    links = json.dumps(links_list)
                    followers = json.dumps(followers_list)
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
    result = get(
        f'http://dop.edu.ru/organization/list?{region}{orientation}institution_type=188&status=1&page=1&perPage={count}'
    ).json()
    print('List of organizations downloaded')
    done, pending = loop.run_until_complete(asyncio.wait([asyncio.ensure_future(get_site_info(x['site_url'])) 
                                                          for x in result['data']['list']]))
    return tqdm(map(
        lambda x: (x[0]['name'], x[0]['full_name'], x[0]['inn'], 
                   x[0]['ogrn'], x[0]['origin_address'],
                   x[0]['phone'], x[0]['email'],
                   x[0]['region_id'], x[0]['site_url'], 
                   *x[1].result()), 
        zip(result['data']['list'], done)
    ), total=int(result['data']['count']))


def save_to_csv(iterator, file_name, title, delimiter=','):
    """":param iterator: iterator over organisations.
    :param file_name: name of file where table will be saved.
    :param delimiter: delemiter for CSV table.
    :param title: table title, first row."""
    with open(file_name, 'w', newline='', encoding='utf8') as csv_file:
        writer = csv.writer(csv_file, delimiter=delimiter, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(title)
        for row in iterator:
            writer.writerow(row)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    orientation_codes = '3,6'  # "Техническая" and "Естественнонаучная"
    save_to_csv(
        get_organisations(None, orientation_codes), 'output.csv', 
        ('name', 'full_name', 'inn', 'ogrn', 'adress', 'phone', 'email', 
         'region_id', 'site_url', 'is_site_working', 'is_site_belonging_to_organization',
         'site_title', 'site_description', 'site_keywords', 'social_links', 'followers')
    )
