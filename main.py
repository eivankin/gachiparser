import json
from requests import get
import csv
from bs4 import BeautifulSoup
import re
from tqdm import tqdm


def repair_encoding(s, default_enc):
    """:param s: string with unknow encoding.
    :param default_enc: auto-detected encoding.
    :returns new_s: string in with correct encoding."""
    if s and not re.match(r'[а-яА-Я0-9a-zA-Z]+', s):
        for enc in ('utf-8', 'windows-1251'):
            try:
                tmp = s.encode(default_enc).decode(enc)
            except UnicodeDecodeError:
                tmp = ''
            except UnicodeEncodeError:
                try:
                    tmp = s.encode(enc).decode(enc)
                except (UnicodeEncodeError, UnicodeDecodeError):
                    pass
            if re.match(r'[а-яА-Я0-9a-zA-Z]+', tmp):
                s = tmp
                break
        else:
            print(s.encode())
            raise ValueError(f'Correct encoding is not utf-8, windows-1251 and {default_enc}')
    return s


def get_content(tag):
    return tag.content if tag and tag.content else ''


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


def get_followers(url, link_type):
    """:param url: link to social media.
    :param link_type: code of social media.
    :returns followers_count"""
    url = url.replace('/vk.com', '/m.vk.com')
    page = BeautifulSoup(get(url).text, features='html.parser')
    count = ''
    if link_type == 'vk':
        count = page.find('em', {'class': 'pm_counter'})
    elif link_type == 'fb':
        count = page.find(
            'div', {'class': '_4-u2 _6590 _3xaf _4-u8'}).find_all(
                'div', {'class': '_2pi9 _2pi2'})[1].find(
                    'div', {'class': '_4bl9'}).string
    elif link_type == 'ok':
        count = page.find('span', {'id': 'groupMembersCntEl'}).string
    return ''.join(filter(str.isdigit, count))


def get_site_info(url):
    """:param url: organization site.
    :returns info: list of fields 'is_site_working', 
    'is_site_belonging_to_organization', 'site_title', 'site_description', 
    'site_keywords', 'social_links', 'followers'."""
    is_working, is_belonging, title, description, keywords, links, followers = [None] * 7
    if url:
        url = url if url.startswith('http') else 'http://' + url
        try:
            res = get(url)
        except Exception:
            res = None
        is_working = (bool(res) and res.status_code == 200 and len(res.content) > 200 and 
                      'страница не найдена' not in res.text.lower())
        if is_working:
            page = BeautifulSoup(res.text, features='html.parser')
            title = page.title.string.strip() if page.title.string else None
            description = get_content(page.find('meta', {'name': 'description'})).strip()
            keywords = get_content(page.find('meta', {'name': 'keywords'})).strip()
            is_belonging = len(url.replace('http://', '').replace('https://', '').split('/')) < 4
            if res.encoding.lower() not in ('utf-8', 'windows-1251'):
                title, description, keywords = map(
                lambda x: repair_encoding(x, res.encoding), [title, description, keywords]
            )
            links_list, followers_list = {}, {}
            contact_link = ''
            for link in page.find_all('a'):
                if link.href:
                    if 'contacts' in link.href.lower() or 'контакты' in str(link.string).lower():
                        contact_link = link.href
                    link_type = get_type(link.href)
                    if link_type:
                        links_list[link_type] = link.href
                        if link_type != 'ig':
                            followers_list[link_type] = get_followers(link.href, link_type)
            if not links_list and contact_link:
                for link in BeautifulSoup(get(contact_link).text, features='html.parser').find_all('a'):
                    if link.href:
                        link_type = get_type(link.href)
                        if link_type:
                            links_list[link_type] = link.href
                            if link_type != 'ig':
                                followers_list[link_type] = get_followers(link.href, link_type)
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
    return tqdm(map(
        lambda x: (x['name'], x['full_name'], x['inn'], 
                   x['ogrn'], x['origin_address'],
                   x['phone'], x['email'],
                   x['region_id'], x['site_url'], 
                   *get_site_info(x['site_url'])), 
        result['data']['list']
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
    orientation_codes = '3,6'  # "Техническая" and "Естественнонаучная"
    save_to_csv(
        get_organisations(None, orientation_codes), 'output.csv', 
        ('name', 'full_name', 'inn', 'ogrn', 'adress', 'phone', 'email', 
         'region_id', 'site_url', 'is_site_working', 'is_site_belonging_to_organization',
         'site_title', 'site_description', 'site_keywords', 'social_links', 'followers')
    )
