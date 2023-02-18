import configparser
import json
#from pysondb import db
import logging
import os, sys
import requests
import datetime
import time
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from html import unescape
import threading

logging.basicConfig(level=logging.INFO, filename="parser.log", filemode="a",
                    format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))


class Api:
    url = 'https://rupep.org'
    headers = {}
    # img_storage = 'pages/images/'
    # remote_img_storage = 'images/'
    proxies = []
    archive_date_start = None
    archive_date_end = None
    _proxy_iter = 0
    archive_links = []
    total = 0
    current = 0
    dbfile = ''
    database = None
    trdict = {
        "а": "a", "А": "A",
        "б": "b", "Б": "B",
        "в": "v", "В": "V",
        "г": "g", "Г": "G",
        "д": "d", "Д": "D",
        "е": "e", "Е": "E",
        "ё": "jo", "Ё": "Jo",
        "ж": "zh", "Ж": "Zh",
        "з": "z", "З": "Z",
        "и": "i", "И": "I",
        "й": "y", "Й": "Y",
        "к": "k", "К": "K",
        "л": "l", "Л": "L",
        "м": "m", "М": "M",
        "н": "n", "Н": "N",
        "о": "o", "О": "O",
        "п": "p", "П": "P",
        "р": "r", "Р": "R",
        "с": "s", "С": "S",
        "т": "t", "Т": "T",
        "у": "u", "У": "U",
        "ф": "f", "Ф": "F",
        "х": "kh", "Х": "Kh",
        "ц": "c", "Ц": "C",
        "ч": "ch", "Ч": "Ch",
        "ш": "sh", "Ш": "Sh",
        "щ": "sch", "Щ": "Sсh",
        "ъ": "", "Ъ": "",
        "ы": "y", "Ы": "Y",
        "ь": "", "Ь": "",
        "э": "eh", "Э": "Eh",
        "ю": "ju", "Ю": "Ju",
        "я": "ja", "Я": "Ja"
    }

    def transliterate(self, in_data):
        buf = ''
        for ch in in_data:
            try:
                chh = self.trdict[ch]
            except:
                chh = ch
            buf = buf + chh
        return buf

    def __init__(self, dbfile='db.json'):
        self.dbfile = dbfile
        #self.database = db.getDb(self.dbfile)

    def _get(self, url, use_proxy=False):
        if use_proxy:
            r = requests.get(url=url, headers=self.headers, proxies=self.proxies[self._proxy_iter])
            if (len(self.proxies) - 1) != self._proxy_iter:
                self._proxy_iter += 1
            else:
                self._proxy_iter = 0
        else:
            r = requests.get(url=url, headers=self.headers)
        return r

    def get_main_data(self, use_proxy=False):
        items = []
        none_str = None
        path = self.url + '/ru/persons_list/'
        r = self._get(path, use_proxy=use_proxy)
        html = r.content.decode('utf-8')
        soup = BeautifulSoup(html, features="html.parser")
        persons_soup = soup.find('table', {'class': 'everything'}).find('tbody').find_all('tr')
        for person_soup in persons_soup:
            tds = person_soup.find_all('td')
            fio_td = tds[0]
            birthday = tds[1].text.strip()
            if birthday == '': birthday = none_str
            inn = tds[2].text.strip()
            if inn == '': inn = none_str
            category = tds[3].text.strip()
            if category == '': category = none_str
            last_job_ru = none_str
            last_job_en = none_str
            last_job = tds[4].text.strip().replace('\t', '').split('\n')
            if last_job == ['']:
                last_job = none_str
            else:
                try:
                    buf = []
                    for i in last_job:
                        bb = i.strip()
                        if bb != '': buf.append(bb)
                    last_job = buf
                    last_job_ru = last_job[0]
                    last_job_en = last_job[1].strip('()')
                except:
                    if last_job_ru != none_str:
                        last_job_en = none_str
                    # logging.info(f'{str(last_job)}')
            fio_link = self.url + fio_td.find('a')['href']
            fio_ru = fio_td.find('a').extract().text
            try:
                fio_others = fio_td.find('small').extract().text.strip()
            except:
                fio_others = none_str
            fio_en = fio_td.text.strip().replace('(', '').replace(')', '')
            id = urlparse(fio_link).path.split('/')[-1:][0]
            item = {
                'person-id': id,
                'fio': {
                    'ru': fio_ru,
                    'en': fio_en,
                    'others': fio_others
                },
                'person-link': fio_link,
                'birthday': birthday,
                'inn': inn,
                'category': category,
                'last-job': {
                    'ru': last_job_ru,
                    'en': last_job_en
                }
            }

            items.append(item)
            logging.info(f'PARSED {id}: {fio_ru} / {fio_en} / {birthday}')
        f = open('persons.json', 'w', encoding='utf-8')
        f.write(json.dumps(items, ensure_ascii=False, indent=4))
        f.close()
        # self.database.addMany(items)
        logging.info(f'TOTAL {len(items)} persons')
        return items

    def parse_person(self, url, use_proxy=False):
        # path = self.url + '/articles/' + url
        path = url
        url = urlparse(path).path.split('/')[-1:][0]
        r = self._get(path, use_proxy=use_proxy)
        html = r.content.decode('utf-8')
        soup = BeautifulSoup(html, features="html.parser")
        profile = soup.find('section', {'id': 'profile'})
        id = urlparse(url).path.split('/')[-1:][0]
        fname = f'persons/{id}.json'
        person = {
            'person-id':id,
            'full-name': None,
            'avatar': None,
            'personal': {
                'category': None,
                'tags': [],
                'birthday': None,
                'taxID': None,
                'nationality': None,
                'lives': None,
                'realty-in': None,
                'sanctions': None,
                'last-job': None,
                'social-profiles': None,
                # 'social-profiles': [
                #     {
                #         'name':social media name,
                #         'link':social profile link,
                #         'archive-link':link to webarchive,
                #         'archive-title':custom format of date of webarchive snapshop
                #     },{},{}...
                # ]
                'sites': None
                # 'sites': [
                #     {
                #         'name':social media name,
                #         'link':social profile link,
                #         'archive-link':link to webarchive,
                #         'archive-title':custom format of date of webarchive snapshop
                #     },{},{}...
                # ]
            }
        }
        try:
            person['avatar'] = self.url + profile.find('div', {'class': 'avatar'}).find('img')['src']
        except:
            pass
        try:
            person['full-name'] = profile.find('header', {'class': 'profile-header'}).text.strip(' \n')
        except:
            pass

        # ----------- FIND BLOCKS --------------
        # -- Общая информация
        try: personal_trs = profile.find('div', {'id': 'personal'}).find('table').find_all('tr')
        except: personal_trs = None
        # -- Карьера
        try: workbefore = profile.find('div', {'id': 'workbefore'}).find('ul',{'class':'timeline'}).find_all('li',{'class':'tl-item'})
        except: workbefore = None
        # -- Связи
        try: connections = profile.find('div', {'id': 'connections'}).find('ul').find('li').find('ul').find_all('li', recursive=False)
        except: connections = None
        # -- Связанные юридические лица
        try: companies = profile.find('div', {'id': 'related-companies'}).find('ul',{'class':'timeline'}).find_all('li')
        except: companies = None
        # -- Уголовные производства и санкции
        try: reputation = profile.find('div', {'id': 'reputation'}).find('div', {'class': 'printWrap'})
        except: reputation = None
        # --------------------------------------

        # ---------- ОБРАБОТКА БЛОКОВ -------------
        if personal_trs is not None:
            for line in personal_trs:
                if line.find_all('td')[0].text.strip() == 'Категория':
                    person['personal']['category'] = line.find_all('td')[1].text.strip()
                elif line.find_all('td')[0].text.strip() == 'Теги персоны':
                    person['personal']['tags'] = line.find_all('td')[1].text.strip().split('\n')
                elif line.find_all('td')[0].text.strip() == 'Дата рождения':
                    person['personal']['birthday'] = line.find_all('td')[1].find('meta')['content'].strip()
                elif line.find_all('td')[0].text.strip() == 'ИНН':
                    person['personal']['taxID'] = line.find_all('td')[1].text.strip()
                elif line.find_all('td')[0].text.strip() == 'Гражданство':
                    person['personal']['nationality'] = line.find_all('td')[1].text.strip()
                elif line.find_all('td')[0].text.strip() == 'Проживает':
                    values = line.find_all('td')[1].text.strip()
                    values = values.replace('\n', '').split(',')
                    for value in values:
                        values[values.index(value)] = value.strip()
                    person['personal']['lives'] = values
                elif line.find_all('td')[0].text.strip() == 'Владеет недвижимостью':
                    values = line.find_all('td')[1].text.strip()
                    values = values.replace('\n', '').split(',')
                    for value in values:
                        values[values.index(value)] = value.strip()
                    person['personal']['realty-in'] = values
                elif line.find_all('td')[0].text.strip() == 'Под санкциями':
                    values = line.find_all('td')[1].text.strip()
                    values = values.replace('\n', '').split(',')
                    for value in values:
                        values[values.index(value)] = value.strip()
                    person['personal']['sanctions'] = values
                elif line.find_all('td')[0].text.strip() == 'Последняя должность':
                    last_job = {}
                    last_job['company-name'] = line.find_all('td')[1].find('span', {'itemprop': 'name'}).text.strip()
                    last_job['company-link'] = self.url + line.find_all('td')[1].find('a')['href']
                    last_job['job-position'] = line.find_all('td')[1].find('span',
                                                                           {'itemprop': 'jobTitle'}).text.strip()

                    person['personal']['last-job'] = last_job
                elif line.find_all('td')[0].text.strip() == 'Профили в социальных сетях':
                    items = []
                    items_soup = line.find_all('td')[1].find_all('a',recursive=False)
                    for item in items_soup:
                        d = {
                            'archive-link': None,
                            'archive-title': None,
                            'name': item.text.strip(),
                            'link': item['href']
                        }
                        try:
                            sib = item.find_next_sibling()
                            if sib.get_attribute_list('class')[0] == 'archived_proof':
                                d['archive-link'] = sib.find('a')['href']
                                d['archive-title'] = sib.find('a')['title']
                        except:
                            pass
                        items.append(d)
                    person['personal']['social-profiles'] = items

                elif line.find_all('td')[0].text.strip() == 'Другие вебсайты':
                    items = []
                    items_soup = line.find_all('td')[1].find_all('a',recursive=False)
                    for item in items_soup:
                        d = {
                            'archive-link': None,
                            'archive-title': None,
                            'name': item.text.strip(),
                            'link': item['href']
                        }
                        try:
                            sib = item.find_next_sibling()
                            if sib.get_attribute_list('class')[0] == 'archived_proof':
                                d['archive-link'] = sib.find('a')['href']
                                d['archive-title'] = sib.find('a')['title']
                        except:
                            pass
                        items.append(d)
                    person['personal']['sites'] = items

        if workbefore is not None:
            hist = []
            date_from = None
            date_to = None
            for place in workbefore:
                d = {}
                dates = []
                datestr = place.find('span',{'class':'tl-date'}).text.replace('от','').replace('до','').strip()
                datestr = datestr.split('\n')
                #print(datestr)
                for dts in datestr:
                    if dts.strip() != '':
                        dates.append(dts.strip())
                if len(dates)>0:
                    if len(dates) > 1:
                        date_from = dates[0].strip()
                        date_to = dates[1].strip()
                    else:
                        date_from = dates[0].replace('от', '').strip()
                        date_to = None
                #else:

                jobname = place.find('div',{'class':'tl-content'})
                jname_link_ext = jobname.find('a').extract()
                jname_name = jname_link_ext.find('span',{'itemprop':'name'}).text.strip()
                jname_link = self.url + jname_link_ext['href']
                try:
                    jobname.find('a').extract()
                except: pass
                jname_pos = jobname.text.replace('\n','').strip(' ,')
                d = {
                    'start':date_from,
                    'stop':date_to,
                    'company-name':jname_name,
                    'company-link':jname_link,
                    'job-positiion':jname_pos
                }
                hist.append(d)
            person['workbefore'] = hist

        if connections is not None:
            d = {}
            #print(len(connections))
            for t in connections:
                conn_type = t.find('span').text.replace('\n','').strip().lower()
                conn_type = self.transliterate(conn_type).replace(' ','-')
                li_s = t.find('ul',{'class':'h'}).find_all('li',{'itemprop':'relatedTo'})
                if len(li_s) == 0:
                    li_s = t.find('ul',{'class':'h'}).find_all('li', {'itemprop': 'knows'})

                l = []
                for li in li_s:
                    p_url = li.find('a',{'itemprop':'url'}).extract()
                    p_id = p_url['href'].split('/')[-1:][0]
                    p_name = p_url.text.strip(' \n')
                    try:
                        p_country = li.find('span',{'class':'flag'}).extract()
                        p_country_name = p_country['title']
                    except:
                        p_country_name = None
                    try:
                        p_birthday = li.find('meta').extract()
                        p_birthday_value = p_birthday['content'].strip()
                    except:
                        p_birthday_value = None
                    try: li.find('a',{'class':'modalConnectionShow'}).extract()
                    except: pass
                    try: li.find('div',{'class':'modalConnectionBox'}).extract()
                    except: pass
                    p_role = li.text.strip()
                    # .replace('–', '').replace(',', '')
                    #p_role = li.text.replace('–','').replace(',','').strip()
                    if len(p_role.split('\n'))>1:
                        buf = ''
                        p_role = p_role.split('\n')
                        for line in p_role:
                            line = line.replace('–', '').replace(',', '').strip()
                            if line != '': buf = buf + ', '+ line
                        p_role = buf.strip(' ,-')
                    else:
                        p_role = p_role.replace('–', '').replace(',', '').strip(' ,-')

                    dd = {
                        'person-id':p_id,
                        'name':p_name,
                        'country':p_country_name,
                        'birthday':p_birthday_value,
                        'role':p_role
                    }
                    #print(dd)
                    l.append(dd)
                d.update({conn_type:l})
                #print(d)
            person['connections'] = d
            #person.update({'connections':d})

        if companies is not None:
            hist = []
            date_from = None
            date_to = None
            for place in companies:
                dates = []
                datestr = place.find('span', {'class': 'tl-date'}).text.replace('от', '').replace('до', '').strip()
                datestr = datestr.split('\n')
                for dts in datestr:
                    if dts.strip() != '':
                        dates.append(dts.strip())
                if len(dates)>0:
                    if len(dates) > 1:
                        date_from = dates[0].strip()
                        date_to = dates[1].strip()
                    else:
                        date_from = dates[0].strip()
                        date_to = None
                cont = place.find('div', {'class': 'tl-content'}).find('div')
                place = cont.find('a',{'itemprop':'worksFor'}).extract()
                place_name = place.find('span',{'itemprop':'name'}).text.strip()
                place_link = self.url + place['href']
                try:
                    place_tax_id = place.find('span', {'itemprop': 'taxID'}).text.strip()
                except:
                    place_tax_id = None
                try:
                    p_country = cont.find('span').extract()
                    p_country_name = p_country['title']
                except:
                    p_country_name = None
                try: cont.find('a', {'class': 'modalConnectionShow'}).extract()
                except: pass
                try: cont.find('div', {'class': 'modalConnectionBox'}).extract()
                except: pass
                p_role = cont.text.strip(' ,')
                if len(p_role.split('\n')) > 1:
                    buf = ''
                    p_role = p_role.split('\n')
                    for line in p_role:
                        line = line.replace('–', '').replace(',', '').strip()
                        if line != '': buf = buf + ', ' + line
                    p_role = buf.strip(' ,-')
                else:
                    p_role = p_role.replace('–', '').replace(',', '').strip(' ,-')
                d = {
                    'start':date_from,
                    'stop':date_to,
                    'company-name':place_name,
                    'company-tax-id':place_tax_id,
                    'company-link':place_link,
                    'company-country':p_country_name,
                    'role':p_role
                }
                hist.append(d)
            person['companies']=hist


        if reputation is not None:
            pass

        # -----------------------------------------
        f = open(fname, 'w', encoding='utf-8')
        f.write(json.dumps(person, ensure_ascii=False, indent=4))
        f.close()

        return person

    def single_threaded_load(self, links, use_proxy=False):
        for link in links:
            try:
                d = self.parse_person(link, use_proxy)
                self.current += 1
                logging.info(f'UPDATED {self.current} of {self.total} -=- {d["full-name"]} ({d["person-id"]}) - {link}')
            except Exception as e:
                logging.info(f'{link} : {e}')
                pass

    def multi_threaded_load(self, links, threads_count, use_proxy=False):
        t_s = []
        tc = threads_count
        logging.info(f'Initial links count:{len(links)}')
        buf = []
        for link in links:
            p = str(urlparse(link).path.split('/')[-1:][0]) + '.json'
            if not os.path.exists('pages/' + p):
                buf.append(link)
        links = buf
        self.total = len(links)
        logging.info(f'Filtered links count:{len(links)}')
        l_count, l_mod = divmod(len(links), tc)

        mod_flag = False
        l_mod = len(links) % tc
        # print(l_mod)
        if l_mod != 0:
            mod_flag = True
            l_mod = len(links) % threads_count
            if l_mod == 0:
                tc = threads_count
                l_count = len(links) // tc
                mod_flag = False
            else:
                tc = threads_count - 1
                l_count = len(links) // tc
                mod_flag = True

        l_c = []
        for i in range(0, threads_count):
            logging.info(f'{i + 1} of {threads_count}')
            # print(f'{i+1} of {threads_count}')
            l_c.append(links[l_count * i:l_count * i + l_count])

        for i in range(0, threads_count):
            t_s.append(
                threading.Thread(target=self.single_threaded_load, args=(l_c[i], use_proxy,),
                                 daemon=True))
        for t in t_s:
            # time.sleep(1)
            t.start()
            # t.join()
            logging.info(f'Started thread #{t_s.index(t) + 1} of {len(t_s)} with {len(l_c[t_s.index(t)])} links')
            # print(f'Started thread #{t_s.index(t)+1} of {len(t_s)} with {len(l_c[t_s.index(t)])} links')
        for t in t_s:
            t.join()
            logging.info(f'Joined thread #{t_s.index(t) + 1} of {len(t_s)} with {len(l_c[t_s.index(t)])} links')
            # print(f'Joined thread #{t_s.index(t) + 1} of {len(t_s)} with {len(l_c[t_s.index(t)])} links')

    def load_html_to_file(self, url, fname='index.html', use_proxy=False):
        r = self._get(url, use_proxy)
        html = r.content.decode('utf-8')
        soup = BeautifulSoup(html, features='html.parser')
        f = open(fname, 'w', encoding='utf-8')
        f.write(soup.prettify())
        f.close()

    def config_load(self, fname='config.ini'):
        config = configparser.ConfigParser()
        config.read(fname)
        print(config['proxies']['path'])  # -> "/path/name/"
        config['DEFAULT']['path'] = '/var/shared/'  # update
        config['DEFAULT']['default_message'] = 'Hey! help me!!'  # create


if __name__ == '__main__':
    a = Api()
    # proxies = ['http://GrandMeg:rTd57fsD@188.191.164.19:9004']
    proxies = ['http://s2CLEw:GRH8uA@45.139.171.166:8000']
    for proxy in proxies:
        a.proxies.append({
            "http": proxy,
            "https": proxy,
            "ftp": proxy
        })
    DEV = True

    # logging.info(f'Total links: {len(links)}')
    # a.load_html_to_file('https://rupep.org/ru/persons_list/',use_proxy=True)
    # items = a.get_main_data(True)
    f = open('persons.json', 'r', encoding='utf-8')
    lines = f.read()
    f.close()
    items = json.loads(lines)
    links = []
    for item in items:
        try:
            links.append(item['person-link'])
        except:
            logging.info(f'No link...')
    #logging.info(f'{links[0]}')
    a.multi_threaded_load(links, 50, True)
    #i = a.parse_person('https://rupep.org/ru/person/4599',True)
    #print(json.dumps(i,ensure_ascii=False,indent=4))

    #

    #