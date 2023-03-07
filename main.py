import configparser
import datetime
import json
import linecache
# from pysondb import db
import logging
import os
import sys
import threading
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, filename="parser.log", filemode="a",
                    format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))


def LogException():
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    logging.info('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj))


class Api:
    url = 'https://rupep.org'
    persons_base = {}
    headers = {}
    # img_storage = 'pages/images/'
    # remote_img_storage = 'images/'
    proxies = []
    kyc_persons = {}
    archive_date_start = None
    archive_date_end = None
    _proxy_iter = 0
    archive_links = []
    total = 0
    current = 0
    total_comp = 0
    current_comp = 0
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
        # self.database = db.getDb(self.dbfile)

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
            self.persons_base.update({str(id): item})
            items.append(item)
            logging.info(f'PARSED {id}: {fio_ru} / {fio_en} / {birthday}')
        f = open('persons.json', 'w', encoding='utf-8')
        f.write(json.dumps(items, ensure_ascii=False, indent=4))
        f.close()
        # self.database.addMany(items)
        logging.info(f'TOTAL {len(items)} persons')
        # logging.info(f'Sync ids...')
        # kyc_persons = self.load_kyc_persons()
        # logging.info(f'Upload data...')
        # for item in tqdm(items):
        #     data = {}
        #     try:
        #         name_ru = ' '.join(item['fio']['ru'].split(' ')[2:]) + ' ' + item['fio']['ru'].split(' ')[0] + " " + \
        #                   item['fio']['ru'].split(' ')[1]
        #         data.update({'name1_ru': name_ru.split(' ')[0]})
        #         data.update({'name2_ru': name_ru.split(' ')[1]})
        #         data.update({'name3_ru': name_ru.split(' ')[2]})
        #         data.update({'name_ru': name_ru})
        #     except:
        #         try:
        #             name_ru = item['fio']['ru'].split(' ')[1] + ' ' + item['fio']['ru'].split(' ')[0]
        #             data.update({'name1_ru': name_ru.split(' ')[0]})
        #             data.update({'name2_ru': name_ru.split(' ')[1]})
        #             data.update({'name_ru': name_ru})
        #         except:
        #             name_ru = item['fio']['ru'].strip()
        #             data.update({'name2_ru': name_ru})
        #             data.update({'name_ru': name_ru})
        #     try:
        #         name_en = ' '.join(item['fio']['en'].split(' ')[2:]) + ' ' + item['fio']['en'].split(' ')[
        #             0] + " " + \
        #                   item['fio']['en'].split(' ')[1]
        #         data.update({'name1_en': name_en.split(' ')[0]})
        #         data.update({'name2_en': name_en.split(' ')[1]})
        #         data.update({'name3_en': name_en.split(' ')[2]})
        #         data.update({'name_en': name_en})
        #     except:
        #         try:
        #             name_en = item['fio']['en'].split(' ')[1] + ' ' + item['fio']['en'].split(' ')[0]
        #             data.update({'name1_en': name_en.split(' ')[0]})
        #             data.update({'name2_en': name_en.split(' ')[1]})
        #             data.update({'name_en': name_en})
        #         except:
        #             name_en = item['fio']['en'].strip()
        #             data.update({'name2_en': name_en})
        #             data.update({'name_en': name_en})
        #     if not name_ru.upper() in kyc_persons:
        #         url = 'https://kycbase.io/parsers/api/persons/'
        #         headers = {
        #             'Accept': '*/*',
        #             # 'Accept-Encoding':'gzip, deflate, br',
        #             # 'Connection': 'keep-alive',
        #             'Content-Type': 'application/json',
        #             'Authorization': 'Token 26b881c992c9b4c0f1b9fe13c9a10cf9c1aacbc1'
        #         }
        #         r = requests.post(url, headers=headers, data=json.dumps(data,ensure_ascii=False,indent=4).encode('utf-8'))
        #         resp = r.json()
        #         if not(isinstance(resp['name_ru'],list)):
        #             kyc_persons.update({resp["name_ru"].upper():resp["id"]})
        #             logging.info(f'ADD: {resp["id"]} - {resp["name_ru"]}')
        #         else:
        #             logging.info(f'Person {data["name_ru"]} is alredy exists!!! Have no idea about ID =(')
        #
        # self.kyc_persons = kyc_persons
        # f = open('kyc_persons.json','w',encoding='utf-8')
        # f.write(json.dumps(kyc_persons,ensure_ascii=False,indent=4))
        # f.close()

        return items

    def load_kyc_persons(self):
        DEV = False
        base = {}
        url = 'https://kycbase.io/parsers/api/persons/'
        headers = {
            'Accept': '*/*',
            # 'Accept-Encoding':'gzip, deflate, br',
            # 'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'Authorization': 'Token 26b881c992c9b4c0f1b9fe13c9a10cf9c1aacbc1'
        }
        r = requests.get(url, headers=headers).json()
        count = r['count']
        logging.info(f'Persons on KYC: {count}')
        limit = 1000
        offset = 0
        ran = round(count / limit) + 1
        for i in tqdm(range(0, ran), desc='Loading persons'):
            if DEV and i == 10:
                break
            path = url + f'?limit={limit}&offset={offset}'
            # print(path)
            try:
                res = self._get(path, headers=headers).json()['results']
                for k in res:
                    try:
                        base.update({k['name_ru'].strip().upper(): k['id']})
                    except:
                        try:
                            base.update({k['name_en'].strip().upper(): k['id']})
                        except:
                            try:
                                base.update({k['name_uk'].strip().upper(): k['id']})
                            except:
                                pass
            except Exception as e:
                logging.info(f'{e}')
                print(f'Ended on {i} page of {ran}')
                break
            offset += limit
        #self.kyc_persons = base
        f = open('kyc_persons.json', 'w', encoding='utf-8')
        f.write(json.dumps(base, ensure_ascii=False, indent=4))
        f.close()

        return base

    def find_company_by_name(self, company_name):
        url = 'https://kycbase.io/parsers/api/companies/?name=' + company_name
        headers = headers = {
            'Accept': '*/*',
            # 'Accept-Encoding':'gzip, deflate, br',
            # 'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'Authorization': 'Token 26b881c992c9b4c0f1b9fe13c9a10cf9c1aacbc1'
        }
        r = requests.get(url, headers=headers)
        try:
            res = r.json()['results'][0]
        except:
            res = {'id': None}
        return res

    def find_person_by_name(self, person):
        url = 'https://kycbase.io/parsers/api/persons/?name=' + person
        headers = headers = {
            'Accept': '*/*',
            # 'Accept-Encoding':'gzip, deflate, br',
            # 'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'Authorization': 'Token 26b881c992c9b4c0f1b9fe13c9a10cf9c1aacbc1'
        }
        r = requests.get(url, headers=headers)
        try:
            res = r.json()['results'][0]
        except:
            # LogException()
            res = {'id': None}
        return res

    def upload_company(self, fnamert: str):
        url = 'https://kycbase.io/parsers/api/companies/'
        headers = {
            'Accept': '*/*',
            # 'Accept-Encoding':'gzip, deflate, br',
            # 'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'Authorization': 'Token 26b881c992c9b4c0f1b9fe13c9a10cf9c1aacbc1'
        }
        print(fnamert)
        f = open(fnamert, 'r', encoding='utf-8')
        company = json.loads(f.read())
        f.close()
        t_c = self.find_company_by_name(company['name'])
        logging.info(t_c)
        if t_c['id'] == None:
            url = 'https://kycbase.io/parsers/api/companies/'
            company['name'] = company['name'].upper()
            company = json.dumps(company,ensure_ascii=False, indent=4)
            r = requests.post(url, headers=headers, data=company.encode('utf-8'))
            logging.info(r.text)
            return r.json()
        else:
            url = 'https://kycbase.io/parsers/api/companies/' + str(t_c['id']) + '/'
            for key in t_c:
                if key != 'id':
                    if (t_c[key] != '') or (t_c[key] != None) or (t_c[key] != 0):
                        try:
                            del company[key]
                        except:
                            pass
            if len(company) > 0:
                company['name'] = company['name'].upper()
                company = json.dumps(company, ensure_ascii=False, indent=4)
                r = requests.patch(url, headers=headers, data=company.encode('utf-8'))
                logging.info(r.text)
                return r.json()
            else:
                return t_c

    def upload_person(self, fname: str):
        url = 'https://kycbase.io/parsers/api/persons/'
        headers = {
            'Accept': '*/*',
            # 'Accept-Encoding':'gzip, deflate, br',
            # 'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'Authorization': 'Token 26b881c992c9b4c0f1b9fe13c9a10cf9c1aacbc1'
        }
        f = open(fname, 'r', encoding='utf-8')
        person = json.loads(f.read())
        f.close()
        t_c = self.find_person_by_name(person['name_ru'])
        if t_c['id'] == None:
            url = 'https://kycbase.io/parsers/api/persons/'
            logging.info(f'{person["name_ru"]}: ADD...')
            person = json.dumps(person, ensure_ascii=False, indent=4)
            r = requests.post(url, headers=headers, data=person.encode('utf-8'))
            logging.info(r.text)
            return r.json()
        else:
            url = 'https://kycbase.io/parsers/api/persons/' + str(t_c['id']) + '/'
            for key in t_c:
                if key != 'id':
                    if (t_c[key] != '') or (t_c[key] != None) or (t_c[key] != 0):
                        del person[key]
            if len(person) > 0:
                logging.info(f'{person["name_ru"]}: UPD...')
                person = json.dumps(person, ensure_ascii=False, indent=4)
                r = requests.patch(url, headers=headers, data=person.encode('utf-8'))
                logging.info(r.text)
                return r.json()

    def parse_person(self, url, use_proxy=False):
        # path = self.url + '/articles/' + url
        companies_links = []
        path = url
        url = urlparse(path).path.split('/')[-1:][0]
        r = self._get(path, use_proxy=use_proxy)
        html = r.content.decode('utf-8')
        soup = BeautifulSoup(html, features="html.parser")

        id = urlparse(url).path.split('/')[-1:][0]
        try:
            profile = soup.find('section', {'id': 'profile'})
        except Exception as e:
            logging.info(f'Cant parse {id}: {e}')
            return {}
        fname = f'persons/{id}.json'
        person = {}

        try:
            person.update({'photo-link': self.url + profile.find('div', {'class': 'avatar'}).find('img')['src']})
        except:
            pass
        try:
            name_ru = profile.find('header', {'class': 'profile-header'}).text.strip(' \n')
            person.update({'name_ru': name_ru})
            try:
                person.update({'name1_ru': name_ru.split(' ')[0]})
                try:
                    person.update({'name2_ru': name_ru.split(' ')[1]})
                except:
                    pass
                try:
                    person.update({'name3_ru': name_ru.split(' ')[2]})
                except:
                    pass
            except:
                pass
        except:
            return {}

        try:
            name_en = self.persons_base[str(id)]['fio']['en']
            person.update({'name_en': name_en})
            try:
                person.update({'name2_en': name_en.split(' ')[0]})
                try:
                    person.update({'name3_en': name_en.split(' ')[1]})
                except:
                    pass
                try:
                    person.update({'name1_en': name_en.split(' ')[2]})
                except:
                    pass
            except:
                pass
        except:
            pass

        # ----------- FIND BLOCKS --------------
        # -- Общая информация
        try:
            personal_trs = profile.find('div', {'id': 'personal'}).find('table').find_all('tr')
        except:
            personal_trs = None
        # -- Карьера
        try:
            workbefore = profile.find('div', {'id': 'workbefore'}).find('ul', {'class': 'timeline'}).find_all('li', {
                'class': 'tl-item'})
        except:
            workbefore = None
        # -- Связи
        try:
            connections = profile.find('div', {'id': 'connections'}).find('ul').find('li').find('ul').find_all('li',
                                                                                                               recursive=False)
        except:
            connections = None
        # -- Связанные юридические лица
        try:
            companies = profile.find('div', {'id': 'related-companies'}).find('ul', {'class': 'timeline'}).find_all(
                'li')
        except:
            companies = None
        # -- Уголовные производства и санкции
        try:
            reputation = profile.find('div', {'id': 'reputation'}).find('div', {'class': 'printWrap'})
        except:
            reputation = None
        # --------------------------------------

        # ---------- ОБРАБОТКА БЛОКОВ -------------
        if personal_trs is not None:
            logging.info(f'{name_ru}: Parsing personsl...')
            for line in personal_trs:
                if line.find_all('td')[0].text.strip() == 'Категория':
                    person.update({'category': line.find_all('td')[1].text.strip()})
                # elif line.find_all('td')[0].text.strip() == 'Теги персоны':
                #     person['personal']['tags'] = line.find_all('td')[1].text.strip().split('\n')
                if line.find_all('td')[0].text.strip() == 'Дата рождения':
                    person.update({'birthday': line.find_all('td')[1].find('meta')['content'].strip()})
                elif line.find_all('td')[0].text.strip() == 'ИНН':
                    try:
                        person.update({'tax_id': line.find_all('td')[1].text.strip()})
                    except Exception as e:
                        LogException()
                        logging.info(e)
                elif line.find_all('td')[0].text.strip() == 'Гражданство':
                    person.update({'citizenship': line.find_all('td')[1].text.strip()})
                elif line.find_all('td')[0].text.strip() == 'Проживает':
                    values = line.find_all('td')[1].text.strip()
                    values = values.replace('\n', '').split(',')
                    for value in values:
                        values[values.index(value)] = value.strip()
                    person.update({'lives': ','.join(values)})
                elif line.find_all('td')[0].text.strip() == 'Владеет недвижимостью':
                    values = line.find_all('td')[1].text.strip()
                    values = values.replace('\n', '').split(',')
                    for value in values:
                        values[values.index(value)] = value.strip()
                    person.update({'realty_in': values})
                elif line.find_all('td')[0].text.strip() == 'Под санкциями':
                    values = line.find_all('td')[1].text.strip()
                    values = values.replace('\n', '').split(',')
                    for value in values:
                        values[values.index(value)] = value.strip()
                    person.update({'sanctions': values})
                elif line.find_all('td')[0].text.strip() == 'Последняя должность':

                    last_job = {}
                    last_job['company-name'] = line.find_all('td')[1].find('span', {'itemprop': 'name'}).text.strip()
                    last_job['company-link'] = self.url + line.find_all('td')[1].find('a')['href']
                    if last_job['company-link'].find('https://rupep.org/ru/company/') > -1:
                        companies_links.append(last_job['company-link'])
                    last_job['job-position'] = line.find_all('td')[1].find('span',
                                                                           {'itemprop': 'jobTitle'}).text.strip()
                    person.update({'current_job_ru': last_job['company-name'] + ', ' + last_job['job-position']})
                    # person['personal']['last-job'] = last_job
                elif line.find_all('td')[0].text.strip() == 'Профили в социальных сетях':
                    items = []
                    items_soup = line.find_all('td')[1].find_all('a', recursive=False)
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
                    person.update({'social_profiles': items})

                elif line.find_all('td')[0].text.strip() == 'Другие вебсайты':
                    items = []
                    items_soup = line.find_all('td')[1].find_all('a', recursive=False)
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
                    person.update({'sites': items})

        if not workbefore is None:
            logging.info(f'{name_ru}: Parsing career_connections...')
            hist = []
            date_from = None
            date_to = None
            for place in workbefore:
                d = {}
                dates = []
                datestr = place.find('span', {'class': 'tl-date'}).text.replace('от', '').replace('до', '').strip()
                datestr = datestr.split('\n')

                for dts in datestr:
                    if dts.strip() != '':
                        dates.append(dts.strip())
                if len(dates) > 0:
                    if len(dates) > 1:
                        dt_str = dates[0].strip()
                        try:
                            date_from = datetime.datetime.strptime(dt_str, '%d.%m.%Y')
                        except:
                            try:
                                date_from = datetime.datetime.strptime(dt_str, '%%m.%Y')
                            except:
                                try:
                                    date_from = datetime.datetime.strptime(dt_str, '%Y')
                                except:
                                    date_from = None
                        dt_str = dates[1].strip()
                        try:
                            date_to = datetime.datetime.strptime(dt_str, '%d.%m.%Y')
                        except:
                            try:
                                date_to = datetime.datetime.strptime(dt_str, '%%m.%Y')
                            except:
                                try:
                                    date_to = datetime.datetime.strptime(dt_str, '%Y')
                                except:
                                    date_to = None
                    else:
                        dt_str = dates[0].replace('от', '').strip()
                        try:
                            date_from = datetime.datetime.strptime(dt_str, '%d.%m.%Y')
                        except:
                            try:
                                date_from = datetime.datetime.strptime(dt_str, '%%m.%Y')
                            except:
                                try:
                                    date_from = datetime.datetime.strptime(dt_str, '%Y')
                                except:
                                    date_from = None
                        date_to = None

                jobname = place.find('div', {'class': 'tl-content'})
                jname_link_ext = jobname.find('a').extract()
                jname_name = jname_link_ext.find('span', {'itemprop': 'name'}).text.strip()
                jname_link = self.url + jname_link_ext['href']
                try:
                    jobname.find('a').extract()
                except:
                    pass
                jname_pos = jobname.text.replace('\n', '').strip(' ,')
                company = self.find_company_by_name(jname_name)
                if (jname_link != None) or (jname_link != ''):
                    if jname_link.find('https://rupep.org/ru/company/') > -1:
                        companies_links.append(jname_link)
                d = {}
                if date_from != None:
                    d.update({'start': date_from.strftime('%Y-%m-%d')})
                if date_to != None:
                    d.update({'stop': date_to.strftime('%Y-%m-%d')})
                if (jname_pos != None) or (jname_pos != ''):
                    d.update({'job_position': jname_pos})
                try:
                    if company['id'] != None:
                        d.update({'company': company['id']})
                    else:
                        if (jname_link != '') or (jname_link != None):
                            if jname_link.find('https://rupep.org/ru/company/') > -1:
                                company = self.parse_company(jname_link)
                                fnamec = "companies/" + str(urlparse(jname_link).path.split('/')[-1:][0]) + '.json'
                                company = self.upload_company(fnamec)
                                logging.info(company)
                                d.update({'company': company['id']})
                            else:
                                continue
                except:
                    LogException()

                # d = {
                #     'start':date_from,
                #     'stop':date_to,
                #     'company-name':jname_name,
                #     'company-link':jname_link,
                #     'job-positiion':jname_pos
                # }
                hist.append(d)
            person['career_connections'] = hist

        if not connections is None:
            logging.info(f'{name_ru}: Parsing personal_connections...')
            d = {}
            l = []
            # print(len(connections))
            for t in connections:
                conn_type = t.find('span').text.replace('\n', '').strip().lower()
                # conn_type = self.transliterate(conn_type).replace(' ','-')
                li_s = t.find('ul', {'class': 'h'}).find_all('li', {'itemprop': 'relatedTo'})
                if len(li_s) == 0:
                    li_s = t.find('ul', {'class': 'h'}).find_all('li', {'itemprop': 'knows'})

                for li in li_s:
                    p_url = li.find('a', {'itemprop': 'url'}).extract()
                    p_id = p_url['href'].split('/')[-1:][0]
                    p_name = p_url.text.strip(' \n')
                    try:
                        p_country = li.find('span', {'class': 'flag'}).extract()
                        p_country_name = p_country['title']
                    except:
                        p_country_name = None
                    try:
                        p_birthday = li.find('meta').extract()
                        p_birthday_value = p_birthday['content'].strip()
                    except:
                        p_birthday_value = None
                    try:
                        li.find('a', {'class': 'modalConnectionShow'}).extract()
                    except:
                        pass
                    try:
                        li.find('div', {'class': 'modalConnectionBox'}).extract()
                    except:
                        pass
                    p_role = li.text.strip()
                    # .replace('–', '').replace(',', '')
                    # p_role = li.text.replace('–','').replace(',','').strip()
                    if len(p_role.split('\n')) > 1:
                        buf = ''
                        p_role = p_role.split('\n')
                        for line in p_role:
                            line = line.replace('–', '').replace(',', '').strip()
                            if line != '': buf = buf + ', ' + line
                        p_role = buf.strip(' ,-')
                    else:
                        p_role = p_role.replace('–', '').replace(',', '').strip(' ,-')
                    dd = {}
                    rel_person = self.find_person_by_name(p_name)
                    dd.update({'category': conn_type})
                    if (p_role != '') or (p_role != None):
                        dd.update({'role': p_role})
                    if rel_person['id'] != None:
                        dd.update({'person2': rel_person['id']})
                    # else:
                    #     if (p_id != '') or (p_id != None):
                    #         rel_person = self.parse_person('https://rupep.org/ru/person/' + str(p_id))
                    #         fname = "persons/" + p_id + '.json'
                    #         rel_person = self.upload_person(fname)
                    #         d.update({'person2': rel_person['id']})
                    # dd = {
                    #     'person-id':p_id,
                    #     'name':p_name,
                    #     'country':p_country_name,
                    #     'birthday':p_birthday_value,
                    #     'role':p_role
                    # }
                    # print(dd)
                    l.append(dd)
                d.update({conn_type: l})
                # print(d)
            person['person_connections'] = l
            # person.update({'connections':d})

        if companies is not None:
            logging.info(f'{name_ru}: Parsing company_connections...')
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
                if len(dates) > 0:
                    if len(dates) > 1:
                        dt_str = dates[0].strip()
                        try:
                            date_from = datetime.datetime.strptime(dt_str, '%d.%m.%Y')
                        except:
                            try:
                                date_from = datetime.datetime.strptime(dt_str, '%%m.%Y')
                            except:
                                try:
                                    date_from = datetime.datetime.strptime(dt_str, '%Y')
                                except:
                                    date_from = None
                        dt_str = dates[1].strip()
                        try:
                            date_to = datetime.datetime.strptime(dt_str, '%d.%m.%Y')
                        except:
                            try:
                                date_to = datetime.datetime.strptime(dt_str, '%%m.%Y')
                            except:
                                try:
                                    date_to = datetime.datetime.strptime(dt_str, '%Y')
                                except:
                                    date_to = None
                    else:
                        dt_str = dates[0].replace('от', '').strip()
                        try:
                            date_from = datetime.datetime.strptime(dt_str, '%d.%m.%Y')
                        except:
                            try:
                                date_from = datetime.datetime.strptime(dt_str, '%%m.%Y')
                            except:
                                try:
                                    date_from = datetime.datetime.strptime(dt_str, '%Y')
                                except:
                                    date_from = None
                        date_to = None
                cont = place.find('div', {'class': 'tl-content'}).find('div')
                place = cont.find('a', {'itemprop': 'worksFor'}).extract()
                place_name = place.find('span', {'itemprop': 'name'}).text.strip()
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
                try:
                    cont.find('a', {'class': 'modalConnectionShow'}).extract()
                except:
                    pass
                try:
                    cont.find('div', {'class': 'modalConnectionBox'}).extract()
                except:
                    pass
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

                if (place_link != None) or (place_link != ''):
                    if place_link.find('https://rupep.org/ru/company/') > -1:
                        companies_links.append(place_link)
                d = {}
                rel_company = self.find_company_by_name(place_name)
                if date_from != None:
                    d.update({'start': date_from.strftime('%Y-%m-%d')})
                if date_to != None:
                    d.update({'stop': date_to.strftime('%Y-%m-%d')})
                if (p_role != '') or (p_role != None):
                    d.update({'role': p_role})
                if rel_company['id'] != None:
                    d.update({'company': rel_company['id']})
                else:
                    if (place_link != '') or (place_link != None):
                        rel_company = self.parse_company(place_link)
                        fname = "companies/" + str(urlparse(place_link).path.split('/')[-1:][0]) + '.json'
                        rel_company = self.upload_company(fname)
                        d.update({'company': rel_company['id']})
                # d = {
                #     'start':date_from,
                #     'stop':date_to,
                #     'company-name':place_name,
                #     'company-tax-id':place_tax_id,
                #     'company-link':place_link,
                #     'company-country':p_country_name,
                #     'role':p_role
                # }
                hist.append(d)
            person['company_connections'] = hist

        if reputation is not None:
            pass
        companies_links = list(set(companies_links))
        logging.info(f'{name_ru}: total {str(len(companies_links))} companies by this person, writing to files...')
        f = open(f'persons/{id}.companies', 'w', encoding='utf-8')
        for line in companies_links:
            f.write(f'{line}\n')
        f.close()
        # -----------------------------------------
        f = open(fname, 'w', encoding='utf-8')
        f.write(json.dumps(person, ensure_ascii=False, indent=4))
        f.close()
        logging.info(f'{name_ru}: Uploading person...')
        r = self.upload_person(fname)
        logging.info(r)

        return person

    def single_threaded_load(self, links, use_proxy=False):
        for link in links:
            try:
                d = self.parse_person(link, use_proxy)
                self.current += 1
                logging.info(f'UPDATED {self.current} of {self.total} -=- {d["full-name"]} ({d["person-id"]}) - {link}')
            except Exception as e:
                logging.info(f'{link} : {e}')
                LogException()

    def single_threaded_load_companies(self, links, use_proxy=False):
        for link in links:
            try:
                d = self.parse_company(link, use_proxy)
                self.current_comp += 1
                logging.info(
                    f'UPDATED {self.current_comp} of {self.total_comp} -=- {d["full-name"]} ({d["person-id"]}) - {link}')
            except Exception as e:
                logging.info(f'{link} : {e}')
                LogException()

    def multi_threaded_load_companies(self, links, threads_count, use_proxy=False):
        t_s = []
        tc = threads_count
        logging.info(f'Initial links count:{len(links)}')
        buf = []
        for link in links:
            p = str(urlparse(link).path.split('/')[-1:][0]) + '.json'
            if not os.path.exists('pages/' + p):
                buf.append(link)
        links = buf
        self.total_comp = len(links)
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
                threading.Thread(target=self.single_threaded_load_companies, args=(l_c[i], use_proxy,),
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

    def get_companies_legacy(self):
        companies = []
        for root, dirs, files in os.walk('persons'):
            fnames = files
        for fname in tqdm(fnames):
            f = open(f'persons/{fname}', 'r', encoding='utf-8')
            js = json.loads(f.read())
            f.close()
            try:
                if js['personal']['last-job']['company-link'].find('https://rupep.org/ru/company/') > -1:
                    companies.append(js['personal']['last-job']['company-link'])
            except:
                pass
            try:
                for i in js['workbefore']:
                    if i['company-link'].find('https://rupep.org/ru/company/') > -1:
                        companies.append(i['company-link'])
            except:
                pass
            try:
                for i in js['companies']:
                    if i['company-link'].find('https://rupep.org/ru/company/') > -1:
                        companies.append(i['company-link'])
            except:
                pass
        res = list(set(companies))
        f = open('companies_links.txt', 'w', encoding='utf-8')
        for line in res:
            f.write(f'{line}\n')
        f.close()

    def get_companies(self):
        companies = []
        for root, dirs, files in os.walk('persons'):
            fnames = files
        for fname in tqdm(fnames):
            if fname.find('.companies') > 0:
                f = open(f'persons/{fname}', 'r', encoding='utf-8')
                lst = f.read().split('\n')
                f.close()
                companies += lst
        res = list(set(companies))
        f = open('companies_links.txt', 'w', encoding='utf-8')
        for line in res:
            f.write(f'{line}\n')
        f.close()

    def parse_company(self, url, use_proxy=False):
        path = url
        url = urlparse(path).path.split('/')[-1:][0]
        r = self._get(path, use_proxy=use_proxy)
        html = r.content.decode('utf-8')
        soup = BeautifulSoup(html, features="html.parser")
        profile = soup.find('section', {'id': 'profile'})
        id = urlparse(url).path.split('/')[-1:][0]
        fname_comp = f'companies/{id}.json'
        company = {}

        name = profile.find('h1', {'itemprop': 'name'}).text.replace('\n', '').strip().upper()
        company.update({'name': name})
        company_trs = profile.find('table').find_all('tr')
        if company_trs is not None:
            for line in company_trs:
                if line.find_all('td')[0].text.strip() == 'ОГРН':
                    company.update({'registration_id': line.find_all('td')[1].text.replace('\t', '').replace('\n',
                                                                                                             '').replace(
                        '\r', '').strip()})
                if line.find_all('td')[0].text.strip() == 'Дата создания':
                    dt_str = line.find_all('td')[1].text.replace('\t', '').replace('\n', '').replace('\r', '').strip()
                    try:
                        date_cr = datetime.datetime.strptime(dt_str, '%d.%m.%Y')
                    except:
                        try:
                            date_cr = datetime.datetime.strptime(dt_str, '%m.%Y')
                        except:
                            try:
                                date_cr = datetime.datetime.strptime(dt_str, '%Y')
                            except:
                                date_cr = None
                    if date_cr != None:
                        company.update({'date_creation': date_cr.strftime('%Y-%m-%d')})
                if line.find_all('td')[0].text.strip() == 'Зарегистрирован(-а)':
                    company.update({'country_registration': line.find_all('td')[1].text.replace('\t', '').replace('\n',
                                                                                                                  '').replace(
                        '\r', '').strip()})
                if line.find_all('td')[0].text.strip() == 'Адрес':
                    company.update({'address_company': line.find_all('td')[1].text.replace('\t', '').replace('\n',
                                                                                                             '').replace(
                        '\r', '').strip()})
                if len(line.find_all('td')) == 1:
                    try:
                        ws = line.find('td').find('a')
                        if ws.text.strip() == 'Вкбсайт':
                            company.update({'website': ws['href']})
                    except:
                        pass

        f = open(fname_comp, 'w', encoding='utf-8')
        f.write(json.dumps(company, ensure_ascii=False, indent=4))
        f.close()

        return company


def init():
    global a
    a = Api()
    # proxies = ['http://GrandMeg:rTd57fsD@188.191.164.19:9004']
    proxies = ['http://cusq7Q:gYAfFe@170.83.235.7:8000']
    for proxy in proxies:
        a.proxies.append({
            "http": proxy,
            "https": proxy,
            "ftp": proxy
        })
    DEV = False


def go_parse():
    global a
    items = a.get_main_data(True)
    links = []
    for item in items:
        try:
            links.append(item['person-link'])
        except:
            logging.info(f'No link...')
    # logging.info(f'{links[0]}')
    f = open('kyc_persons.json','r',encoding='utf-8')
    a.kyc_persons = json.loads(f.read())
    f.close()
    a.multi_threaded_load(links, 1, True)


if __name__ == '__main__':
    init()
    go_parse()
    # a.get_companies()
