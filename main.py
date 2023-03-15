import configparser
import datetime
import json
import linecache
# from pysondb import db
import logging
import os
import sys
import threading
import urllib.parse
from urllib.parse import urlparse

import requests
from requests_toolbelt import MultipartEncoder
from bs4 import BeautifulSoup
from tqdm import tqdm
from tqdm.contrib import tenumerate
from pathlib import *
import warnings

warnings.filterwarnings("ignore")

#requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
requests.adapters.DEFAULT_RETRIES = 5
#requests.


dtnow = datetime.date.today().strftime('%d_%m_%Y')
logging.basicConfig(level=logging.INFO, filename=f"parser_{dtnow}.log", filemode="a",
                    format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

home_path = Path.cwd()
persons_path = home_path / 'persons'
companies_path = home_path / 'companies'


def to_json(inp_d):
    return json.dumps(inp_d,ensure_ascii=False,indent=4)


def to_json_file(inp_d, filename):
    with open(filename,'w',encoding='utf-8') as f:
        f.write(to_json(inp_d))


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
    BASE_URL = 'https://92.246.85.71/'
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
            print(url)
            r = requests.get(url=url, headers=self.headers, proxies=self.proxies[self._proxy_iter])#, verify=False)
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
        logging.info(f'Sync ids...')
        kyc_persons = self.load_kyc_persons()
        logging.info(f'Upload data...')
        limit = 200
        current = 0
        lst = []
        #kyc_txt = json.dumps(kyc_persons,ensure_ascii=False)

        for idx, item in tenumerate(items, desc='Uploading...'):
            FLAG = False
            #if current < limit:
            data = {}
            try:
                name_ru = ' '.join(item['fio']['ru'].split(' ')[2:]) + ' ' + item['fio']['ru'].split(' ')[0] + " " + \
                          item['fio']['ru'].split(' ')[1]
                data.update({'name1_ru': name_ru.split(' ')[0]})
                data.update({'name2_ru': name_ru.split(' ')[1]})
                data.update({'name3_ru': name_ru.split(' ')[2]})
                data.update({'name_ru': name_ru})
            except:
                try:
                    name_ru = item['fio']['ru'].split(' ')[1] + ' ' + item['fio']['ru'].split(' ')[0]
                    data.update({'name1_ru': name_ru.split(' ')[0]})
                    data.update({'name2_ru': name_ru.split(' ')[1]})
                    data.update({'name_ru': name_ru})
                except:
                    name_ru = item['fio']['ru'].strip()
                    data.update({'name2_ru': name_ru})
                    data.update({'name_ru': name_ru})
            try:
                name_en = ' '.join(item['fio']['en'].split(' ')[2:]) + ' ' + item['fio']['en'].split(' ')[
                    0] + " " + \
                          item['fio']['en'].split(' ')[1]
                data.update({'name1_en': name_en.split(' ')[0]})
                data.update({'name2_en': name_en.split(' ')[1]})
                data.update({'name3_en': name_en.split(' ')[2]})
                data.update({'name_en': name_en})
            except:
                try:
                    name_en = item['fio']['en'].split(' ')[1] + ' ' + item['fio']['en'].split(' ')[0]
                    data.update({'name1_en': name_en.split(' ')[0]})
                    data.update({'name2_en': name_en.split(' ')[1]})
                    data.update({'name_en': name_en})
                except:
                    name_en = item['fio']['en'].strip()
                    data.update({'name2_en': name_en})
                    data.update({'name_en': name_en})
            # -- CHECK ITEM IN KYC --
            if data['name_ru'].upper() in kyc_persons:
                FLAG = True
                KYC_NAME = data['name_ru'].upper()
            try:
                if data['name_en'].upper() in kyc_persons:
                    FLAG = True
                    KYC_NAME = data['name_en'].upper()
            except:
                pass
            # -----------------------
            per_id = str(item['person-id'])
            item_path = persons_path / per_id
            item_path.mkdir(parents=False, exist_ok=True)
            item_base_file = item_path / f'base_file'
            item_gid_file = item_path / f'gid'
            with open(item_base_file, 'w', encoding='utf-8') as f:
                f.write(json.dumps(data, ensure_ascii=False, indent=4))

            if FLAG:
                lst.append(data)
                with open(item_gid_file, 'w', encoding='utf-8') as f:
                    f.write(str(kyc_persons[KYC_NAME]))
                current += 1
            # else:
            #     current = 0
            #     url = self.BASE_URL + 'parsers/api/persons/bulk/'
            #     headers = {
            #         'Accept': '*/*',
            #         # 'Accept-Encoding':'gzip, deflate, br',
            #         # 'Connection': 'keep-alive',
            #         'Content-Type': 'application/json',
            #         'Authorization': 'Token 26b881c992c9b4c0f1b9fe13c9a10cf9c1aacbc1'
            #     }
            #     try:
            #         r = requests.post(url, headers=headers, data=json.dumps(lst,ensure_ascii=False,indent=4).encode('utf-8'), verify=False)
            #         resp = r.json()
            #         for ind, i in enumerate(resp):
            #             try:
            #                 if not(isinstance(i['name_ru'],list)):
            #                     kyc_persons.update({i["name_ru"].upper():int(i["id"])})
            #                     logging.info(f'ADD: {i["id"]} - {i["name_ru"]}')
            #                 else:
            #                     kyc_persons.update({lst[ind]["name_ru"].upper(): int(i["name_ru"][0]["id"])})
            #                     logging.info(f'Person {lst[ind]["name_ru"]} is alredy exists! ID: {i["name_ru"][0]["id"]}')
            #             except:
            #                 try:
            #                     name = "name_en"
            #                     kyc_persons.update({lst[ind][name].upper(): int(i[name][0]["id"])})
            #                     logging.info(f'Person {lst[ind]["name_ru"]} is alredy exists! ID: {i[name][0]["id"]}')
            #                 except:
            #                     try:
            #                         name = "name_uk"
            #                         kyc_persons.update({lst[ind]["name_ru"].upper(): int(i[name][0]["id"])})
            #                         logging.info(f'Person {lst[ind]["name_ru"]} is alredy exists! ID: {i[name][0]["id"]}')
            #                     except:
            #                         LogException()
            #     except:
            #         LogException()
            #     lst = []


        self.kyc_persons = kyc_persons
        f = open('kyc_persons.json','w',encoding='utf-8')
        f.write(json.dumps(kyc_persons,ensure_ascii=False,indent=4))
        f.close()

        return items

    def load_kyc_persons(self):
        DEV = False
        base = {}
        url = self.BASE_URL + 'parsers/api/persons/'
        headers = {
            'Accept': '*/*',
            # 'Accept-Encoding':'gzip, deflate, br',
            # 'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'Authorization': 'Token 26b881c992c9b4c0f1b9fe13c9a10cf9c1aacbc1'
        }
        r = requests.get(url, headers=headers, verify=False).json()
        count = r['count']
        logging.info(f'Persons on KYC: {count}')
        limit = 1000
        offset = 0
        ran = round(count / limit) + 1
        for i in tqdm(range(0, ran), desc='Loading KYC persons'):
            if DEV and i == 10:
                break
            path = url + f'?limit={limit}&offset={offset}'
            # print(path)
            try:
                res = requests.get(path, headers=headers, verify=False).json()['results']
                for k in res:
                    try:
                        base.update({k['name_ru'].strip().upper(): int(k['id'])})
                    except:
                        try:
                            base.update({k['name_en'].strip().upper(): int(k['id'])})
                        except:
                            try:
                                base.update({k['name_uk'].strip().upper(): int(k['id'])})
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
        
        url = self.BASE_URL + 'parsers/api/companies/?name=' + company_name
        headers = headers = {
            'Accept': '*/*',
            # 'Accept-Encoding':'gzip, deflate, br',
            # 'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'Authorization': 'Token 26b881c992c9b4c0f1b9fe13c9a10cf9c1aacbc1'
        }
        r = requests.get(url, headers=headers, verify=False)
        try:
            res = r.json()['results'][0]
        except:
            res = {'id': None}
        return res

    def find_person_by_name(self, person):
        
        url = self.BASE_URL + 'parsers/api/persons/?name=' + urllib.parse.quote_plus(person)
        #print(url)
        headers = headers = {
            'Accept': '*/*',
            # 'Accept-Encoding':'gzip, deflate, br',
            # 'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'Authorization': 'Token 26b881c992c9b4c0f1b9fe13c9a10cf9c1aacbc1'
        }
        r = requests.get(url, headers=headers, verify=False)
        res_new = []
        try:
            res_s = r.json()['results']
            #print(r.text)
            for res in res_s:
                try:
                    if res['name_en'] == person:
                        #res = {'id': None}
                        logging.info(f'-- deleting {res["id"]}: {res["name_en"]}')
                        requests.delete(self.BASE_URL + f'parsers/api/persons/{res["id"]}/', headers=headers, verify=False)
                    else:
                        res_new.append(res)
                except:
                    pass
            if len(res_new)==0:
                result = {'id': None}
            else:
                result = res_new[0]
                logging.info(f'found: {result["name_ru"]}')
        except:
            # LogException()
            result = {'id': None}
        return result

    def upload_company(self, fnamert: str):
        
        url = self.BASE_URL + 'parsers/api/companies/'
        headers = {
            'Accept': '*/*',
            # 'Accept-Encoding':'gzip, deflate, br',
            # 'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'Authorization': 'Token 26b881c992c9b4c0f1b9fe13c9a10cf9c1aacbc1'
        }
        #print(fnamert)
        f = open(fnamert, 'r', encoding='utf-8')
        company = json.loads(f.read())
        f.close()
        t_c = self.find_company_by_name(company['name'])
        #logging.info(t_c)
        if t_c['id'] == None:
            url = self.BASE_URL + 'parsers/api/companies/'
            company['name'] = company['name'].upper()
            company = json.dumps(company,ensure_ascii=False, indent=4)
            r = requests.post(url, headers=headers, data=company.encode('utf-8'), verify=False)
            #logging.info(r.text)
            return r.json()
        else:
            url = self.BASE_URL + 'parsers/api/companies/' + str(t_c['id']) + '/'
            bb = company
            for key in t_c:
                if key != 'id':
                    if (t_c[key] == '') or (t_c[key] == None) or (t_c[key] == 0):
                        try:
                            del bb[key]
                        except:
                            pass
            company = bb
            if len(company) > 0:
                company['name'] = company['name'].upper()
                company = json.dumps(company, ensure_ascii=False, indent=4)
                r = requests.patch(url, headers=headers, data=company.encode('utf-8'), verify=False)
                #logging.info(r.text)
                return r.json()
            else:
                return t_c

    def upload_companies(self, bulk_dict):
        
        url = self.BASE_URL + 'parsers/api/companies/bulk/'
        headers = {
            'Accept': '*/*',
            'Content-Type': 'application/json',
            'Authorization': 'Token 26b881c992c9b4c0f1b9fe13c9a10cf9c1aacbc1'
        }
        r = requests.post(url, headers=headers, json=bulk_dict, verify=False)
        added = []
        exist = []
        try:
            res = r.json()
            for item in res:
                idx = res.index(item)
                #print(item)
                try:
                    if isinstance(item['name'], list):
                        item_id = int(item['name_ru'][0]['id'])
                        try:
                            item_out = bulk_dict[idx]
                            item_out.update({'id':item_id})
                            exist.append(item_out)
                        except:
                            LogException()
                except: pass
                if ("name" in item.keys()) and (isinstance(item['name'],str)):
                    try:
                        item_out = item
                        added.append(item_out)
                    except:
                        LogException()
        except:
            pass

        out = {
            'ADDED': added,
            'EXISTS': exist
        }

        logging.info(f'{len(added)}:{len(exist)}')

        return out

    def upload_persons(self, bulk_dict):
        
        url = self.BASE_URL + 'parsers/api/persons/bulk/'
        headers = {
            'Accept': '*/*',
            'Content-Type': 'application/json',
            'Authorization': 'Token 26b881c992c9b4c0f1b9fe13c9a10cf9c1aacbc1'
        }
        payload = json.dumps(bulk_dict,ensure_ascii=False,indent=4).encode('utf-8')
        #logging.info(payload.decode('utf-8'))
        r = requests.post(url, headers=headers, data=payload, verify=False)
        added = []
        exist = []
        #try:
        res = json.loads(r.text)
        logging.info(json.dumps(res,ensure_ascii=False,indent=4))
        print(len(res))
        #except:
        #    result = []
        try:
            for item in res:
                idx = res.index(item)
                print(item)
                flag = False
                try:
                    if isinstance(item['name_ru'], list):
                        flag = True
                        item_id = int(item['name_ru'][0]['id'])
                        try:
                            item_out = bulk_dict[idx]
                            item_out.update({'id':item_id})
                            exist.append(item_out)
                        except:
                            LogException()
                except: pass
                try:
                    if isinstance(item['name_en'], list) and (not(flag)):
                        flag = True
                        item_id = int(item['name_en'][0]['id'])
                        try:
                            item_out = bulk_dict[idx]
                            item_out.update({'id': item_id})
                            exist.append(item_out)
                        except:
                            LogException()
                except:
                    pass
                try:
                    if isinstance(item['name_uk'], list) and (not(flag)):
                        flag = True
                        item_id = int(item['name_uk'][0]['id'])
                        try:
                            item_out = bulk_dict[idx]
                            item_out.update({'id': item_id})
                            exist.append(item_out)
                        except:
                            LogException()
                except: pass
                if ("name_ru" in item.keys()) and (isinstance(item['name_ru'],str)):
                    try:
                        item_out = item
                        added.append(item_out)
                    except:
                        LogException()
        except:
            LogException()
        out = {
            'ADDED': added,
            'EXISTS': exist
        }
        logging.info(f'{len(added)}:{len(exist)}')
        #try:
        for item in exist:
            self.update_person_from_dict(item)
        #except:
        #    LogException()

        return out

    def process_uploading_companies(self, limit = 200):
        
        files = list(companies_path.rglob('*.json'))
        current = 0
        lst = []
        buf = []
        for fname in files:
            if current < limit:
                with open(fname, 'r', encoding='utf-8') as f:
                    item = json.loads(f.read())
                    logging.info(f'{fname}: {item["name"]}')
                    lst.append(item)
                    current += 1
            else:
                buf = self.upload_persons(lst)
                current = 0
                lst = []
        if len(lst) > 0:
            buf = self.upload_persons(lst)

    def process_uploading_persons(self, limit = 100):
        
        files = os.listdir('persons')
        #limit = 200
        current = 0
        lst = []
        buf = []
        for fname in tqdm(files):
            if fname.find('.json')>-1:
                fname = 'persons/'+fname
                if current < limit:
                    #logging.info(fname)
                    f = open(fname, 'r', encoding='utf-8')
                    item = json.loads(f.read())
                    logging.info(f'{fname}: {item["name_ru"]}')
                    if "social_profiles" in item.keys():
                        buf = []
                        for k in item['social_profiles']:
                            try:
                                buf.append(k['link'])
                            except: pass
                        item['social_profiles'] = buf
                    if "sites" in item.keys():
                        buf = []
                        for k in item['sites']:
                            try:
                                buf.append(k['link'])
                            except: pass
                        item['sites'] = buf
                    # try:
                    #     del item['social_profiles']
                    # except:
                    #     pass
                    try:
                        buf = []
                        for conn in item['person_connections']:
                            if 'person2' in conn.keys():
                                buf.append(conn)
                        item['person_connections'] = buf
                    except:
                        pass
                    try:
                        buf = []
                        for conn in item['career_connections']:
                            if 'company' in conn.keys():
                                buf.append(conn)
                        item['career_connections'] = buf
                    except:
                        pass
                    try:
                        buf = []
                        for conn in item['company_connections']:
                            if 'company' in conn.keys():
                                buf.append(conn)
                        item['company_connections'] = buf
                    except:
                        pass
                    buf = []
                    f.close()
                    f = open(fname, 'w', encoding='utf-8')
                    f.write(json.dumps(item,ensure_ascii=False,indent=4))
                    f.close()
                    lst.append(item)
                    current += 1

                else:
                    #tqdm.write(json.dumps(lst,ensure_ascii=False,indent=4))
                    buf = self.upload_persons(lst)
                    #print(json.dumps(buf, ensure_ascii=False, indent=4))
                    current = 0
                    lst = []
        try:
            buf = self.upload_persons(lst)
            #print(json.dumps(buf,ensure_ascii=False,indent=4))
        except:
            pass

    def convert_person(self,person):
        
        out = {}

    def add_person_from_dict(self, person: dict):
        
        url = self.BASE_URL + 'parsers/api/persons/'
        headers = {
            'Accept': '*/*',
            # 'Accept-Encoding':'gzip, deflate, br',
            # 'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'Authorization': 'Token 26b881c992c9b4c0f1b9fe13c9a10cf9c1aacbc1'
        }

        # if t_c['id'] == None:
        #     url = self.BASE_URL + 'parsers/api/persons/'
        #     logging.info(f'{person["name_ru"]}: ADD...')
        #     person = json.dumps(person, ensure_ascii=False, indent=4)
        #     r = requests.post(url, headers=headers, data=person.encode('utf-8'), verify=False)
        #     logging.info(r.text)
        #     return r.json()
        # else:
        per = {}
        url = self.BASE_URL + 'parsers/api/persons/'
        for key in person:
            if key != 'id':
                if (person[key] != '') or (person[key] != None) or (person[key] != 0):
                    per.update({key:person[key]})
        data = None
        if "photo-link" in per.keys():
            if per["photo-link"][0] != '/':
                data = 'IMAGE'

        if len(per) > 0:
            logging.info(f'{person["name_ru"]}: UPD...')
            try:
                del per['photo-link']
            except:
                pass
            try:
                del per['id']
            except:
                pass
            p_str = json.dumps(per, ensure_ascii=False, indent=4)
            # if (person['name_ru'] == 'Щербаков Иван Александрович') or (person['name_ru'] == "Чукова Валентина Владимировна"):
            #     print(p_str)
            try:
                r = requests.post(url, headers=headers, data=p_str.encode('utf-8'), verify=False)
                resp = {'id':None}

                try:
                    resp = json.loads(r.text)
                except:
                    logging.info("!!!! ERROR 500 !!! ")
                    logging.info(r.text)
                #print(resp)
                if (data != None) and ("photo_link" in resp.keys()):
                    if resp["photo_link"][0] != '/':
                        img_url = per['photo-link']
                        ext = urlparse(img_url).path.split('/')[-1:][0].split('.')[-1:][0]
                        f = open(f'images/avatar.{ext}', 'wb')
                        try:
                            rb = self._get(img_url, True)
                            f.write(rb.content)
                        except:
                            LogException()
                        f.close()
                        f_name = f'avatar.{ext}'
                        f_path = 'images/' + f_name
                        data = {
                            'file': (f_name, open(f_path, 'rb'))
                        }
                        m = MultipartEncoder(data, boundary='WebAppBoundary')
                        headers_img = {
                            'Accept': '*/*',
                            'Connection': 'keep-alive',
                            'Authorization': 'Token 26b881c992c9b4c0f1b9fe13c9a10cf9c1aacbc1',
                            'Content-Type': m.content_type
                        }
                        r_img = requests.post(url + str(resp["id"]) + '/upload_image/', data=m.to_string(), headers=headers_img, verify=False)

                        logging.info(r_img.json()['photo_link'])
                        #logging.info(r.text)
                return resp
            except:
                LogException()

        else:
            logging.info(f'PASS')

    def update_person_from_dict(self, person: dict):
        
        url = self.BASE_URL + 'parsers/api/persons/'
        headers = {
            'Accept': '*/*',
            # 'Accept-Encoding':'gzip, deflate, br',
            # 'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'Authorization': 'Token 26b881c992c9b4c0f1b9fe13c9a10cf9c1aacbc1'
        }
        # f = open(fname, 'r', encoding='utf-8')
        # person = json.loads(f.read())
        # f.close()
        t_c = self.find_person_by_name(person['name_ru'])
        # print(t_c)
        # if t_c['id'] == None:
        #     url = self.BASE_URL + 'parsers/api/persons/'
        #     logging.info(f'{person["name_ru"]}: ADD...')
        #     person = json.dumps(person, ensure_ascii=False, indent=4)
        #     r = requests.post(url, headers=headers, data=person.encode('utf-8'), verify=False)
        #     logging.info(r.text)
        #     return r.json()
        # else:
        per = {}
        url = self.BASE_URL + 'parsers/api/persons/' + str(person['id']) + '/'
        for key in person:
            if key != 'id':
                if (person[key] != '') or (person[key] != None) or (person[key] != 0):
                    per.update({key:person[key]})
        data = None
        if "photo-link" in per.keys():
            if per["photo-link"][0] != '/':
                data = 'IMAGE'

        if len(per) > 0:
            logging.info(f'{person["name_ru"]}: UPD...')
            try:
                del per['photo-link']
            except:
                pass
            p_str = json.dumps(per, ensure_ascii=False, indent=4)
            # if (person['name_ru'] == 'Щербаков Иван Александрович') or (person['name_ru'] == "Чукова Валентина Владимировна"):
            #     print(p_str)
            try:
                r = requests.patch(url, headers=headers, data=p_str.encode('utf-8'), verify=False)
                try:
                    resp = json.loads(r.text)
                except:
                    logging.info("!!!! ERROR 500 !!! ")
                #print(resp)
                if (data != None) and ("photo_link" in resp.keys()):
                    if resp["photo_link"][0] != '/':
                        img_url = per['photo-link']
                        ext = urlparse(img_url).path.split('/')[-1:][0].split('.')[-1:][0]
                        f = open(f'images/avatar.{ext}', 'wb')
                        try:
                            rb = self._get(img_url, True)
                            f.write(rb.content)
                        except:
                            LogException()
                        f.close()
                        f_name = f'avatar.{ext}'
                        f_path = 'images/' + f_name
                        data = {
                            'file': (f_name, open(f_path, 'rb'))
                        }
                        m = MultipartEncoder(data, boundary='WebAppBoundary')
                        headers_img = {
                            'Accept': '*/*',
                            'Connection': 'keep-alive',
                            'Authorization': 'Token 26b881c992c9b4c0f1b9fe13c9a10cf9c1aacbc1',
                            'Content-Type': m.content_type
                        }
                        r_img = requests.post(url + 'upload_image/', data=m.to_string(), headers=headers_img, verify=False)

                        logging.info(r_img.json()['photo_link'])
                        #logging.info(r.text)
                return r.json()
            except:
                LogException()

        else:
            logging.info(f'PASS')

    def upload_person(self, fname: str):
        
        url = self.BASE_URL + 'parsers/api/persons/'
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
            url = self.BASE_URL + 'parsers/api/persons/'
            logging.info(f'{person["name_ru"]}: ADD...')
            person = json.dumps(person, ensure_ascii=False, indent=4)
            r = requests.post(url, headers=headers, data=person.encode('utf-8'), verify=False)
            logging.info(r.text)
            return r.json()
        else:
            url = self.BASE_URL + 'parsers/api/persons/' + str(t_c['id']) + '/'
            for key in t_c:
                if key != 'id':
                    if (t_c[key] != '') or (t_c[key] != None) or (t_c[key] != 0):
                        del person[key]
            if len(person) > 0:
                logging.info(f'{person["name_ru"]}: UPD...')
                person = json.dumps(person, ensure_ascii=False, indent=4)
                r = requests.patch(url, headers=headers, data=person.encode('utf-8'), verify=False)
                logging.info(r.text)
                return r.json()

    def parse_career_connections(self, workbefore):
        
        # logging.info(f'{name_ru}: Parsing career_connections...')
        person = {}
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
            #                company = self.find_company_by_name(jname_name)
            if (jname_link != None) or (jname_link != ''):
                if jname_link.find('https://rupep.org/ru/company/') > -1:
                    d.update({'company-link': jname_link})
            d = {}
            try:
                # ID : 'company'
                d.update({'company-name': jname_name})
                if date_from != None:
                    d.update({'start': date_from.strftime('%Y-%m-%d')})
                if date_to != None:
                    d.update({'stop': date_to.strftime('%Y-%m-%d')})
                if (jname_pos != None) or (jname_pos != ''):
                    d.update({'job_position': jname_pos})
            except:
                pass
            if d != {}:
                hist.append(d)
        # person['career_connections'] = hist
        if len(hist) > 0:
            person.update({'career_connections': hist})
        return person

    def parse_personal_connections(self, connections):
        
        # logging.info(f'{name_ru}: Parsing personal_connections...')
        person = {}
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

                dd.update({'person-lid':str(p_id)})
                #dd.update({'person-link': str(p_url)})
                #dd.update({'person2': rel_person['id']})
                dd.update({'category': conn_type})
                if (p_role != '') or (p_role != None):
                    dd.update({'role': p_role})
                if dd != {}:
                    l.append(dd)
            d.update({conn_type: l})

        # person['person_connections'] = l
        if len(l) > 0:
            person.update({'person_connections': l})
        return person

    def parse_companies_connections(self, companies):
        
        # logging.info(f'{name_ru}: Parsing company_connections...')
        person = {}
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
            d = {}
            cont = place.find('div', {'class': 'tl-content'}).find('div')
            place = cont.find('a', {'itemprop': 'worksFor'}).extract()
            place_name = place.find('span', {'itemprop': 'name'}).text.strip()
            place_link = self.url + place['href']
            try:
                place_tax_id = place.find('span', {'itemprop': 'taxID'}).text.strip()
                d.update({'company-taxid': place_tax_id})
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

            # if (place_link != None) or (place_link != ''):
            #     if place_link.find('https://rupep.org/ru/company/') > -1:
            #         companies_links.append(place_link)

            #rel_company = self.find_company_by_name(place_name)

            #if rel_company['id'] != None:
            #d.update({'company': rel_company['id']})
            d.update({'company-name': place_name})
            d.update({'company-link': place_link})
            if date_from != None:
                d.update({'start': date_from.strftime('%Y-%m-%d')})
            if date_to != None:
                d.update({'stop': date_to.strftime('%Y-%m-%d')})
            if (p_role != '') or (p_role != None):
                d.update({'role': p_role})

            if d != {}:
                hist.append(d)
        #person['company_connections'] = hist
        if len(hist) > 0:
            person.update({'company_connections': hist})
        return person

    def parse_personal(self,personal_trs):
        
        person = {}
        # logging.info(f'{name_ru}: Parsing personsl...')
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
                    pass
                    # LogException()
                    # logging.info(e)
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
                # if last_job['company-link'].find('https://rupep.org/ru/company/') > -1:
                #     companies_links.append(last_job['company-link'])
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
        return person

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
            #logging.info(f'Cant parse {id}: {e}')
            return {}
        #fname = f'persons/{id}.json'
        fname = persons_path / str(id) / 'full_init'
        base_item_path = persons_path / str(id) / 'base_file'
        person = {}

        try:
            ava_url = self.url + profile.find('div', {'class': 'avatar'}).find('img')['src']
            person.update({'photo-link': ava_url})

            ext = urlparse(ava_url).path.split('/')[-1:][0].split('.')[-1:][0]
            ava_file = persons_path / str(id) / f'avatar.{ext}'
            try:
                r = self._get(ava_url)
                with open(ava_file, 'wb') as f:
                    f.write(r.content)
            except:
                pass
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
            person.update(self.parse_personal(personal_trs))
            base_d = person

            with open(base_item_path, 'w', encoding='utf-8') as f:
                f.write(json.dumps(base_d, ensure_ascii=False, indent=4))
        except:
            LogException()

        # -- Карьера
        try:
            workbefore = profile.find('div', {'id': 'workbefore'}).find('ul', {'class': 'timeline'}).find_all('li', {
                'class': 'tl-item'})
            person.update(self.parse_career_connections(workbefore))
        except: pass

        # -- Персональные cвязи
        try:
            connections = profile.find('div', {'id': 'connections'}).find('ul').find('li').find('ul').find_all('li',
                                                                                                               recursive=False)
            person.update(self.parse_personal_connections(connections))
        except: pass

        # -- Связанные юридические лица
        try:
            companies = profile.find('div', {'id': 'related-companies'}).find('ul', {'class': 'timeline'}).find_all(
                'li')
            person.update(self.parse_companies_connections(companies))
        except: pass

        to_json_file(person,fname)

        return person

    def single_threaded_load(self, links, use_proxy=False):
        
        for link in links:
            try:
                d = self.parse_person(link, use_proxy)
                self.current += 1
                logging.info(f'UPDATED {self.current} of {self.total} -=- {d["name_ru"]} - {link}')
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

    def bak_get_companies(self):
        
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

    def get_companies(self):
        files = list(persons_path.rglob('full_init'))
        #print(files)
        companies_list = home_path / 'companies.toparse'
        c_l = []
        for p_file in tqdm(files):
            with open(p_file,'r',encoding='utf-8') as p_strs:
                person = json.loads(p_strs.read())
                p1 = person
                if 'career_connections' in person.keys():
                    comps = []
                    for company in person['career_connections']:
                        if not('company' in company.keys()):
                            if 'company-link' in company.keys():
                                c_l.append(company['company-link'])
                            else:
                                d = {
                                    'name': company['company-name']
                                }
                                headers = {
                                            'Accept': '*/*',

                                            'Content-Type': 'application/json',
                                            'Authorization': 'Token 26b881c992c9b4c0f1b9fe13c9a10cf9c1aacbc1'
                                        }
                                r = requests.post(self.BASE_URL + '/parsers/api/companies/', headers=headers, json=d, verify=False)
                                resp = r.json()
                                try:
                                    company.update({'company':int(resp['id'])})
                                    tqdm.write(str(resp['id']) + ' - ' + company['company-name'])
                                    #print(str(resp['id']) + ' - ' + company['company-name'])
                                except:
                                    tqdm.write(str(resp['name'][0]) + ' - ' + company['company-name'])
                                    #print(str(resp['name'][0]) + ' - ' + company['company-name'])
                                    company.update({'company': int(resp['name'][0])})
                                try:
                                    del company['company-name']
                                except: pass
                        comps.append(company)
                    person['career_connections'] = comps


                if 'company_connections' in person.keys():
                    comps = []
                    for company in person['company_connections']:
                        if not('company' in company.keys()):
                            if 'company-link' in company.keys():
                                c_l.append(company['company-link'])
                            else:
                                d = {
                                    'name': company['company-name']
                                }
                                headers = {
                                            'Accept': '*/*',

                                            'Content-Type': 'application/json',
                                            'Authorization': 'Token 26b881c992c9b4c0f1b9fe13c9a10cf9c1aacbc1'
                                        }
                                r = requests.post(self.BASE_URL + '/parsers/api/companies/', headers=headers, json=d, verify=False)
                                resp = r.json()
                                try:
                                    company.update({'company':int(resp['id'])})
                                    tqdm.write(str(resp['id']) + ' - ' + company['company-name'])
                                    #print(str(resp['id']) + ' - ' + company['company-name'])
                                except:
                                    tqdm.write(str(resp['name'][0]) + ' - ' + company['company-name'])
                                    #print(str(resp['name'][0]) + ' - ' + company['company-name'])
                                    company.update({'company': int(resp['name'][0])})
                                try:
                                    del company['company-name']
                                except: pass
                        comps.append(company)
                    person['career_connections'] = comps

            if person != p1:
                with open(p_file,'w',encoding='utf-8') as f:
                    f.write(json.dumps(person,ensure_ascii=False,indent=4))

        with open(companies_list,'w',encoding='utf-8') as f:
            f.write('\n'.join(c_l))

    def load_companies(self):
        companies_list = home_path / 'companies.toparse'
        with open(companies_list,'r',encoding='utf-8') as f:
            lines = f.read().split('\n')
            lines = list(set(lines))
            for line in tqdm(lines):
                self.parse_company(line)

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
                lines = line.find_all('td')
                if lines[0].text.strip() == 'ОГРН':
                    company.update({'registration_id': line.find_all('td')[1].text.replace('\t', '').replace('\n',
                                                                                                             '').replace(
                        '\r', '').strip()})
                if lines[0].text.strip() == 'Дата создания':
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
                if lines[0].text.strip() == 'Зарегистрирован(-а)':
                    company.update({'country_registration': line.find_all('td')[1].text.replace('\t', '').replace('\n',
                                                                                                                  '').replace(
                        '\r', '').strip()})
                if lines[0].text.strip() == 'Адрес':
                    p_role = line.find_all('td')[1].text.strip()
                    if len(p_role.split('\n')) > 1:
                        buf = ''
                        p_role = p_role.split('\n')
                        for line in p_role:
                            line = line.replace('–', '').replace(',', '').strip()
                            if line != '': buf = buf + ', ' + line
                        p_role = buf.strip(' ,-')
                    else:
                        p_role = p_role.replace('–', '').replace(',', '').strip(' ,-')
                    company.update({'address_company': p_role})
                if len(lines) == 1:
                    try:
                        ws = line.find('td').find('a')
                        if ws.text.strip() == 'Вебсайт':
                            company.update({'website': ws['href']})
                    except:
                        pass

        to_json_file(company,fname_comp)

        return company


def init():
    
    global a
    a = Api()
    # proxies = ['http://GrandMeg:rTd57fsD@188.191.164.19:9004']
    proxies = []
    rf = 'https://3FrkKs:J4vEBR@185.183.160.146:8000'
    usa = 'http://wcQT86:jZ6Z7D@154.30.133.132:8000'
    proxies.append(usa)
    for proxy in proxies:
        a.proxies.append({
            "http": proxy,
            "https": proxy,
            "ftp": proxy
        })
    DEV = False
    if not os.path.isdir("companies"):
        os.mkdir("companies")
    if not os.path.isdir("persons"):
        os.mkdir("persons")
    if not os.path.isdir("images"):
        os.mkdir("images")


def clear_folders(folders: list):
    for folder in folders:
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    os.remove(file_path)
            except Exception as e:
                print('Failed to delete %s. Reason: %s' % (file_path, e))


def go_parse():
    
    global a
    clear_folders(['companies','persons','images'])
    items = a.get_main_data(True)
    links = []
    for item in items:
        try:
            links.append(item['person-link'])
        except:
            logging.info(f'No link...')
    logging.info(f'{links[0]}')
    f = open('kyc_persons.json','r',encoding='utf-8')
    a.kyc_persons = json.loads(f.read())
    f.close()
    a.multi_threaded_load(links, 50, True)


if __name__ == '__main__':
    
    init()
    #go_parse()
    #a.get_companies()
    a.load_companies()
    a.process_uploading_companies(500)
    # a.process_uploading_persons()
