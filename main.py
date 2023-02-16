import configparser
import json
from pysondb import db
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
    #img_storage = 'pages/images/'
    #remote_img_storage = 'images/'
    proxies = []
    archive_date_start = None
    archive_date_end = None
    _proxy_iter = 0
    archive_links = []
    total = 0
    current = 0
    dbfile = ''
    database = None

    def __init__(self, dbfile = 'db.json'):
        self.dbfile = dbfile
        self.database = db.getDb(self.dbfile)

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
            last_job = tds[4].text.strip().replace('\t','').split('\n')
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
                    #logging.info(f'{str(last_job)}')
            fio_link = self.url + fio_td.find('a')['href']
            fio_ru = fio_td.find('a').extract().text
            try:
                fio_others = fio_td.find('small').extract().text.strip()
            except:
                fio_others = none_str
            fio_en = fio_td.text.strip().replace('(','').replace(')','')
            id = urlparse(fio_link).path.split('/')[-1:][0]
            item = {
                'person-id':id,
                'fio':{
                    'ru':fio_ru,
                    'en':fio_en,
                    'others':fio_others
                },
                'person-link':fio_link,
                'birthday':birthday,
                'inn':inn,
                'category':category,
                'last-job':{
                    'ru':last_job_ru,
                    'en':last_job_en
                }
            }

            items.append(item)
            logging.info(f'PARSED {id}: {fio_ru} / {fio_en} / {birthday}')
        f = open('persons.json','w', encoding='utf-8')
        f.write(json.dumps(items,ensure_ascii=False,indent=4))
        f.close()
        #self.database.addMany(items)
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
        try:
            avatar = self.url + profile.find('div',{'class':'avatar'}).find('img')['src']
        except:
            avatar = None
        fio = profile.find('header',{'class':'profile-header'}).text.strip(' \n')

        js = {
            'person-id':id,
            'fio':fio,
            'avatar-url':avatar
        }

        f = open(fname,'w',encoding='utf-8')
        f.write(json.dumps(js,ensure_ascii=False,indent=4))
        f.close()

        return js




    def single_threaded_load(self, links, use_proxy=False):
        for link in links:
            try:
                d = self.parse_person(link,use_proxy)
                self.current += 1
                logging.info(f'UPDATED {self.current} of {self.total} -=- {d["fio"]} ({d["person-id"]}) - {link}')
            except Exception as e:
                logging.info(f'{e}')
                pass


    def multi_threaded_load(self, links, threads_count, use_proxy=False):
        t_s = []
        tc = threads_count
        logging.info(f'Initial links count:{len(links)}')
        buf = []
        for link in links:
            p = str(urlparse(link).path.split('/')[-1:][0])+'.json'
            if not os.path.exists('pages/'+p):
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

    def load_html_to_file(self,url,fname='index.html',use_proxy=False):
        r = self._get(url,use_proxy)
        html = r.content.decode('utf-8')
        soup = BeautifulSoup(html,features='html.parser')
        f = open(fname,'w',encoding='utf-8')
        f.write(soup.prettify())
        f.close()

    def config_load(self,fname='config.ini'):
        config = configparser.ConfigParser()
        config.read(fname)
        print(config['proxies']['path'])  # -> "/path/name/"
        config['DEFAULT']['path'] = '/var/shared/'  # update
        config['DEFAULT']['default_message'] = 'Hey! help me!!'  # create

if __name__ == '__main__':
    a = Api()
    #proxies = ['http://GrandMeg:rTd57fsD@188.191.164.19:9004']
    proxies = ['http://s2CLEw:GRH8uA@45.139.171.166:8000']
    for proxy in proxies:
        a.proxies.append({
            "http": proxy,
            "https": proxy,
            "ftp": proxy
        })
    DEV = True

    #logging.info(f'Total links: {len(links)}')
    #a.load_html_to_file('https://rupep.org/ru/persons_list/',use_proxy=True)
    #items = a.get_main_data(True)
    f = open('persons.json','r',encoding='utf-8')
    lines = f.read()
    f.close()
    items = json.loads(lines)
    links = []
    for item in items:
        try:
            links.append(item['person-link'])
        except:
            logging.info(f'No link...')
    logging.info(f'{links[0]}')
    a.multi_threaded_load(links,50,True)

