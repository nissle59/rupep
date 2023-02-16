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
        self.database.addMany(items)
        logging.info(f'TOTAL {len(items)} persons')
        return items

    '''
    def _parse_page(self, html):
        soup = BeautifulSoup(html, features="html.parser")
        base = soup.find('div', {'id': 'content'}).find('div', {'class': 'wrap'}).find('div', {'id': 'col-1'})
        links_urls = []
        ls = base.find_all('a', {'class': 'articles_title'})
        for link in ls:
            links_urls.append(self.url + link['href'])
        return links_urls'''

    '''def get_day_links(self, url: object, use_proxy: object = False) -> object:
        links_urls = []
        path = url
        logging.debug(f'--- page #1')
        # print(f'--- page #1')
        r = self._get(path, use_proxy=use_proxy)
        html = r.content.decode('windows-1251')
        soup = BeautifulSoup(html, features="html.parser")
        base = soup.find('div', {'id': 'content'}).find('div', {'class': 'wrap'}).find('div', {'id': 'col-1'})
        pages = base.find('div', {'class': 'pagenate'})
        if pages:
            pages_count = int(pages.text.split(' ')[1].strip('():')) - 1
        else:
            pages_count = 0
        # current_page = 0
        links_urls = self._parse_page(html)
        for page in range(1, pages_count):
            path = url + '?pg=' + str(page)
            logging.debug(f'--- page #{page + 1}')
            # print(f'--- page #{page+1}')
            r = self._get(path, use_proxy=use_proxy)
            html = r.content.decode('windows-1251')
            links_urls += self._parse_page(html)
        return links_urls'''

    def parse_person(self, url, use_proxy=False):
        # path = self.url + '/articles/' + url
        path = url
        url = urlparse(path).path.split('/')[-1:][0]
        r = self._get(path, use_proxy=use_proxy)
        html = r.content.decode('windows-1251')
        soup = BeautifulSoup(html, features="html.parser")
        base = soup.find('div', {'id': 'content'}).find('div', {'class': 'wrap'}).find('div', {'id': 'col-1'})

        return response


    def single_threaded_load(self, links, fname_js, use_proxy=False):
        d = []
        fl = False
        try:
            f = open(fname_js + '.json', 'r', encoding='utf-8')
            d = json.loads(f.read())
            f.close()
        except Exception as e:
            logging.debug(e)
            # print(e)

        for link in links:
            self.current += 1
            flag = False
            for item in d:
                # print(item['source'])
                # print(link)
                if item['source'] == link:
                    flag = True
                    # print(f'FLAG! {len(d)}')
                    # print(d)
            # print(link)
            if not flag:
                try:
                    resp = self.parse_article(link, use_proxy)
                    fl = False
                except Exception as e:
                    fl = True
                    logging.debug(e)
                    # print(e)
                if not fl:
                    logging.info(
                        f'{fname_js} : {links.index(link)} of {len(links)} - TOTAL: {self.current} of {self.total} -=-=-- {resp["source"]}')
                    # print(f'{fname_js} : {links.index(link)} of {len(links)} - TOTAL: {self.current} of {self.total} -=-=-- {resp["source"]}')
                    d.append(resp)
                    time.sleep(0.5)
                    f = open(fname_js + '.json', 'w', encoding='utf-8')
                    f.write(json.dumps(d, ensure_ascii=False, indent=4))
                    f.close()

    def multi_threaded_load(self, links, fname_base, threads_count, use_proxy=False):
        t_s = []
        tc = threads_count
        logging.info(f'Initial links count:{len(links)}')
        buf = []
        for link in links:
            p = str(urlparse(link).path.split('/')[-1:][0])+'.html'
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
                threading.Thread(target=self.single_threaded_load, args=(l_c[i], fname_base + '-' + str(i), use_proxy,),
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
    items = a.get_main_data(True)
