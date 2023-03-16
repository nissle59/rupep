import logging
import os
import sys
import datetime
import json
from pathlib import *
from urllib.parse import urlparse
import requests
from requests_toolbelt import MultipartEncoder
from tqdm import tqdm
import warnings

# ------- GLOB VARS -----------------

home_path = Path.cwd()
persons_path = home_path / 'persons'
companies_path = home_path / 'companies'

kyc_url = 'https://kycbase.io/parsers/api/'
kyc_companies_api_url = kyc_url + 'companies/'
kyc_companies_api_url_bulk = kyc_companies_api_url + 'bulk/'
kyc_persons_api_url = kyc_url + 'persons/'
kyc_persons_api_url_bulk = kyc_persons_api_url + 'bulk/'

# -----------------------------------

warnings.filterwarnings("ignore")

dtnow = datetime.date.today().strftime('%d_%m_%Y')
logging.basicConfig(level=logging.INFO, filename=f"uploader_{dtnow}.log", filemode="a",
                    format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

headers = {
            'Accept': '*/*',
            'Content-Type': 'application/json',
            'Authorization': 'Token 26b881c992c9b4c0f1b9fe13c9a10cf9c1aacbc1'
        }


def POST(url, json):
    r = requests.post(url, json=json, headers=headers, verify=False)
    try:
        response = r.json()
    except:
        response = None
    return response


def POST_IMG(url, fname):
    data = {
        'file': (fname.name, open(fname, 'rb'))
    }
    m = MultipartEncoder(data, boundary='WebAppBoundary')
    headers_img = {
        'Accept': '*/*',
        'Connection': 'keep-alive',
        'Authorization': 'Token 26b881c992c9b4c0f1b9fe13c9a10cf9c1aacbc1',
        'Content-Type': m.content_type
    }
    r = requests.post(url, data=m.to_string(), headers=headers_img, verify=False)
    #r = requests.post(url, json=json, headers=headers, verify=False)
    try:
        response = r.json()
    except:
        response = None
    return response


def PATCH(url, json):
    r = requests.patch(url, json=json, headers=headers, verify=False)
    try:
        response = r.json()
    except:
        response = None
    return response


def GET(url, params = None):
    if params != None:
        r = requests.get(url, headers=headers, params=params, verify=False)
    else:
        r = requests.get(url, headers=headers, verify=False)
    try:
        #logging.info(r.text)
        response = r.json()
    except:
        response = None
    return response


def to_json(inp_d):
    return json.dumps(inp_d,ensure_ascii=False,indent=4)


def to_json_file(inp_d, filename):
    with open(filename,'w',encoding='utf-8') as f:
        f.write(to_json(inp_d))


def from_json(inp_s):
    return json.loads(inp_s)


def from_json_file(filename):
    with open(filename,'r',encoding='utf-8') as f:
        return from_json(f.read())


def upload_companies(limit=500):
    def upload(l, added, exists):
        response = POST(kyc_companies_api_url_bulk, lst)
        if response:
            for item in response:
                idx = response.index(item)
                if "name" in item.keys():
                    if isinstance(item['name'], str):
                        item_out = item
                        added.append(item_out)
                    else:
                        item_out = lst[idx]
                        item_out.update({'id': item['name'][0]['id']})
                        exists.append(item_out)
                    tqdm.write(f'{item_out["id"]} - {item_out["name"]}')
                else:
                    logging.info(f'ERR {";".join(list(item.keys()))}')
        else:
            tqdm.write('Upload error!')
    files = list(companies_path.rglob('*.json'))
    lst = []
    current = 0
    added = []
    exists = []
    for fname in tqdm(files):
        if current != limit:
            company = from_json_file(fname)
            lst.append(company)
            current += 1
        else:
            upload(lst,added,exists)
            lst = []
            current = 0
    if len(lst) > 0:
        upload(lst,added,exists)

    hist_d = {
        'added' : added,
        'exists' : exists
    }
    logging.info(f'Summary processed {str(len(added)+len(exists))} companies of {str(len(files))}')
    hist_path = home_path / 'companies.history'
    to_json_file(hist_d,hist_path)


def generate_persons_compare_file():
    # ------ LOAD persons from KYC -----

    limit = 1000
    offset = 0
    r_init = GET(kyc_persons_api_url)
    count = int(r_init['count'])
    ran = round(count / limit) + 1
    compare_list = {}
    for i in tqdm(range(0, ran), desc='Loading KYC persons'):
        par = {
            'limit': limit,
            'offset': offset
        }
        r = GET(kyc_persons_api_url, params=par)
        for item in r['results']:
            compare_list.update({
                item['name_ru'] : {
                    'gid':int(item['id'])
                }
            })
        offset += limit-1
    to_json_file(compare_list,'kyc_persons.json')

    # ---------- LOAD local persons -----
    kyc_persons = from_json_file('kyc_persons.json')
    files = list(persons_path.rglob('*/full_init'))
    logging.info(f'{len(files)} local persons found')
    for person in tqdm(files):
        person = Path(person)
        lid = int(person.parts[-2:][0])
        p_dict = from_json_file(person)
        p_name_ru = p_dict['name_ru']
        if p_name_ru in kyc_persons.keys():
            p_dict.update({'id':kyc_persons[p_name_ru]['gid']})
            gidfile = person.parent / 'gid'
            if not gidfile.is_file():
                gidfile.touch()
                gidfile.write_text(str(p_dict['id']))
            kyc_persons[p_name_ru].update({
                'lid':lid
            })
            to_json_file(p_dict,person)
    to_json_file(kyc_persons,'kyc_persons.json')


def load_kyc_companies():
    # ------ LOAD companies from KYC -----

    limit = 1000
    offset = 0
    r_init = GET(kyc_companies_api_url)
    count = int(r_init['count'])
    ran = round(count / limit) + 1
    compare_list = {}
    for i in tqdm(range(0, ran), desc='Loading KYC companies'):
        par = {
            'limit': limit,
            'offset': offset
        }
        r = GET(kyc_companies_api_url, params=par)
        for item in r['results']:
            compare_list.update({
                item['name'].upper(): int(item['id'])
            })
        offset += limit - 1
    to_json_file(compare_list, 'kyc_companies.json')


def process_persons_files(dev = False):
    kyc_persons = from_json_file('kyc_persons.json')
    kyc_companies = from_json_file('kyc_companies.json')
    files = list(persons_path.rglob('*/full_init'))
    comps = list(companies_path.rglob('*.json'))
    comps_lids = [int(pp.stem) for pp in comps]
    pers_count = len(files)
    lids = [int(pp.parts[-2]) for pp in files]
    #print(lids)
    logging.info(f'{len(files)} local persons found')
    count = 1
    for person in tqdm(files):
        if (count == 2) and dev: break
        person = Path(person)
        #lid = int(person.parts[-2:])
        p_dict = from_json_file(person)
        deb = []

        p_res_dict = p_dict
        try:
            car_con_count = len(p_res_dict['career_connections'])
            car_conns = p_res_dict['career_connections']
            #tqdm.write(f'CAR COUNT: {car_con_count}')
            del p_res_dict['career_connections']
        except:
            car_con_count = 0
            car_conns = []
        try:
            per_con_count = len(p_res_dict['person_connections'])
            per_conns = p_res_dict['person_connections']
            #tqdm.write(f'PER COUNT: {per_con_count}')
            del p_res_dict['person_connections']
        except:
            per_con_count = 0
            per_conns = []
        try:
            com_con_count = len(p_res_dict['company_connections'])
            com_conns = p_res_dict['company_connections']
            #tqdm.write(f'COM COUNT: {com_con_count}')
            del p_res_dict['company_connections']
        except:
            com_con_count = 0
            com_conns = []

        init_conns = {
            'career_connections':car_conns,
            'person_connections':per_conns,
            'company_connections':com_conns
        }
        deb.append(init_conns)

        career_connections = []
        person_connections = []
        company_connections = []
        #if 'career_connections' in p_dict.keys():
        for p_con in car_conns:
            if p_con["company-name"].upper() in kyc_companies:
                c_id = kyc_companies[p_con["company-name"].upper()]
                p_con.update({'company':int(c_id)})
                try:
                    del p_con["company-name"]
                except:
                    pass
                #tqdm.write(to_json(p_con))
                career_connections.append(p_con)
            elif p_con["company-name"] in kyc_companies:
                c_id = kyc_companies[p_con["company-name"]]
                p_con.update({'company':int(c_id)})
                try:
                    del p_con["company-name"]
                except:
                    pass
                #tqdm.write(to_json(p_con))
                career_connections.append(p_con)

        #if 'company_connections' in p_dict.keys():
        for p_con in com_conns:
            if p_con["company-name"].upper() in kyc_companies:
                c_id = kyc_companies[p_con["company-name"].upper()]
                p_con.update({'company':int(c_id)})
                try:
                    del p_con["company-name"]
                except:
                    pass
                try:
                    del p_con["company-taxid"]
                except:
                    pass
                try:
                    del p_con["company-link"]
                except:
                    pass
                #tqdm.write(to_json(p_con))
                company_connections.append(p_con)
            elif p_con["company-name"] in kyc_companies:
                c_id = kyc_companies[p_con["company-name"]]
                p_con.update({'company':int(c_id)})
                try:
                    del p_con["company-name"]
                except:
                    pass
                try:
                    del p_con["company-taxid"]
                except:
                    pass
                try:
                    del p_con["company-link"]
                except:
                    pass
                #tqdm.write(to_json(p_con))
                company_connections.append(p_con)
            elif 'company-link' in p_con.keys():
                if p_con["company-link"].find('rupep') > -1:
                    company_lid = int(urlparse(p_con["company-link"]).path.split('/')[-1:][0])
                    if company_lid in comps_lids:
                        js_fname = str(company_lid) + '.json'
                        comp_name = from_json_file(companies_path / js_fname)['name']
                        if comp_name.upper() in kyc_companies:
                            c_id = kyc_companies[comp_name.upper()]
                            p_con.update({'company': int(c_id)})
                            try:
                                del p_con["company-name"]
                            except:
                                pass
                            try:
                                del p_con["company-taxid"]
                            except:
                                pass
                            try:
                                del p_con["company-link"]
                            except:
                                pass
                            # tqdm.write(to_json(p_con))
                            company_connections.append(p_con)
                        elif comp_name in kyc_companies:
                            c_id = kyc_companies[comp_name]
                            p_con.update({'company': int(c_id)})
                            try:
                                del p_con["company-name"]
                            except:
                                pass
                            try:
                                del p_con["company-taxid"]
                            except:
                                pass
                            try:
                                del p_con["company-link"]
                            except:
                                pass
                            # tqdm.write(to_json(p_con))
                            company_connections.append(p_con)


        #if 'person_connections' in p_dict.keys():
        for p_con in per_conns:
            if int(p_con["person-lid"]) in lids:
                p_rel = from_json_file(persons_path / str(p_con["person-lid"]) / 'base_file')
                p_name_ru = p_rel['name_ru']
                if p_name_ru in kyc_persons.keys():
                    if 'gid' in kyc_persons[p_name_ru].keys():
                        p_id = kyc_persons[p_name_ru]['gid']
                        p_con.update({'person2':int(p_id)})
                        try:
                            del p_con["person-lid"]
                        except Exception as e:
                            pass
                            #tqdm.write(e)
                        #tqdm.write(to_json(p_con))
                        person_connections.append(p_con)
                else:
                    pass
                    #tqdm.write(f'{p_name_ru} not in kyc_persons.keys()')
            else:
                per_con_count = per_con_count - 1
                #tqdm.write(f'{p_con["person-lid"]} not in lids')

        if (len(career_connections) == car_con_count) and (len(person_connections) == per_con_count) and (len(company_connections) == com_con_count):
            p_res_dict.update({
                'career_connections': career_connections,
                'person_connections': person_connections,
                'company_connections':company_connections
            })
            out_file = person.parent / 'to_upload.json'
            tqdm.write(f'{p_res_dict["name_ru"]}: Ready to upload!')
            #logging.info()
            to_json_file(p_res_dict,out_file)
        else:
            end_conns = {
                'career_connections': career_connections,
                'person_connections': person_connections,
                'company_connections': company_connections
            }
            st = f'Diff: CAR {car_con_count} -> {len(career_connections)}; PER {per_con_count} -> {len(person_connections)}; CAR {com_con_count} -> {len(company_connections)}; '
            logging.info(st)
            debug_file = Path('debug') / p_res_dict['name_ru']
            debug_file.touch(exist_ok=True)
            deb.append(end_conns)
            deb.append({'res': st})
            debug_file.write_text(to_json(deb),'utf-8')
            deb = []

        count += 1

    files = list(persons_path.rglob('*/to_upload.json'))
    logging.info(f'{len(files)} persons of {pers_count} ready to upload')


def upload_persons_base(limit=500):
    def upload(l, added, exists):
        response = POST(kyc_persons_api_url_bulk, lst)
        if response:
            for item in response:
                idx = response.index(item)
                if "name_ru" in item.keys():
                    if isinstance(item['name_ru'], str):
                        item_out = item
                        added.append(item_out)
                    else:
                        item_out = lst[idx]
                        item_out.update({'id': item['name_ru'][0]['id']})
                        exists.append(item_out)
                    tqdm.write(f'{item_out["id"]} - {item_out["name_ru"]}')
                elif "name_en" in item.keys():
                    if isinstance(item['name_en'], str):
                        item_out = item
                        added.append(item_out)
                    else:
                        item_out = lst[idx]
                        item_out.update({'id': item['name_en'][0]['id']})
                        exists.append(item_out)
                    tqdm.write(f'{item_out["id"]} - {item_out["name_en"]}')
                else:
                    logging.info(f'ERR {";".join(list(item.keys()))}')
        else:
            tqdm.write('Upload error!')
    files = list(persons_path.rglob('*/base_file'))
    lst = []
    current = 0
    added = []
    exists = []
    for fname in tqdm(files):
        if current != limit:
            company = from_json_file(fname)
            lst.append(company)
            current += 1
        else:
            upload(lst,added,exists)
            lst = []
            current = 0
    if len(lst) > 0:
        upload(lst,added,exists)

    hist_d = {
        'added' : added,
        'exists' : exists
    }
    logging.info(f'Summary processed: {str(len(added)+len(exists))} of {str(len(files))}')
    hist_path = home_path / 'persons.history'
    to_json_file(hist_d,hist_path)


def upload_avatars():
    files = list(persons_path.rglob('*/avatar*'))
    count = len(files)
    logging.info(f'Found {count} avatars for persons')
    for fname in tqdm(files):
        fname = Path(fname)
        gidfile = fname.parent / 'gid'
        if gidfile.is_file():
            with open(gidfile,'r') as f:
                gid = f.read()
            person_url = kyc_persons_api_url + str(gid) + '/'
            person_url_image = person_url + 'upload_image/'
            per_data = GET(person_url)
            if (per_data['photo_link'] == None) or (per_data['photo_link'][0] != '/'):
                r = POST_IMG(person_url_image,fname)
                tqdm.write(r['photo_link'])
            else:
                tqdm.write('DO NOT NEED TO UPD: '+per_data['photo_link'])
        else:
            pass
            #tqdm.write('No GID file')


def upload_persons_full(limit=10):
    def upload():
        response = PATCH(kyc_persons_api_url_bulk, lst)
        if response:
            for item in response:
                out.append(item)
        else:
            tqdm.write('Upload error!')
    logging.info('Start files walking...')
    files = list(persons_path.rglob('*/to_upload.json'))
    count = len(files)
    logging.info(f'Found {count} persons to upload')
    lst = []
    out = []
    current = 0
    for fname in tqdm(files):
        if current != limit:
            person = from_json_file(fname)
            lst.append(person)
            gidfile = fname.parent / 'gid'
            if not gidfile.is_file:
                with open(gidfile,'w') as f:
                    f.write(str(person['id']))
            current += 1
        else:
            upload()
            lst = []
            current = 0
    if len(lst) > 0:
        upload()
    logging.info(f'Summary processed: {str(len(out))} of {str(len(files))}')



if __name__ == '__main__':
    logging.info(dtnow)
    # upload_companies()
    # upload_persons_base()
    # generate_persons_compare_file()
    # load_kyc_companies()
    # process_persons_files()
    upload_persons_full()
    # upload_avatars()

