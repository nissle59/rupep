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


def PATCH(url, json):
    r = requests.patch(url, json=json, headers=headers, verify=False)
    try:
        response = r.json()
    except:
        response = None
    return response


def GET(url, params = {}):
    r = requests.post(url, headers=headers, params=params, verify=False)
    try:
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


def upload_companies(limit=200):
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
                    logging.info(f'{item_out["id"]} - {item_out["name"]}')
        else:
            logging.info('Upload error!')
    files = list(companies_path.rglob('*.json'))
    lst = []
    current = 1
    added = []
    exists = []
    for fname in tqdm(files):
        if current < limit:
            company = from_json_file(fname)
            lst.append(company)
            current += 1
        else:
            upload(lst,added,exists)
            lst = []
            current = 1
    if len(lst) > 0:
        upload(lst,added,exists)

    hist_d = {
        'added' : added,
        'exists' : exists
    }

    to_json_file(hist_d,home_path / 'companies.history')


if __name__ == '__main__':
    upload_companies(200)

