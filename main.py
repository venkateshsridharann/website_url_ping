import shutil
import os.path
import requests
import time,datetime
from lxml import html
import concurrent.futures

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"}

def ping_urls(url):
    url = 'http://'+url[2] if url[2][:4]!='http' else url[2]
    try:
        redirect = False
        r = requests.get(url, stream=True, headers=headers, timeout=1)
        if r.history :
            redirect = True
        return r.url,r.status_code,str(r.history)
    except requests.exceptions.ConnectTimeout as e:  
        return('connection timeout')
    except requests.exceptions.ReadTimeout as e:  
        return('read timeout')
    except requests.exceptions.ConnectionError as e:
        return('connection error')
        
    
def main(URLS):
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for url, ret_url_and_status_code in zip(URLS, executor.map(ping_urls, URLS)):
            if type(ret_url_and_status_code) is tuple:
                with open('.\\data\\output\\website_ping_results.txt','a',encoding='utf8') as wf:
                    wf.write("{}\t{}\t{}\t{}\t{}\t{}\n".format(url[0],url[1],url[2],ret_url_and_status_code[0],ret_url_and_status_code[1],ret_url_and_status_code[2]))
            else:
                with open('.\\data\\input\\website_ping_errors.txt','a',encoding='utf8') as wf:
                    wf.write("{}\t{}\t{}\n".format(url[0],url[1],url[2]))
            

if __name__ == '__main__':
    
    if not os.path.isfile('.\\data\\input\\company_id_and_websites_input.txt'):
        shutil.copy('.\\data\\company_id_and_websites_download.txt','.\\data\\input\\company_id_and_websites_input.txt')

    with open('.\\data\\input\\company_id_and_websites_input.txt','r',encoding='utf8') as rf:
        id_names_websites = rf.read()
        id_names_websites = [x.split('\t') for x in id_names_websites.split('\n')]

    # URLS = id_names_websites[:500]
    id_names_websites = id_names_websites[:500]

    for i in range(0, len(id_names_websites),200):
        main(id_names_websites[i:i+200])
    
    
