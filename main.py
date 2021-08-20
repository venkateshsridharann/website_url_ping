import shutil
import os.path
import requests
import time,datetime
from lxml import html
import concurrent.futures

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"}

def url_exact_match(url_input,url_returned):
    
    def strip_(url_):
        url_ = url_.split('/')[2].strip('www.')
        url_ = ".".join(url_.split('.')[:-1])
        return url_
    
    if strip_(url_input)==strip_(url_returned):
        return "True"
    return "False"

def ping_urls(url):
    now = datetime.datetime.now()
    time_stamp = now.strftime("%m/%d/%Y, %H:%M:%S")
    try:
        url = 'http://'+url[2] if url[2][:4]!='http' else url[2]
        redirect = False
        r = requests.get(url, stream=True, headers=headers, timeout=1)
        if r.history :
            redirect = True
        status_explained = requests.status_codes._codes[r.status_code]

        if type(status_explained) is not tuple or len(status_explained)<1:
            return r.url,r.status_code,url_exact_match(url,r.url),'-',str(time_stamp)    
        
        return r.url,r.status_code,url_exact_match(url,r.url),status_explained[0],str(time_stamp)
        
    except :
        return(time_stamp)
        
    
def main(URLS):
    
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for url, ret_url_and_status_code in zip(URLS, executor.map(ping_urls, URLS)):
            if type(ret_url_and_status_code) is tuple:
                with open('.\\data\\output\\website_ping_results.txt','a',encoding='utf8') as wf:
                    wf.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(url[0],url[1],url[2],ret_url_and_status_code[0],ret_url_and_status_code[1],ret_url_and_status_code[3],ret_url_and_status_code[2],ret_url_and_status_code[4]))
                if ret_url_and_status_code[1]>299:
                    with open('.\\data\\output\\website_ping_errors.txt','a',encoding='utf8') as wf:
                        wf.write("{}\t{}\t{}\t{}\n".format(url[0],url[1],url[2],ret_url_and_status_code[-1]))
                        
            else:
                with open('.\\data\\output\\website_ping_errors.txt','a',encoding='utf8') as wf:
                    wf.write("{}\t{}\t{}\t{}\n".format(url[0],url[1],url[2],ret_url_and_status_code))
            

if __name__ == '__main__':
    time1 = datetime.datetime.now()
    if not os.path.isfile('.\\data\\input\\company_id_and_websites_input.txt'):
        shutil.copy('.\\data\\company_id_and_websites_download.txt','.\\data\\input\\company_id_and_websites_input.txt')

    with open('.\\data\\input\\company_id_and_websites_input.txt','r',encoding='utf8') as rf:
        id_names_websites = rf.read()
        id_names_websites = [x.split('\t') for x in id_names_websites.split('\n')]

    # testing
    id_names_websites = id_names_websites[:2000]

    for i in range(0, len(id_names_websites),100):        
        print(str(int(100*(i/len(id_names_websites))))+'%')
        main(id_names_websites[i:i+100])
        
    time2 = datetime.datetime.now()
    time_taken = time2-time1
    print(time_taken)
