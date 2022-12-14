import re
import shutil
import os.path
import requests
import unidecode
import tldextract
import time,datetime
from lxml import html
import concurrent.futures
from bs4 import BeautifulSoup
from lxml.html import fromstring

# import argparse
# parser = argparse.ArgumentParser()                                               
# parser.add_argument("--file", "-f", type=str, required=True)
# args = parser.parse_args()

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"}

def cleanhtml(raw_html):
    # removes unwanted tags
  cleanr = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});') 
#   removes unwanted newline tags
  clean_new_line = re.compile('\n')
  cleantext = re.sub(cleanr, '', raw_html)
  cleantext = re.sub(clean_new_line, '', cleantext)
  cleantext = cleantext.replace("[&#8230;]","")
  cleantext = cleantext.replace(",","")
  cleantext = unidecode.unidecode(cleantext)
  return cleantext

def url_exact_match(url_input,url_returned):
    
    def strip_(url_):
        url_ = url_.split('/')[2].strip('www.')
        url_ = ".".join(url_.split('.')[:-1])
        return url_
    
    if strip_(url_input)==strip_(url_returned):
        return "True"
    return "False"

def url_domain_extract(url_):
    ext = tldextract.extract(url_)
    domain_ = ext.domain
    return domain_


def ping_urls(url):
    now = datetime.datetime.now()
    time_stamp = now.strftime("%m/%d/%Y, %H:%M:%S")

    try:
        
        url = 'http://'+url[2] if url[2][:4]!='http' else url[2]
        redirect = False
        r = requests.get(url, stream=True, headers=headers, timeout=1)
        
        try:
            tree = fromstring(r.content)
            title = str(tree.findtext('.//title'))
            
        except:
            title=''
        try:
            soup = BeautifulSoup(r.text,'lxml')
            description = str(soup.find('head').find('meta',{'name':'description'})['content'])
            description = cleanhtml(description).replace('\n','').replace('\r','').replace('\t',' ')
        except:
            description = ''
        if r.history:
            redirect = True
        status_explained = requests.status_codes._codes[r.status_code]
        if type(status_explained) is not tuple or len(status_explained)<1:
            return r.url,r.status_code,url_exact_match(url,r.url),'-',str(time_stamp)    
        title = title.replace('\n',"").replace('\t',"").replace('\r',"")
        url_domain = url_domain_extract(url)
        ret_url_domain = url_domain_extract(r.url)
        domain_match = url_domain == ret_url_domain
        return r.url,r.status_code,url_exact_match(url,r.url),status_explained[0],str(time_stamp),str(title),str(description),url_domain,ret_url_domain,domain_match
        
    except Exception as e:
        return(time_stamp)
        
    
def main(URLS):
    
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for url, ret_url_and_status_code in zip(URLS, executor.map(ping_urls, URLS)):
            if type(ret_url_and_status_code) is tuple:
                with open('.\\data\\output\\website_ping_dec_remaining.txt','a',encoding='utf8') as wf:
                    wf.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(url[0],url[1],url[2],ret_url_and_status_code[0],ret_url_and_status_code[1],ret_url_and_status_code[3],ret_url_and_status_code[2],ret_url_and_status_code[4],ret_url_and_status_code[5],ret_url_and_status_code[6],ret_url_and_status_code[7],ret_url_and_status_code[8],ret_url_and_status_code[9]))
                    
                # based on multiple runs the observed that adding this url to error 
                # file and reruning these urls results in the same errorcode again 
                # if ret_url_and_status_code[1]>299:
                #     with open('.\\data\\output\\website_ping_errors.txt','a',encoding='utf8') as wf:
                #         wf.write("{}\t{}\t{}\t{}\n".format(url[0],url[1],url[2],ret_url_and_status_code[-1]))
                        
            else:
                with open('.\\data\\output\\website_ping_dec_remaining_errors.txt','a',encoding='utf8') as wf:
                    wf.write("{}\t{}\t{}\t{}\n".format(url[0],url[1],url[2],ret_url_and_status_code))
            
            
if __name__ == '__main__':
    time1 = datetime.datetime.now()
    if not os.path.isfile('.\\data\\input\\company_id_and_websites_input.txt'):
        shutil.copy('.\\data\\company_id_and_websites_download.txt','.\\data\\input\\company_id_and_websites_input.txt')

    with open('.\\data\\input\\company_id_and_websites_input.txt','r',encoding='utf8') as rf:
        id_names_websites = rf.read()
        id_names_websites = [x.split('\t') for x in id_names_websites.split('\n')]

    # smaller set for testing
    # id_names_websites = id_names_websites[:]
    
    # currently using full for processing

    for i in range(0, len(id_names_websites),200):        
        print(str(int(100*(i/len(id_names_websites))))+'%')
        main(id_names_websites[i:i+200])
    time2 = datetime.datetime.now()
    time_taken = time2-time1
    print(time_taken)
