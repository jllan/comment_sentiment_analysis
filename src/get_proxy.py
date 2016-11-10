import requests
import requests.adapters
import time
from threading import Thread
from queue import Queue
from bs4 import BeautifulSoup
from .get_hotels_id import ids_remain

proxy_list = Queue()


class ProxyGetting(Thread):
    def __init__(self):
        self.session = requests.session()
        adapter = requests.adapters.HTTPAdapter(max_retries=3)
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)
        Thread.__init__(self)

    def get_xici(self, page=1):
        url = 'http://www.xicidaili.com/nn/{}'.format(page)
        headers_xici = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, sdch',
            'Accept-Language': 'zh-CN,zh;q=0.8',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Host': 'www.xicidaili.com',
            'Upgrade-Insecure-Requests': 1,
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.75 Safari/537.36'
        }
        print('crawl {} page'.format(page))
        response = self.session.get(url, headers=headers_xici)
        soup = BeautifulSoup(response.text, 'lxml')
        ip_list = soup.find('table', id='ip_list').select('tr')[1:]
        for i in ip_list:
            # print(i)
            ip = i.select('td')[1].text.lower()
            port = i.select('td')[2].text
            type = i.select('td')[4].text
            protocol = i.select('td')[5].text.lower()
            print(ip, port, type, protocol)
            data = {'ip': ip, 'port': port, 'protocol': protocol}
            proxies = {}
            proxies[data['protocol']] = data['protocol']+'://'+data['ip']+':'+str(data['port'])
            proxy_list.put(proxies)

    def get_kuaidaili(self, page=1):
        s = requests.session()
        url = 'http://www.kuaidaili.com/free/inha/{}/'.format(page)
        headers_kuaidaili={
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, sdch',
            'Accept-Language': 'zh-CN,zh;q=0.8',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Host': 'www.kuaidaili.com',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.75 Safari/537.36'
        }
        print('crawl {} page'.format(page))
        response = s.get(url, headers=headers_kuaidaili)
        soup = BeautifulSoup(response.text, 'lxml')
        ip_list = soup.table.select('tr')[1:]
        for i in ip_list:
            ip = i.select('td')[0].text.lower()
            port = i.select('td')[1].text
            type = i.select('td')[2].text
            protocol = i.select('td')[3].text.lower()
            print(ip, port, type, protocol)
            data = {'ip': ip, 'port': port, 'protocol': protocol}
            proxies = {}
            proxies[data['protocol']] = data['protocol']+'://'+data['ip']+':'+str(data['port'])
            proxy_list.put(proxies)

    '''从proxy_list中取出一个代理'''
    def get_one_proxy(self):
        if not proxy_list.empty():
            proxy = proxy_list.get()
            return proxy
        else:
            return ''

    def run(self):
        while True:
            if not ids_remain.empty():
                if proxy_list.qsize() < 10:
                    self.get_xici()
                else:
                    time.sleep(10)
            else:
                time.sleep(300)     # 如果待爬取队列空，则暂停300s（因为有些失败的id会重新进入队列）后退出
                break


if __name__ == '__main__':
    proxy_get = ProxyGetting()
    proxy_get.get_xici()
    print(proxy_get.get_one_proxy())
    print(type(proxy_get.get_one_proxy()))
    # proxy_get.run()
