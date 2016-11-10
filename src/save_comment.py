import requests
import requests.adapters
import json
import datetime
import time
import pymongo
from tqdm import *
from threading import Thread
from queue import Queue
from .config import comment_basic, comment_detail, log_file, ids_got_file, ids_empty_file
from .get_proxy import ProxyGetting, proxy_list
from .get_hotels_id import ids_remain

MAX_RETRIES = 3
ids_empty = set()   # 没有点评数据的id
ids_got = Queue()   # 爬取成功的id


class CtripComment(Thread):

    def __init__(self, hotel_id=''):
        Thread.__init__(self)
        self.hotel_id = hotel_id
        self.start_page = 1
        # self.deadline = ''
        # self.proxy = proxy_list.get()
        self.proxy = ProxyGetting().get_one_proxy()
        # self.proxy = ''
        self.session = requests.session()
        adapter = requests.adapters.HTTPAdapter(max_retries=MAX_RETRIES)
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)

    def get_data(self, page, try_num=1):
        url = "http://m.ctrip.com/restapi/soa2/10935/hotel/booking/commentgroupsearch?_fxpcqlniredt=09031020210316541274"
        headers = {
            'Host': 'm.ctrip.com',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:47.0) Gecko/20100101 Firefox/47.0',
            'Accept': 'application/json',
            'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate, br',
            'X-Requested-With': 'XMLHttpRequest',
            'Content-Type': 'application/json',
            'Referer': 'http://m.ctrip.com/webapp/hotel/hoteldetail/dianping/{}.html?roomtype=&opr=&fr=detail&daylater=0&days=1'.format(self.hotel_id),
            'Connection': 'keep-alive'
        }
        # params = {"flag":1,"id":self.hotel_id,"htype":1,"sort":{"idx":1,"size":10,"sort":1,"ord":1},"search":{"kword":"","gtype":4,"opr":0,"ctrl":14,"filters":[]},"alliance":{"aid":"66672","sid":"508668","ouid":"","ishybrid":0},"Key":"","head":{"cid":"09031020210316541274","ctok":"","cver":"1.0","lang":"01","sid":"55552328","syscode":"09","auth":"","extension":[{"name":"pageid","value":"228032"},{"name":"webp","value":0},{"name":"referrer","value":""},{"name":"protocal","value":"http"}]},"contentType":"json"}
        params = {"flag":1,"id":self.hotel_id,"htype":1,"sort":{"idx":1,"size":10,"sort":1,"ord":1},"search":{"kword":"","gtype":4,"opr":0,"ctrl":14,"filters":[]},"alliance":{"aid":"66672","sid":"508668","ouid":"","ishybrid":0},"Key":"f83e59228064b4ede9b33bc4325eb3d9","head":{"cid":"09031020210316541274","ctok":"","cver":"1.0","lang":"01","sid":"8888","syscode":"09","auth":"","extension":[{"name":"pageid","value":"228032"},{"name":"webp","value":0},{"name":"referrer","value":""},{"name":"protocal","value":"https"}]},"contentType":"json"}
        print('使用代理：', self.proxy)
        params['sort']['idx'] = page   # 设置页码
        try:
            response = self.session.post(url, data=json.dumps(params), headers=headers, proxies=self.proxy, timeout=60, stream=False)
            print(response.text)
            print(response.status_code)
        except Exception as e:
            print('error:', e)
            self.handle_error(page)
            return None
        else:
            if response.status_code != 200:
                self.handle_error(page)
                return None
            response = response.json()
            if response['rc'] == 200:
                if not response.get('hcsi'):
                    self.handle_error(page)
                    return None
                if response['hcsi']['total'] > '0':
                    return response
                else:
                    print('{}没有评论数据'.format(self.hotel_id))
                    ids_empty.add(self.hotel_id)
                    with open(ids_empty_file, 'a+') as f:
                        f.write(self.hotel_id+'\n')
                    with open(log_file, 'a+') as f:
                        f.write('{}没有评论数据，当前使用代理{}\n'.format(self.hotel_id, self.proxy))
                    return None

    def get_comment(self, page=1, try_num=1):
        page = pages = self.start_page
        # comment_list = []
        start_time = time.time()
        while page <= pages:
            response = self.get_data(page)
            if response:
                # if page == 1:
                if page == pages:
                    total_pages = response['groups'][0]['pages']
                    count = response['groups'][0]['count']
                    score = response['hcsi']['avgpts']['all']
                    recommend_rate = response['hcsi']['recmd']
                    pages = total_pages
                    comment_basic.find_one_and_update(
                        {'hotel_id': self.hotel_id},
                        {'$set': {'score':score, 'recommend_rate':recommend_rate, 'comment_total':count}},
                        upsert = True
                    )    # 酒店的评论概要信息写入comment_basic,因为每次爬取时这些信息可能会有更新，所以用find_one_and_update的方法
                    if not comment_detail.find({'hotel_id': self.hotel_id}).count():
                        self.deadline = str(datetime.date.today() - datetime.timedelta(days=180))    # 如果没有对应id的酒店的点评数据，说明该酒店是第一次被抓去，截止日期设为半年前
                    else:
                        self.deadline = comment_detail.find_one(
                            {'hotel_id':self.hotel_id},
                            sort=[('comment_date', pymongo.DESCENDING)]
                        )['comment_date']   # 上次抓取的评论的最新的日期作为本次的截止日期
                    print('deadline:', self.deadline)
                    print('{}共有{}条评论，共{}页，评分{}，推荐率{}'.format(self.hotel_id, count, pages, score, recommend_rate))
                print('{}共有{}条评论，共{}页，当前正在爬取第{}页的数据'.format(self.hotel_id, count, pages, page))
                with open(log_file, 'a+') as f:
                    f.write('{}共有{}条评论，共{}页，当前正在爬取第{}页的数据，当前使用代理{}\n'.format(self.hotel_id, count, pages, page, self.proxy))
                comments = response['groups'][0]['comments']
                comment_date = ''
                for comment in comments:
                    comment_score = comment['rats']['all']
                    comment_date = comment['date'].strip().split(' ')[0]
                    comment_id = self.hotel_id+str(comment['comid'])
                    comment_text = comment['text'].strip()
                    comment_dict = {
                        'hotel_id': self.hotel_id,
                        'comment_id': comment_id,
                        'comment_text': comment_text,
                        'comment_score': comment_score,
                        'comment_date': comment_date
                    }
                    # print(comment_dict)
                    if comment_date > self.deadline:
                        if comment_detail.find({'comment_id':comment_id}).count():
                            print('该条评论已存在')
                        else:
                            # comment_list.append(comment_dict)
                            comment_detail.insert_one(comment_dict)
                    else:
                        print(comment_date)
                        print('数据已过期，{}爬取成功！'.format(self.hotel_id))
                        # for comment_dict in comment_list:
                        #     comment_detail.insert_one(comment_dict)
                        ids_got.put(self.hotel_id)  # 成功爬取的id加入队列id_got中
                        with open(ids_got_file, 'a+') as f:
                            f.write(self.hotel_id+'\n')
                        with open(log_file, 'a+') as f:
                            f.write('{}爬取成功！\n'.format(self.hotel_id))
                        break
                if comment_date <= self.deadline:
                    break
                # time.sleep(random.random()*20)
                page += 1
            else:
                print('{}爬取失败'.format(self.hotel_id))
                break
        end_time = time.time()
        print('{}耗时：{}'.format(self.hotel_id, end_time-start_time))

    def handle_error(self, page):
        if page == 1:
            ids_remain.put({'hotel_id':self.hotel_id, 'start_page':page})       # 失败的酒店id重新进入队列id_total
        else:
            ids_remain.put({'hotel_id':self.hotel_id, 'start_page':page, 'deadline':self.deadline})
        # self.proxy = proxy_list.get()
        self.proxy = ProxyGetting().get_one_proxy()

    def run(self):
        start_time = time.time()
        while not ids_remain.empty():
            # self.hotel_id = ids_remain.get()
            record = ids_remain.get()
            self.hotel_id = record['hotel_id']
            self.start_page = record['start_page']
            self.deadline = record.get('deadline')
            ids_remain.task_done()
            # self.proxy = proxy_list_available.get()
            print('开始从第{}页爬取id为{}的酒店,还有{}个酒店待爬取'.format(self.start_page, self.hotel_id, ids_remain.qsize()))
            with open(log_file, 'a+') as f:
                f.write('开始从第{}页爬取id为{}的酒店,还有{}个酒店待爬取\n'.format(self.start_page, self.hotel_id, ids_remain.qsize()))
            self.get_comment()
            print('队列中还有{}个id'.format(ids_remain.qsize()))
        end_time = time.time()
        print('所有酒店爬取完毕，用时：', end_time-start_time)

def print_run_time(func):
    """装饰器函数，输出运行时间"""
    def wrapper(*args, **kw):
        start_time = time.time()
        func()
        print('run time is {:.2f}'.format(time.time() - start_time))
    return wrapper

@print_run_time
def start():
    proxy, comment = [], []

    for j in range(1):
        c = CtripComment()
        comment.append(c)
        c.start()

    for j in comment:
        j.join()


if __name__ == '__main__':
    ctrip_comment = CtripComment()
    ctrip_comment.hotel_id = '429541'
    ctrip_comment.get_comment()
    print(ids_empty)