#_*_coding:utf-8 _*_
import time
import datetime
import re
import random
import codecs
import pymongo
from tqdm import *
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.proxy import *
from bs4 import BeautifulSoup
from threading import Thread
from queue import Queue
from .config import comment_basic, comment_detail, log_file, ids_got_file, ids_empty_file
from .get_proxy import ProxyGetting, proxy_list
from .get_hotels_id import ids_remain

ids_empty = set()   # 没有点评数据的id
ids_got = Queue()   # 成功爬取的id
n = 0
# 从携程获取评论然后保存到数据库或文件
class CtripComment(Thread):

    def __init__(self, hotel_id=''):
        self.hotel_id = hotel_id
        self.start_page = 1
        self.proxy = ProxyGetting().get_one_proxy()
        Thread.__init__(self)

    # 抓取一个酒店半年内的评论，不区分好评差评，存储到‘hotel_comment_酒店id’中，目的主要是用来进行测试
    def save_comments_all_pages(self, page=1):
        start = time.time()
        dcap = dict(DesiredCapabilities.PHANTOMJS)
        dcap["phantomjs.page.settings.userAgent"] = (
            "Mozilla/5.0 (Linux; Android 5.1.1; Nexus 6 Build/LYZ28E) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.23 Mobile Safari/537.36"
        )
        print('使用代理：', self.proxy)
        p = ''.join(self.proxy.values()).split('/')[-1]
        proxy = Proxy({
            'proxyType': ProxyType.MANUAL,
            'httpProxy': p,
            'noProxy': ''
        })
        driver = webdriver.Firefox(proxy=proxy)
        # driver = webdriver.PhantomJS()
        driver.set_page_load_timeout(30)
        url = 'http://hotels.ctrip.com/hotel/dianping/{}.html'.format(self.hotel_id)
        try:
            driver.get(url)
            time.sleep(3)
        except Exception as e:
            print('error:', e)
            with open(log_file, 'a+') as f:
                f.write('{}页面打开失败, 留待以后再爬！\n'.format(self.hotel_id))
            ids_remain.put(self.hotel_id)    # 失败的重进进入待爬取队列
            # self.handle_error(page)
        else:
            print(driver.current_url)

            if driver.current_url != url or '此酒店暂无点评' in driver.page_source:
                print('该酒店无点评数据！')
                with open(log_file, 'a+') as f:
                    f.write('{}没有点评数据！\n'.format(self.hotel_id))
                with open(ids_empty_file, 'a+') as f:
                        f.write(self.hotel_id+'\n')
                ids_empty.add(self.hotel_id)
            else:
                if u'您访问的太快了， 休息一下吧。 或者输入验证码继续访问' in driver.page_source:
                    print('访问太快被禁止，暂停五分钟后继续')
                    # for i in tqdm(range(6000)):
                    #     time.sleep(.1)    #进度条每0.1s前进一次，总时间为3000*0.1=300s
                    # driver.get(url)
                    ids_remain.put(self.hotel_id)
                    # self.handle_error(page)
                    # self.save_comments_all_pages(page=page)
                else:
                    try:
                        select = Select(driver.find_element_by_class_name('select_sort'))
                        time.sleep(1)
                        select.select_by_value('1')     # 下拉框选择按时间排序
                        time.sleep(3)
                    except Exception as e:
                        print('error:', e)
                        ids_remain.put(self.hotel_id)
                        # self.handle_error(page)
                        with open(log_file, 'a+') as f:
                            f.write('{}页面的点评数据无法按照时间排序，留待以后再爬！\n'.format(self.hotel_id))
                    else:
                        last_page_comment = ''
                        while True:
                            soup = BeautifulSoup(driver.page_source)
                            if page == 1:
                                score = soup.find('div',class_="comment_total_score").find('span', class_='score').span.text.strip()             #评分
                                recommend_rate = soup.find('span', class_='rec').span.text.strip('%')   #推荐度
                                comment_total_tmp = soup.find('span', id='All_Comment').text.strip()    #评论数量
                                comment_total = re.search('\d+', comment_total_tmp).group(0)
                                print('ID为{}的酒店共有{}条评论，评分{}，推荐率{}'.format(self.hotel_id, comment_total, score, recommend_rate))
                                comment_basic.find_one_and_update(
                                    {'hotel_id': self.hotel_id},
                                    {'$set': {'score':score, 'recommend_rate':recommend_rate, 'comment_total':comment_total}},
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
                            print('正在爬取{}第{}页的数据'.format(self.hotel_id, page))
                            with open(log_file, 'a+') as f:
                                f.write('正在爬取{}第{}页的数据\n'.format(self.hotel_id, page))
                            comments = soup.findAll('div', {'class':'comment_block J_asyncCmt'})
                            comment_date = ''
                            if comments == last_page_comment:
                                print('和上一页数据相同，不进行解析！')
                            else:
                                last_page_comment = comments
                                comment_num = len(comments)
                                print('正在获取ID为{}的酒店第{}页评论数据，该页有{}条评论'.format(self.hotel_id, page, comment_num))
                                for i in comments:
                                    comment_text = i.find('div', class_="J_commentDetail").text.replace('\n', ' ')   #多行评论转为一行
                                    comment_date = i.select('span[class="time"]')[0].text
                                    comment_date = re.sub("[\u4e00-\u9fa5()]+", '', comment_date)
                                    try:
                                        comment_score = i.select('span[class="score"]')[0].span.text.strip()
                                    except Exception:
                                        comment_score = ''
                                    if comment_date > self.deadline:     # 只需要大于截止日期的数据
                                        comment_dict = {
                                            'hotel_id': self.hotel_id,
                                            # 'comment_id': comment_id,
                                            'comment_text': comment_text,
                                            'comment_score': comment_score,
                                            'comment_date': comment_date
                                        }
                                        print(comment_dict)
                                        comment_detail.insert_one(comment_dict)
                                    else:
                                        print(comment_date, comment_text)
                                        print('评论已过期！')
                                        ids_got.put(self.hotel_id)  # 成功爬取的id加入队列id_got中
                                        with open(log_file, 'a+') as f:
                                            f.write('{}爬取成功！\n'.format(self.hotel_id))
                                        with open(ids_got_file, 'a+') as f:
                                            f.write(self.hotel_id+'\n')
                                        break           #首先结束for循环
                            if comment_date <= self.deadline:
                                break
                            '''查找下一页'''
                            try:
                                next = driver.find_element_by_class_name('c_down')
                            except Exception as e:
                                print('error:',e)
                                ids_got.put(self.hotel_id)  # 成功爬取的id加入队列id_got中
                                with open(log_file, 'a+') as f:
                                    f.write('{}爬取成功！\n'.format(self.hotel_id))
                                with open(ids_got_file, 'a+') as f:
                                    f.write(self.hotel_id+'\n')
                                break
                            else:
                                next_page = soup.find('a', class_='c_down')['value']
                                if page == next_page:   #如果点击了下一页但是没有生效，再次点击下一页，等待时间长一点，以便能顺利进入下一页
                                    print('没有成功进入{}页，当前仍处于{}页'.format(page, int(page)-1))
                                    try:
                                        next.click()
                                    except Exception as e:
                                        print('error:', e)
                                        comment_detail.remove({'comment_date':{'$gt':self.deadline}})    # 进入下一页失败，该酒店爬取失败，清空本次已爬到的数据，留待以后再爬
                                        ids_remain.put(self.hotel_id)
                                        break
                                else:
                                    page = next_page
                                    try:
                                        next.click()
                                    except Exception as e:
                                        print('error:', e)
                                        comment_detail.remove({'comment_date':{'$gt':self.deadline}})    # 进入下一页失败，该酒店爬取失败，清空本次已爬到的数据，留待以后再爬
                                        ids_remain.put(self.hotel_id)
                                        break
                            time.sleep(5)

        driver.quit()
        print('ID为%s的酒店总耗时：'%self.hotel_id, time.time()-start)

    '''def handle_error(self, page):
        if page == 1:
            ids_remain.put({'hotel_id':self.hotel_id, 'start_page':page})       # 失败的酒店id重新进入队列id_total
        else:
            ids_remain.put({'hotel_id':self.hotel_id, 'start_page':page, 'deadline':self.deadline})
        self.proxy = ProxyGetting().get_one_proxy()'''

    def run(self):
        global n
        # dcap = dict(DesiredCapabilities.PHANTOMJS)
        # dcap["phantomjs.page.settings.userAgent"] = (
        #     "Mozilla/5.0 (Linux; Android 5.1.1; Nexus 6 Build/LYZ28E) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.23 Mobile Safari/537.36"
        # )
        # # driver = webdriver.PhantomJS()
        # driver = webdriver.Firefox()
        # driver.set_page_load_timeout(30)
        start_time_stamp = time.time()
        while not ids_remain.empty():
            n += 1
            # self.hotel_id = ids_remain.get()
            record = ids_remain.get()
            self.hotel_id = record['hotel_id']
            # self.start_page = record['start_page']
            # self.deadline = record.get('deadline')

            ids_remain.task_done()
            # self.proxy = proxy_list_available.get()
            print('开始爬取第{}个酒店，id为{},还有{}个酒店待爬取'.format(n, self.hotel_id, ids_remain.qsize()))
            with open(log_file, 'a+') as f:
                f.write('开始爬取id为{}的酒店,还有{}个酒店待爬取\n'.format(self.hotel_id, ids_remain.qsize()))
            self.save_comments_all_pages()
            if n%50 == 0:
                # driver.quit()
                time.sleep(300)
            else:
                time.sleep(random.random()*10)
        # driver.quit()
        end_time_stamp = time.time()
        print('所有酒店爬取完毕，用时：', end_time_stamp-start_time_stamp)

def print_run_time(func):
    """装饰器函数，输出运行时间"""
    def wrapper(*args, **kw):
        start_time = time.time()
        func()
        print('run time is {:.2f}'.format(time.time() - start_time))
    return wrapper

@print_run_time
def start():
    producers, consumers = [], []
    for i in range(2):
        p = CtripComment()
        producers.append(p)
        p.start()
        time.sleep(10)

    for i in producers:
        i.join()


if __name__ == '__main__':
    ctrip_comment = CtripComment()
    ctrip_comment.save_comments_all_pages()
    print(ids_empty)