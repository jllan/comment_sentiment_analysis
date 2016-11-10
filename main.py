import time
from src.get_proxy import ProxyGetting
# from src.save_comment import CtripComment
from src.save_comment_selenium import CtripComment
from src.get_hotels_id import HotelIdGetting
from src.set_sentiment_score import SentimentScoreSetting
from src.set_record import RecordSetting


def start():
    HotelIdGetting().get_hids_remain()
    start_time_stamp = time.time()
    proxy, comment, score = [], [], []
    for i in range(1):
        p = ProxyGetting()
        proxy.append(p)
        print('启动代理线程')
        p.start()

    time.sleep(60)

    for j in range(4):
        c = CtripComment()
        comment.append(c)
        print('启动爬虫线程{}'.format(str(j)))
        c.start()
        time.sleep(5)

    time.sleep(300)
    for k in range(4):
        s = SentimentScoreSetting()
        score.append(s)
        s.start()

    for i in proxy:
        i.join()
        print('代理线程退出')

    for j in comment:
        j.join()
        print('爬虫线程退出')


    for k in score:
        k.join()
        print('分析线程退出')

    end_time_stamp = time.time()
    HotelIdGetting().ids_file_del()  # 程序运行完毕，已爬取所有id，清空已爬取id列表，以便下次重新开始爬取
    RecordSetting().set_record(start_time_stamp, end_time_stamp)    # 插入批次记录

if __name__ == '__main__':
    start()