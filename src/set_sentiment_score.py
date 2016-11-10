#_*_coding:utf-8 _*_

import time
import datetime
import re
from .config import  comment_basic, comment_detail, log_file
from .sentiment_analysis import SentimentAnalyse
from multiprocessing.dummy import Pool as ThreadPool
from threading import Thread
from .save_comment import ids_got
from .get_hotels_id import ids_remain

pattern_zh = re.compile(u'[\u4e00-\u9fa5]+')


class SentimentScoreSetting(Thread):

    def __init__(self):
        Thread.__init__(self)

    # 为酒店的每条评论做情感值打分
    def set_sentiment_score(self, _id, score):
        comment_detail.update(
            {'_id': _id},
            {'$set': {'sentiment_score': score}},
        )

    # 计算该酒店所有评论情感值的平均分
    def set_sentiment_score_average(self, hotel_id, sum_score, comment_num):
        score_avg = sum_score/comment_num
        # score_avg = float('%.1f' %(score_avg*5))      #把score_avg变换城五分制
        score_avg = round(score_avg*5, 1)               #把score_avg变成五分制并四舍五入保留一位小数
        comment_basic.update(
            {'hotel_id': hotel_id},
            {'$set': {'sentiment_score_avg': score_avg}},
        )

    def get_sentiment_score(self, comment):
        sentiment_analysis = SentimentAnalyse(comment)
        sentiment_score = sentiment_analysis.get_sentiment_score()
        return sentiment_score

    def main(self, hotel_id):
        today = datetime.date.today()
        deadline = str(today - datetime.timedelta(days=180)) #截止日期设为半年前
        # comments = comment_detail.find({'hotel_id':hotel_id, 'comment': {'$regex': '[\u4e00-\u9fa5]'}}).sort('_id')
        result = comment_detail.find({'hotel_id':hotel_id,'comment_date':{'$gte':deadline},'comment_text': {'$regex': u'[\u4e00-\u9fa5]'}})  #查找半年内且没有被设置过情感值的评论
        if result.count():
            sum_score = 0
            for res in result:
                _id, comment = res['_id'], re.sub('\n+', '', res['comment_text'])
                sentiment_score = res.get('sentiment_score')
                if not sentiment_score:
                    sentiment_score = self.get_sentiment_score(comment)
                sum_score += sentiment_score
                # print(sentiment_score, comment)
                self.set_sentiment_score(_id, sentiment_score)
            if result.count() > 10:    #如果抓取到评论数量少于10个，就不进行分析
                self.set_sentiment_score_average(hotel_id, sum_score, result.count())
                with open(log_file, 'a+') as f:
                    f.write('{}共有{}条评论数据，其中{}条数据被设置了情感值，情感值{}\n'.format(hotel_id, comment_detail.count({'hotel_id':hotel_id}), result.count(), sum_score/result.count()))
            else:
                print('{}的评论数量较少，暂不设情感值分'.format(hotel_id))
                with open(log_file, 'a+') as f:
                    f.write('{}的评论数量较少，暂不设情感值\n'.format(hotel_id))
            print('{}共有{}条评论数据，其中{}条数据被设置了情感值，情感值{}'.format(hotel_id, comment_detail.count({'hotel_id':hotel_id}), result.count(), sum_score/result.count()))
        else:
            print('{}未找到符合条件的点评数据！'.format(hotel_id))

    def run(self):
        while True:
            if not ids_got.empty():
                hotel_id = ids_got.get()
                self.main(hotel_id)
            else:
                if not ids_remain.empty():
                    print('队列中暂无数据，分析线程等待中......')
                    time.sleep(300)
                else:
                    break
        print('队列id_got已空，已爬取了的id全部进行情感分析完毕')

def start():
    sentiments = []
    for i in range(2):
        p = SentimentScoreSetting()
        sentiments.append(p)
        p.start()

    for i in sentiments:
        i.join()

if __name__ == '__main__':
    sentiment_setting = SentimentScoreSetting()
    hotel_ids = comment_detail.distinct('hotel_id')     # 已爬取了的酒店id
    print('共有{}个酒店'.format(len(hotel_ids)))
    pool = ThreadPool(processes=4)
    start_time = time.time()
    pool.map(sentiment_setting.main, hotel_ids)
    pool.close()
    pool.join()
    print('队列id_got已空，已爬取了的id全部分析完毕')
    print('用时:', time.time()-start_time)