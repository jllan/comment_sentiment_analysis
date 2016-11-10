#_*_coding:utf-8 _*_
import datetime
from pymongo import MongoClient

client = MongoClient('localhost',27017)
ctrip_comment = client['ctrip_comment']

'''单条点评数据，包括hotel_id，comment_id，comment_dat，comment_text，score, sentiment_score'''
comment_detail = ctrip_comment['comment_detail']

'''一个酒店点评数据的概要信息，包括hotel_id,comment_num,available_comment_num,score,recommend_rate,sentiment_score,deadline'''
comment_basic = ctrip_comment['comment_basic']

'''批次记录'''
comment_batch = ctrip_comment['comment_batch']
# comment_batch = client['ctrip_0811']['orderlist']

log_file = 'log/log_{}.txt'.format(str(datetime.date.today()))
ids_total_file = 'hotel_ids/ids_total.txt'      # 全部id
ids_got_file = 'hotel_ids/ids_got.txt'          # 已爬取成功的id
ids_empty_file = 'hotel_ids/ids_empty.txt'      # 没有点评数据的id