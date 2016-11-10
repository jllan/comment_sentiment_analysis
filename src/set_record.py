import time
import hashlib
from .config import comment_batch, log_file
from .save_comment import ids_empty

class RecordSetting:
    def __init__(self):
        pass

    def set_record(self, start_time_stamp, end_time_stamp):
        record = {
            'orderid': hashlib.md5(str(start_time_stamp).encode('utf8')).hexdigest(),   # 对开始时间进行的md5加密
            'tag': 1,
            'inserttime': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time_stamp)),
            'type': 'S',
            'ispull': 0,
            'endtime': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time_stamp)),
            'day': time.strftime('%Y-%m-%d', time.localtime(start_time_stamp))
        }
        comment_batch.insert_one(record)
        print('所有酒店爬取完毕，用时：', end_time_stamp-start_time_stamp)
        with open(log_file, 'a+') as f:
            f.write('所有酒店爬取完毕，用时：{}\n'.format(str(end_time_stamp-start_time_stamp)))
            f.write('找不到点评数据的酒店:{}\n'.format(ids_empty))