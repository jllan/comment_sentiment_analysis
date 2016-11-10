import requests
from queue import Queue
from .config import log_file, ids_total_file, ids_got_file, ids_empty_file

ids_remain = Queue()    # 待爬取的id

'''获取hotel_id'''


class HotelIdGetting:
    def __init__(self):
        pass

    '''从服务器获取酒店id'''
    def get_hids_new(self):
        url = ''
        try:
            response = requests.get(url, timeout=300)
        except Exception:
            print('获取酒店id出错')
            with open(log_file, 'a+') as f:
                f.write('获取酒店id出错\n')
        else:
            ids = response.json()['hids']
            ids = set([id['hotelid'].strip() for id in ids])
            with open(ids_total_file, 'r+') as f:    # 所有id
                ids_total = f.readlines()
                ids_total = set(id.strip() for id in ids_total)
                ids_new = ids - ids_total
                if ids_new:
                    for id_new in ids_new:
                        f.write(id_new+'\n')
                print('新增{}个酒店id'.format(len(ids_new)))
                with open(log_file, 'a+') as f:
                    f.write('新增{}个酒店id\n'.format(len(ids_new)))

    def get_hids_remain(self):
        self.get_hids_new()
        with open(ids_total_file, 'r') as f:    # 所有id
            ids_total = f.readlines()
            ids_total = set(id.strip() for id in ids_total)
        with open(ids_got_file, 'r') as f:     # 已经爬取过的id
            ids_got = f.readlines()
            ids_got = set(id.strip() for id in ids_got)
        with open(ids_empty_file, 'r') as f:     # 已经爬取过的id
            ids_empty = f.readlines()
            ids_empty = set(id.strip() for id in ids_empty)
        ids = ids_total - ids_got - ids_empty               # 待爬取的id
        for id in ids:
            ids_remain.put({'hotel_id':id, 'start_page':1})
        with open(log_file, 'a+') as f:
            f.write('共有{}个id，其中{}个已爬取，还有{}个待爬取\n'.format(len(ids_total),len(ids_got),ids_remain.qsize()))
        print('共有{}个id，其中{}个已爬取，还有{}个待爬取\n'.format(len(ids_total),len(ids_got),ids_remain.qsize()))
        return ids_remain

    '''如果全部爬取完毕，需要清空已爬取id数据，以便下次重新开始爬取'''
    def ids_file_del(self):
        print('清空已爬取的id列表和空id列表')
        with open(ids_got_file, 'w') as f:
            f.truncate()
        with open(ids_empty_file, 'w') as f:
            f.truncate()


if __name__ == '__main__':
    get_ids = HotelIdGetting()
    get_ids.get_hids_remain()