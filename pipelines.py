from datetime import datetime
from scrapy.utils.log import configure_logging
from eservices.items import DistrictItem, TalukaItem, VillageItem


class EservicesPipeline(object):
    now = datetime.now()
    cur_date_time = now.strftime("%m/%d/%Y, %H:%M:%S")

    def __init__(self):

        self.district_items = []
        self.taluka_items = []
        self.village_items = []

    def process_item(self, item, spider):
        if isinstance(item, DistrictItem):
            district_values = tuple(item.values())
            district_key = item["id"]
            if district_key not in self.district_items:
                self.district_items.append(district_values)
                self.query_d = "INSERT INTO %s ( %s ) VALUES ( %s )" % ("district", "id,d_name", '%s,%s')

        elif isinstance(item, TalukaItem):
            taluka_values = tuple(item.values())
            taluka_key = item["t_id"]
            if taluka_key not in self.taluka_items:
                self.taluka_items.append(taluka_values)
                self.query_t = "INSERT INTO %s ( %s ) VALUES ( %s )" % ("taluka", "d_id,t_id,t_name",'%s,%s,%s')

        elif isinstance(item, VillageItem):
            village_values = tuple(item.values())
            village_key = item["v_id"]
            if village_key not in self.village_items:
                self.village_items.append(village_values)
                self.query_v = "INSERT INTO %s ( %s ) VALUES ( %s )" % ("village", "t_id,v_id,v_name", '%s,%s,%s')

        if len(self.district_items) > 0:
            try:
                spider.cursor.executemany(self.query_d, self.district_items)
                self.district_items = []
                print(self.cur_date_time, "Inserted District Items are ==>", item["id"], item["d_name"])

            except Exception as e:
                if 'MsSQL server has gone away' in str(e):
                    configure_logging(install_root_handler=False)
                    spider.connectDB()
                    spider.cursor.executemany(self.query_d, self.district_items)
                    self.district_items = []
                else:
                    raise e

        if len(self.taluka_items) > 0:
            try:
                spider.cursor.executemany(self.query_t, self.taluka_items)
                self.taluka_items = []
                print(self.cur_date_time, "Inserted Taluka Items are ==>",item["d_id"], item["t_id"], item["t_name"])

            except Exception as e:
                if 'MsSQL server has gone away' in str(e):
                    configure_logging(install_root_handler=False)
                    spider.connectDB()
                    spider.cursor.executemany(self.query_t, self.taluka_items)
                    self.taluka_items = []
                else:
                    raise e

        if len(self.village_items) > 0:
            try:
                spider.cursor.executemany(self.query_v, self.village_items)
                self.village_items = []
                print(self.cur_date_time, "Inserted Village Items are ==>", item["t_id"], item["v_id"], item["v_name"])

            except Exception as e:
                if 'MsSQL server has gone away' in str(e):
                    configure_logging(install_root_handler=False)
                    spider.connectDB()
                    spider.cursor.executemany(self.query_v, self.village_items)
                    self.village_items = []
                else:
                    raise e

        return item

def close_spider(self, spider):
    try:
        spider.cursor.executemany(self.query_d, self.district_items)
        spider.cursor.executemany(self.query_t, self.taluka_items)
        spider.cursor.executemany(self.query_v, self.village_items)
        self.district_items = []
        self.taluka_items = []
        self.village_items = []
    except Exception as e:
        if 'MSSQL server has gone away' in str(e):
            spider.connectDB()
            spider.cursor.executemany(self.query_d, self.district_items)
            spider.cursor.executemany(self.query_t, self.taluka_items)
            spider.cursor.executemany(self.query_v, self.village_items)
            self.district_items = []
            self.taluka_items = []
            self.village_items = []
        else:
            raise e
    spider.conn.close()

