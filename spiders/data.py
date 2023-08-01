import scrapy
from eservices.items import DistrictItem,TalukaItem,VillageItem
import json
import pymssql



class DataSpider(scrapy.Spider):
    name = 'data'
    # allowed_domains = ['https://eservices.tn.gov.in/eservicesnew/land/chittaCheckNewRural_en.html?lan=en']
    # start_urls = ['https://eservices.tn.gov.in/eservicesnew/land/chittaCheckNewRural_en.html?lan=en']


    def __init__(self):
        self.connectDB()

    def connectDB(self):
        self.conn = pymssql.connect(server='mssql.esmsys.in',port=14251, user='interview',password='Interview@123', database='interview')
        self.cursor = self.conn.cursor(as_dict=True)
        self.conn.commit()

    def start_requests(self):
        urls = 'https://eservices.tn.gov.in/eservicesnew/land/ajax.html?page=ruralservice&ser=dist&lang=en'

        params = {'page':'ruralservice',
                    'lang':'en',
                    'ser':'dist'}

        yield scrapy.Request(urls, method='POST',body=json.dumps(params),callback=self.parse_district)

    def parse_district(self,response):

        item = DistrictItem()
        data = json.loads(response.text)

        district = data['landrecords'].get('response')

        if len(district) > 1:
            d_data = district[1:]

            for i in d_data:
                item['id']= i['name']
                item['d_name'] = i['value']

                yield item

                district_id = item['id']
                taluka_url = f"https://eservices.tn.gov.in/eservicesnew/land/ajax.html?page=ruralservice&ser=tlk&distcode={district_id}&lang=en"
                params = {'page':'ruralservice',
                            'ser':'tlk',
                            'distcode':district_id,
                            'lang':'en'}

                yield scrapy.Request(taluka_url,method='POST',body=json.dumps(params) ,callback=self.parse_talukas, meta={"district_id": district_id})

    def parse_talukas(self, response):

        item = TalukaItem()
        id = response.meta["district_id"]
        data = json.loads(response.text)

        taluka = data['landrecords'].get('response')

        if len(taluka) >= 1:
            t_data = taluka[1:]

            for i in t_data:
                item['d_id'] = id
                item['t_id'] = i['name']
                item['t_name'] = i['value']

                yield item

                taluka_id = item['t_id']
                district_id = id

                params = {'page':'ruralservice',
                            'ser':'vill',
                            'distcode':district_id,
                            'talukcode':taluka_id,
                            'lang':'en'}
                village_url = f"https://eservices.tn.gov.in/eservicesnew/land/ajax.html?page=ruralservice&ser=vill&distcode={district_id}&talukcode={taluka_id}&lang=en"
                yield scrapy.Request(village_url,method='POST',body=json.dumps(params) , callback=self.parse_villages, meta={"taluka_id": taluka_id})

    def parse_villages(self, response):
        taluka_id = response.meta["taluka_id"]

        item = VillageItem()
        data = json.loads(response.text)

        village = data['landrecords'].get('response')

        if len(village) >= 1:
            v_data = village[1:]

            for i in v_data:
                item['t_id'] = taluka_id
                item['v_id'] = i['name']
                item['v_name'] = i['value'].strip()

                yield item




# from scrapy.cmdline import execute
# execute("scrapy crawl data".split())
