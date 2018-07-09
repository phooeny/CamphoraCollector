from spider import EMianWang
from db import * 
import sys
import logging

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%m/%d/%Y %H:%M:%S"

logging.basicConfig(filename='cron_task.log', filemode='a', level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)

def filter_asin_id(asin_id):
	str_id = "%d"%(asin_id);
	str_id = str_id.strip();
	if len(str_id) != 11:
		return False;

def handle_uncrawled_submitted():
	dao = CottonPHDAO();
	spider = EMianWang();
	for asin_id in dao.query_uncrawled_asins():
		asin = spider.crawlDetailInfoByPH("%d"%(asin_id));
		if None == asin:
			logging.error("error in crawling ph: %d"%(asin_id));
			continue;
		asin = filter_asin(asin);
		if None == asin:
			logging.info("asin:%s is filterd by rules."%(asin_id))
			continue;
		asin_item = CottonPH(asin); 
		response = dao.insert_asin(asin_item);
		if 0 == response:
			dao.del_uncrawled_asin(asin_id);
		else:
			logging.error('error in insert ph:%d'%(asin_id));

def scan_factory(year='17'):
	factory_list_file='./factory_list';
	dao = CottonPHDAO();
	spider = EMianWang();
	with open(factory_list_file,'r') as fd:
		for line in fd:
			logging.info(line);
			line = line.strip();
			arr = line.split('\t');
			factory_id, factory_name = arr;
			asin_list = dao.query_asinids_by_factoryid(factory_id, year);
			dic_filter_asin = {};
			for k in asin_list:
				dic_filter_asin[str(k)] = 1;
			asin_list = spider.getPHsByManufactoryID(int(factory_id), [int(year)], dic_filter_asin);
			for asin in asin_list:
				asin = filter_asin(asin);
				asin_item = CottonPH(asin);
				response = dao.insert_asin(asin_item);
				if 0 != response:
					logging.error('error in insert asin \n%s'%(json.dumps(asin_id)));
				else:
					logging.info("scan success: %s"%(asin['ph']));

if __name__ == '__main__':
	handle_uncrawled_submitted();
	scan_factory();
