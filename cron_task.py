from spider import EMianWang
from db import * 
import sys
import pdb

if __name__ == '__main__':
	dao = CottonPHDAO();
	spider = EMianWang();
	for asin_id in dao.query_uncrawled_asins():
		asin = spider.getDetailInfoByPH("%d"%(asin_id));
		if None == asin:
			print("error in crawling ph: %d"%(asin_id));
			continue;
		asin_item = CottonPH(asin); 
		pdb.set_trace();
		response = dao.insert_asin(asin_item);
		if 0 == response:
			dao.del_uncrawled_asin(asin_id);
		else:
			print('error in insert ph:%d'%(asin_id));
