#coding=utf-8
import requests
import json
import time
from lxml import etree
import pdb
import re
import sys
import logging

class Spider(object):
	
	def crawByManufactorys(self, dic_filter_asin):
		factory_list_file='./factory_list';
		fp = open(factory_list_file,'r');
		for line in fp:
			line = line.strip();
			arr = line.split('\t');
			factory_id, factory_name = arr;
			self.getPHsByManufactoryID(int(factory_id), [17], dic_filter_asin);
	
	def getPHsByManufactoryID(self, n_manufactory_id=65069, n_year_list=[17], dic_filter_asin={}):
		ret_asins = [];
		seris_error_threshold = 5;
		sleep_interval = 1;
		sleep_period = 2;
		for year in n_year_list:
			for beltline in range(1,10):
				stage_num = 0;
				seris_error_num = 0;
				end_p_id = 999;
				for p_id in range(1,1000):
					str_ph="%.5d%.2d%.1d%.3d"%(n_manufactory_id, year, beltline, p_id);
					if str_ph in dic_filter_asin:
						seris_error_num = 0;
						continue;
					ret,str_hash_id = self.getHashIDByPH(str_ph);
					logging.info('spider status: %s\t%d'%(str_ph,ret));
					if 0 == ret:
						seris_error_num = 0;
						info_detail = self.getDetailPage(str_hash_id,str_ph);
						if None != info_detail:
							info_detail['ph'] = str_ph;
							ret_asins.append(info_detail);
							#str_json = json.dumps(info_detail);
							#print(str_json);
					else:
						seris_error_num +=1;
						if seris_error_num >= 100:
							end_p_id = p_id;
							break;
					#sys.exit(1);
					#stage_num += 1;
					#if sleep_interval == stage_num:
					#	time.sleep(sleep_period);
					#	stage_num = 0;
				if end_p_id <= seris_error_threshold:
					break;
		return ret_asins;

class EMianCang(Spider):
	
	def __init__(self):
		
		self.url_search='http://www.emiancang.com/work/gjsj/search/getMhPhList.mvc?ph=';
		self.url_detail='http://www.emiancang.com/work/queryQuality/view_productZupi.jsp?gcmgy_mhph=';
		self.http_headers = {
				'Accept': 'application/json, text/javascript, */*; q=0.01',
				'Accept-Encoding': 'gzip, deflate',
				'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
				'Connection': 'keep-alive',
				'Host': 'www.emiancang.com',
				'Referer': 'http://www.emiancang.com',
				'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36',
				'X-Requested-With': 'XMLHttpRequest',
				};

		self.xpath_dic = {
				'color_11':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[9]/td[3]",
				'color_21':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[10]/td[2]",
				'color_31':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[11]/td[2]",
				'color_41':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[12]/td[2]",
				'color_51':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[13]/td[2]",
				'color_12':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[14]/td[2]",
				'color_22':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[15]/td[2]",
				'color_32':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[16]/td[2]",
				'color_13':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[17]/td[2]",
				'color_23':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[18]/td[2]",
				'color_33':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[19]/td[2]",
				'color_14':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[20]/td[2]",
				'color_24':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[21]/td[2]",
				'length_avg':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[8]/td[@class='wuri']",
				'length_32':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[9]/td[@class='wuri']",
				'length_31':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[10]/td[@class='wuri']",
				'length_30':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[11]/td[@class='wuri']",
				'length_29':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[12]/td[@class='wuri']",
				'length_28':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[13]/td[@class='wuri']",
				'length_27':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[14]/td[@class='wuri']",
				'length_26':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[15]/td[@class='wuri']",
				'length_25':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[16]/td[@class='wuri']",
				'micronaire_avg':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[18]/td[@class='wuri']",
				'micronaire_c1':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[19]/td[@class='wuri']",
				'micronaire_b1':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[20]/td[@class='wuri']",
				'micronaire_a':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[21]/td[@class='wuri']",
				'micronaire_b2':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[22]/td[@class='wuri']",
				'micronaire_c2':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[23]/td[@class='wuri']",
				'breaking_tenacity_avg':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[26]/td[@class='wuri']",
				'breaking_tenacity_max':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[25]/td[@class='wuri']",
				'breaking_tenacity_min':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[24]/td[@class='wuri']",
				'length_uniformty_avg':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[24]/td[2]",
				'length_uniformty_max':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[23]/td[2]",
				'length_uniformty_min':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[22]/td[3]",
				'preparation':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[6]/td[@class='qitatz wuri']",
				'package_num':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[3]/td[@class='qitatz'][2]",
				'weight_gross':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[4]/td[@class='qitatz'][2]",
				'weight_tare':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[5]/td[@class='qitatz'][2]",
				'weight_net':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[6]/td[@class='qitatz'][2]",
				'weight_conditoned':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[5]/td[@class='qitatz wuri']",
				'huichao_avg':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[3]/td[@class='qitatz wuri']",
				'hanza_avg':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[4]/td[@class='qitatz wuri']",
				#'jiagongleixing':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[1]/td[@class='tongzh'][1]",
				#'jiagongleixing' : "div[@class='topResou']/div[@class='leftResou fl']/div[@class='firstResou']/span[@class='resouMeth fl']",
				#'ph_in_page' : "/html/body/div[@class='topResou']/div[@class='leftResou fl']/div[@class='firstResou']/span[@class='resouPH fl']",
				'jiagongqiye':"/html/body/div[@class='surfacer']/div[@class='caption']/p[1]",
				'cangku':"/html/body/div[@class='surfacer']/table[@class='summar tabw']/tbody/tr[1]/td[3]",
				};

	def getHashIDByPH(self, str_ph):
		url="%s%s"%(self.url_search, str_ph);
		try:
			response = requests.get(url, headers=self.http_headers, cookies=self.cookies, timeout=10);
		except requests.exceptions.Timeout:
			sys.stderr.write('timeout in getHashIDByPH:%s\n'%(str_ph));
			return -1, None;
		except :
			sys.stderr.write('exception in getHashIDByPH:%s\n'%(str_ph));
			return -1, None;
		#res_json = json.loads(response.text);
		res_json = response.json();
		if None != res_json and 'resCode' in res_json and 1==res_json['resCode']:
			return 0, res_json['resData'][0]['enph'];
		else:
			return -1, None

	def getDetailPage(self, str_hash_id, str_ph):
		url="%s%s"%(self.url_detail, str_hash_id);
		try:
			response = requests.get(url, headers=self.http_headers, cookies=self.cookies, timeout=10);
		except requests.exceptions.Timeout:
			sys.stderr.write('timeout in getDetailPage:%s\n'%(str_ph));
			return None;
		except :
			sys.stderr.write('exception in getDetailPage:%s\n'%(str_ph));
			return None;
		return self.parseDetailPage(response.content);

	def parseDetailPage(self, page_html):
		dic_ret = {};
		html = etree.HTML(page_html);
		for field in sorted(self.xpath_dic.keys()):
			#if 'jiagongleixing' == field:
			#	pdb.set_trace();
			result = html.xpath(self.xpath_dic[field]);
			#print("%s\t%s"%(field,result[0].text.encode('utf-8').strip()));
			dic_ret[field] = result[0].text.encode('utf-8').strip();
		return dic_ret;
	
	def crawlDetailInfoByPH(self, str_ph):
		r = requests.get('http://www.emiancang.com/', headers = self.http_headers);
		self.cookies = r.cookies;
		#pdb.set_trace();
		#res, str_hash_id = self.getHashIDByPH(str_ph);
		#if 0 != res:
		#	return None;
		str_hash_id = '82C8B9CC64EF3D5ECF1C1E98064488EB';
		info_detail = self.getDetailPage(str_hash_id,str_ph);
		#for k in sorted(info_detail.keys()):
		#	print("%s\t%s"%(k,info_detail[k]));
		if None != info_detail:
			info_detail['ph'] = str_ph;
		return info_detail;

	def getPagesByContentIDs(self):
		cnt_id_start=312819;
		url='http://www.emiancang.com/product/content/content--312819.html';

class EMianWang(Spider):
	
	def __init__(self):
		self.url_search_ph='http://www.cottoneasy.com/cottonSearchFrame';
		self.url_detail_ph='http://www.cottoneasy.com/';
		self.headers = {
				'Accept': 'text/html, */*; q=0.01',
				'Accept-Encoding': 'gzip, deflate',
				'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
				'Connection': 'keep-alive',
				'Content-Length': '62',
				'Content-Type': 'application/json',
				#'Cookie': 'sid=7fa0afc9-5dcd-4c79-95e9-429ba940f2cc; IESESSION=alive; pgv_pvi=8466082816; pgv_si=s8013568000',
				'Host': 'www.cottoneasy.com',
				'Origin': 'http://www.cottoneasy.com',
				'Referer': 'http://www.cottoneasy.com/cottonSearch',
				'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36',
				'X-Requested-With': 'XMLHttpRequest',
				};
		self.xpath_dic = {
				'color_11':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[8]/td[3]",
				'color_21':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[9]/td[2]",
				'color_31':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[10]/td[2]",
				'color_41':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[11]/td[2]",
				'color_51':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[12]/td[2]",
				'color_12':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[13]/td[2]",
				'color_22':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[14]/td[2]",
				'color_32':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[15]/td[2]",
				'color_13':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[16]/td[2]",
				'color_23':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[17]/td[2]",
				'color_33':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[18]/td[2]",
				'color_14':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[19]/td[2]",
				'color_24':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[20]/td[2]",
				'length_avg':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[7]/td[5]",
				'length_32':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[15]/td[4]",
				'length_31':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[14]/td[4]",
				'length_30':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[13]/td[4]",
				'length_29':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[12]/td[4]",
				'length_28':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[11]/td[4]",
				'length_27':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[10]/td[4]",
				'length_26':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[9]/td[4]",
				'length_25':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[8]/td[6]",
				'micronaire_avg':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[17]/td[4]",
				'micronaire_c1':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[18]/td[5]",
				'micronaire_b1':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[19]/td[4]",
				'micronaire_a':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[20]/td[4]",
				'micronaire_b2':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[21]/td[5]",
				'micronaire_c2':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[22]/td[4]",
				'breaking_tenacity_avg':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[25]/td[4]",
				'breaking_tenacity_max':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[24]/td[5]",
				'breaking_tenacity_min':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[23]/td[5]",
				'length_uniformty_avg':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[23]/td[2]",
				'length_uniformty_max':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[22]/td[2]",
				'length_uniformty_min':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[21]/td[3]",
				'preparation_p1':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/ul[@id='ginningScale']/li[@name='P1']",
				'preparation_p2':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/ul[@id='ginningScale']/li[@name='P2']",
				'preparation_p3':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/ul[@id='ginningScale']/li[@name='P3']",
				'package_num':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[2]/td[@class='txt-left'][1]",
				'weight_gross':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[3]/td[@class='txt-left'][1]",
				'weight_tare':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[4]/td[@class='txt-left'][1]",
				'weight_net':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[5]/td[@class='txt-left'][1]",
				'weight_conditoned':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[4]/td[@class='txt-left'][2]",
				'huichao_avg':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[2]/td[@class='txt-left'][2]",
				'hanza_avg':"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[3]/td[@class='txt-left'][2]",
				'jiagongleixing' :"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[1]/td[2]/span",
				'ph_in_page' :"/html/body/div[@class='em-main']/div[3]/div[@class='content-body']/div[@id='tab1-content']/table[@class='content-table']/tr[1]/td[1]",
				'jiagongqiye':"/html/body/div[@class='em-main']/div[@class='quality-info']/div[@class='quality-row quality-tr'][1]/div[@class='q-item'][5]",
				'cangku':"/html/body/div[@class='em-main']/div[1]/div[@class='store-local']/div[2]/span",
				};

	def getHashIDByPH(self, str_ph):
		pay_load = {
				'count':0,
				'headChineseList':[],
				'headStrList' : [str_ph]
				};
		str_pay_load = json.dumps(pay_load);
		try:
			res = requests.post(self.url_search_ph, data=str_pay_load, headers=self.headers, timeout=10);
		except requests.exceptions.Timeout:
			logging.error('timeout in getHashIDByPH:%s\n'%(str_ph));
			return 1,None;
		except:
			logging.error('exception in getHashIDByPH:%s\n'%(str_ph));
			return 1,None;

		urls = re.findall(r'cottonBatchDetail[^"]+',res.text,re.S);
		if len(urls) >= 1:
			return 0,urls[1];
		else:
			logging.error("no hashcode is found for asin:%s"%(str_ph));
			return 1,None;

	def getDetailPage(self, str_hash_id, str_ph):
		url="%s%s"%(self.url_detail_ph, str_hash_id);
		#print(url);
		try:
			response = requests.get(url, timeout=10);
		except requests.exceptions.Timeout:
			logging.error('timeout in getDetailPage:%s\t%s\n'%(str_ph, str_hash_id));
			return None;
		except :
			logging.error('exception in getDetailPage:%s\t%s\n'%(str_ph, str_hash_id));
			return None;
		return self.parseDetailPage(response.content);

	def parseDetailPage(self, page_html):
		dic_ret = {};
		html = etree.HTML(page_html);
		for field in sorted(self.xpath_dic.keys()):
			#print(field);
			#pdb.set_trace();
			result = html.xpath(self.xpath_dic[field]);
			if len(result) < 1:
				continue;
			#print("%s\t%s"%(field, result[0].text.encode('utf-8').strip()));
			if None != result[0] and None != result[0].text:
				#dic_ret[field] = result[0].text.encode('utf-8').strip().decode();
				dic_ret[field] = result[0].text.strip();
				if field in ['huichao_avg', 'hanza_avg']:
					sep_pos = dic_ret[field].find('%');
					if -1 != sep_pos:
						dic_ret[field] = dic_ret[field][:sep_pos];
			else:
				dic_ret[field] = 'NaN';
		return dic_ret;
	
	def crawlDetailInfoByPH(self, str_ph):
		res, str_hash_id = self.getHashIDByPH(str_ph);
		if 0 != res:
			return None;
		info_detail = self.getDetailPage(str_hash_id,str_ph);
		#for k in sorted(info_detail.keys()):
		#	print("%s\t%s"%(k,info_detail[k]));
		if None != info_detail:
			info_detail['ph'] = str_ph;
		return info_detail;


def init():
	#emiancang = EMianCang();
	emiancang = EMianWang();
	emiancang.crawByManufactorys();
	#emiancang.getPHsByManufactory();
	#emiancang.getDetailInfoByPH('65140161194');
	#emiancang.getDetailInfoByPH('65638171086');

def append_asin(file_name='ph'):
	dic_ph = {};
	with open(file_name, 'r') as fd:
		for line in fd:
			line = line.strip();
			dic_ph[line] = 1;
	emiancang = EMianWang();
	emiancang.crawByManufactorys(dic_ph);

def crawl_by_asin_id(asin_id="65021171001"):
	#spider = EMianWang();
	spider = EMianCang();
	asin = spider.crawlDetailInfoByPH(asin_id);
	print(asin);

if __name__=="__main__":
	#init();
	#append_asin();
	crawl_by_asin_id('65673171447');
