#!/usr/bin/env python
# coding=utf-8

import sys
import pymysql
from collections import OrderedDict
import pdb
import logging

__all__ = ('CottonPHDAO','CottonPH','filter_asin');

class MySQLUtill:
	def __init__(self, host="localhost", dbname="ssm_demo_db", usr="root", psw="root"):
		self.connectdb(host, dbname, usr, psw);
	
	#def __del__(self):
	#	self.closedb();

	def connectdb(self, host, dbname, usr, psw):
		self.db = pymysql.connect(host, usr, psw, dbname);
	
	def closedb(self):
		self.db.close()

	def createtable(self, dbname, sql):
		cursor = self.db.cursor()
		# 如果存在表Sutdent先删除
		cursor.execute("DROP TABLE IF EXISTS Student")
		sql = """CREATE TABLE Student (
				ID CHAR(10) NOT NULL,
				Name CHAR(8),
				Grade INT )"""
		# 创建Sutdent表
		cursor.execute(sql)




	def updatedb(self):
		# 使用cursor()方法获取操作游标 
		cursor = self.db.cursor()
		# SQL 更新语句
		sql = "UPDATE Student SET Grade = Grade + 3 WHERE ID = '%s'" % ('003')

		try:
			# 执行SQL语句
			cursor.execute(sql)
			# 提交到数据库执行
			db.commit()
		except:
			logging.error('更新数据失败!');
			# 发生错误时回滚
			db.rollback()

class CottonPH:
	
	fields_mapping = OrderedDict(sorted(
			{
				'ph': ['production_code', 0, int],
				'color_11': ['colour_w1', 0.0, float],
				'color_21': ['colour_w2', 0.0, float],
				'color_31': ['colour_w3', 0.0, float],
				'color_41': ['colour_w4', 0.0, float],
				'color_51': ['colour_w5', 0.0, float],
				'color_12': ['colour_l1', 0.0, float],
				'color_22': ['colour_l2', 0.0, float],
				'color_32': ['colour_l3', 0.0, float],
				'color_13': ['colour_ly1', 0.0, float],
				'color_23': ['colour_ly2', 0.0, float],
				'color_33': ['colour_ly3', 0.0, float],
				'color_14': ['colour_y1', 0.0, float],
				'color_24': ['colour_y2', 0.0, float],
				'length_avg': ['avg_length', 0.0, float],
				'length_32': ['length_32', 0.0, float],
				'length_31': ['length_31', 0.0, float],
				'length_30': ['length_30', 0.0, float],
				'length_29': ['length_29', 0.0, float],
				'length_28': ['length_28', 0.0, float],
				'length_27': ['length_27', 0.0, float],
				'length_26': ['length_26', 0.0, float],
				'length_25': ['length_25', 0.0, float],
				'micronaire_avg': ['avg_micronaire', 0.0, float],
				'micronaire_c1': ['micronaire_c1', 0.0, float],
				'micronaire_b1': ['micronaire_b1', 0.0, float],
				'micronaire_a': ['micronaire_a', 0.0, float],
				'micronaire_b2': ['micronaire_b2', 0.0, float],
				'micronaire_c2': ['micronaire_c2', 0.0, float],
				'breaking_tenacity_avg': ['strength', 0.0, float],
				'breaking_tenacity_max': ['strength_max', 0.0, float],
				'breaking_tenacity_min': ['strength_min', 0.0, float],
				'length_uniformty_avg': ['avg_evenness', 0.0, float],
				'length_uniformty_max': ['evenness_max', 0.0, float],
				'length_uniformty_min': ['evenness_min', 0.0, float],
				'preparation_p1': ['ginning_p1', 0.0, float],
				'preparation_p2': ['ginning_p2', 0.0, float],
				'preparation_p3': ['ginning_p3', 0.0, float],
				'package_num': ['package_num', 0.0, '\'{}\''.format],
				'weight_gross': ['weight_gross', 0.0, float],
				'weight_tare': ['weight_tare', 0.0, float],
				'weight_net': ['weight_net', 0.0, float],
				'weight_conditoned': ['weight_conditoned', 0.0, float],
				'huichao_avg': ['huichao', 0.0, float],
				'hanza_avg': ['miscellaneous', 0.0, float],
				'jiagongleixing' : ['jiagongleixing', 0.0, '\'{}\''.format],
				'ph_in_page' : ['', 0, int],
				'jiagongqiye': ['factory', 'NaN', '\'{}\''.format ],
				'cangku': ['warehouse', 'NaN', '\'{}\''.format],
				'chandi': ['production_area', 'NaN', '\'{}\''.format],
				}.items(), key=lambda t: t[0]));

	def __init__(self, dic_detail):
		self.attrs = OrderedDict();
		for k in self.fields_mapping.keys():
			if '' != self.fields_mapping[k][0]:
				self.attrs[self.fields_mapping[k][0]] = self.fields_mapping[k][2](dic_detail.get(k, self.fields_mapping[k][1]));

	def get_attr_values(self):
		return ",".join([str(v) for v in self.attrs.values()]);

	def get_attr_str(self):
		return ",".join(["(%s, %s)"%(str(k),str(v)) for k,v in self.attrs.items()]);
	
	@classmethod
	def get_attr_names(cls):
		return ",".join([cls.fields_mapping[k][0] for k in cls.fields_mapping if '' != cls.fields_mapping[k][0]])

class CottonPHDAO():
	tbl_uncrawled = "cotton_crawler";
	tbl_ctn_detail = "cotton_batch";
	
	def __init__(self):
		self.conn = MySQLUtill();
		#self.tbl_uncrawled = "cotton_crawler";
		#self.tbl_ctn_detail = "cotton_batch";
	
	def query_max_asinid_by_factoryid(self, factory_id, year):
		batch_id = "%s%s"%(factory_id,year);
		cursor = self.conn.db.cursor()
		sql = "SELECT max(production_code) FROM %s where round(production_code/10000)=%s"%(self.tbl_ctn_detail, batch_id);
		ret = None;
		try:
			cursor.execute(sql)
			row = cursor.fetchone()
			ret = row[0];
		except:
			logging.error("Error: query_max_asinid_by_factoryid unable to fecth data");
		return ret;


	def query_asinids_by_factoryid(self, factory_id, year):
		batch_id = "%s%s"%(factory_id,year);
		cursor = self.conn.db.cursor()
		sql = "SELECT production_code FROM %s where round(production_code/10000)=%s"%(self.tbl_ctn_detail, batch_id);
		ret = [];
		try:
			cursor.execute(sql)
			results = cursor.fetchall()
			for row in results:
				ret.append( row[0]);
		except:
			logging.error("Error: query_asinids_by_factoryid unable to fecth data");
		return ret;

	def query_uncrawled_asins(self):
		cursor = self.conn.db.cursor()
		sql = "SELECT production_code,source,scan_num FROM %s order by production_code"%(self.tbl_uncrawled);
		try:
			cursor.execute(sql)
			results = cursor.fetchall()
			for row in results:
				yield (row[0],row[1],row[2]);
		except:
			logging.error("Error: query_uncrawled_asins unable to fecth data")

	def del_uncrawled_asin(self, asin_id):
		cursor = self.conn.db.cursor()
		sql = "DELETE FROM %s WHERE production_code = '%d'"%(self.tbl_uncrawled, asin_id);
		try:
		   cursor.execute(sql)
		   self.conn.db.commit()
		except:
			logging.error('删除数据失败!');
			self.conn.db.rollback()

	def insert_uncrawled_asin(self, asin_id, **attr):
		cursor = self.conn.db.cursor()
		production_code = int(asin_id);
		attr_source = str(attr.get('source','NULL'));
		attr_scan_num = int(attr.get('scan_num', 0));
		sql = "REPLACE INTO %s(production_code, source, scan_num) VALUES (%d, %s, %d)"%(
				self.tbl_uncrawled, production_code, attr_source, attr_scan_num);
		try:
			cursor.execute(sql)
			self.conn.db.commit()
		except:
			logging.error('插入数据失败!');
			self.conn.db.rollback()

	def insert_asin(self, dao_asin):
		ret = 0;
		cursor = self.conn.db.cursor()
		sql = "REPLACE INTO %s(%s) VALUES (%s)"%(self.tbl_ctn_detail, dao_asin.get_attr_names(), dao_asin.get_attr_values());
		try:
			cursor.execute(sql)
			self.conn.db.commit()
		except:
			logging.error('插入数据失败!');
			self.conn.db.rollback()
			ret = 1;
		return ret;


def filter_asin(asin_attrs):
	for k in asin_attrs.keys():
		if asin_attrs[k] in ['NaN', '--']:
			return None;
	for k in ['huichao_avg', 'hanza_avg']:
		if k in asin_attrs:
			pos = asin_attrs[k].find('%');
			if -1 != pos:
				asin_attrs[k] = asin_attrs[k][:pos];
				if len(asin_attrs[k]) < 1:
					return None;
	for k in ['weight_gross','weight_tare','weight_net','weight_conditoned']:
		if k in asin_attrs:
			for unit in ['t','T','kg','KG']:
				pos = asin_attrs[k].find(unit);
				if -1 != pos:
					asin_attrs[k] = asin_attrs[k][:pos];
					if len(asin_attrs[k]) < 1:
						return None;
	ph = asin_attrs['ph'];
	if ph.startswith('65'):
		asin_attrs['chandi'] = '新疆地方';
	elif ph.startswith('66'):
		asin_attrs['chandi'] = '新疆兵团';
	else:
		asin_attrs['chandi'] = '内地';
	
	if 'jiagongleixing' in asin_attrs:
		if asin_attrs['jiagongleixing'] in ['锯齿细绒棉']:
			asin_attrs['jiagongleixing'] = '手摘棉';
		elif asin_attrs['jiagongleixing'] in ['锯齿机采棉']:
			asin_attrs['jiagongleixing'] = '机采棉';
	return asin_attrs;


def sql_from_jsons(json_file_name, sql_file='insert.sql'):
	import json
	fd = open(json_file_name,'r');
	fd_w = open(sql_file,'w');
	#pdb.set_trace();
	#header = "INSERT INTO %s \n(%s) values \n"%(CottonPHDAO.tbl_ctn_detail, CottonPH.get_attr_names());
	header = "REPLACE INTO %s \n(%s) values \n"%(CottonPHDAO.tbl_ctn_detail, CottonPH.get_attr_names());
	fd_w.write(header);
	for line in fd:
		line = line.strip();
		if not line.startswith("{"):
			continue;
		dic_attr = json.loads(line);
		dic_attr = filter_asin(dic_attr);
		if None == dic_attr:
			logging.error("error in line:");
			logging.error(line);
			continue;
		#logging.error(dic_attr['ph']);
		asin_item = CottonPH(dic_attr);
		fd_w.write("(%s),\n"%(asin_item.get_attr_values()));
	fd.close();
	fd_w.close();

def demo_test():
	dao = CottonPHDAO();
	#for asin in range(12345,12445):
	#	dao.insert_uncrawled_asin(asin);
	#sys.exit(0);
	for asin_id,source,scan_num in dao.query_uncrawled_asins():
		print(asin_id);
		print(source);
		print(scan_num);
		#logging.error(asin_id);
		#dao.del_uncrawled_asin(asin_id);

if __name__ == "__main__":
	demo_test();
	#sql_from_jsons('d');
