#!/usr/bin/env python
# coding=utf-8

import sys
import pymysql
from collections import OrderedDict

__all__ = ('CottonPHDAO','CottonPH');

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
			print('更新数据失败!');
			# 发生错误时回滚
			db.rollback()


class CottonPH:
	def __init__(self, dic_detail):
		fields_mapping = OrderedDict(
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
				'length_32': ['', 0.0, float],
				'length_31': ['', 0.0, float],
				'length_30': ['', 0.0, float],
				'length_29': ['', 0.0, float],
				'length_28': ['', 0.0, float],
				'length_27': ['', 0.0, float],
				'length_26': ['', 0.0, float],
				'length_25': ['', 0.0, float],
				'micronaire_avg': ['avg_micronaire', 0.0, float],
				'micronaire_c1': ['', 0.0, float],
				'micronaire_b1': ['', 0.0, float],
				'micronaire_a': ['', 0.0, float],
				'micronaire_b2': ['', 0.0, float],
				'micronaire_c2': ['', 0.0, float],
				'breaking_tenacity_avg': ['strength', 0.0, float],
				'breaking_tenacity_max': ['', 0.0, float],
				'breaking_tenacity_min': ['', 0.0, float],
				'length_uniformty_avg': ['avg_evenness', 0.0, float],
				'length_uniformty_max': ['', 0.0, float],
				'length_uniformty_min': ['', 0.0, float],
				'preparation_p1': ['ginning_p1', 0.0, float],
				'preparation_p2': ['ginning_p2', 0.0, float],
				'preparation_p3': ['ginning_p3', 0.0, float],
				'package_num': ['', 0.0, float],
				'weight_gross': ['', 0.0, float],
				'weight_tare': ['', 0.0, float],
				'weight_net': ['', 0.0, float],
				'weight_conditoned': ['', 0.0, float],
				'huichao_avg': ['', 0.0, float],
				'hanza_avg': ['miscellaneous', 0.0, float],
				'jiagongleixing' : ['', 0.0, float],
				'ph_in_page' : ['', 0, int],
				'jiagongqiye': ['factory', 'NaN', '\'{}\''.format ],
				'cangku': ['warehouse', 'NaN', '\'{}\''.format],
				});
		self.attr_mapping = OrderedDict();
		self.attrs = OrderedDict();
		for k in fields_mapping.keys():
			if '' != fields_mapping[k][0]:
				self.attrs[fields_mapping[k][0]] = fields_mapping[k][2](dic_detail.get(k, fields_mapping[k][1]));

	def get_attr_names(self):
		return ",".join(self.attrs.keys());

	def get_attr_values(self):
		return ",".join([str(v) for v in self.attrs.values()]);

	def get_attr_str(self):
		return ",".join(["(%s, %s)"%(str(k),str(v)) for k,v in self.attrs.items()]);
		

class CottonPHDAO():

	def __init__(self):
		self.conn = MySQLUtill();
		self.tbl_uncrawled = "cotton_crawler";
		self.tbl_ctn_detail = "cotton_batch";

	def query_uncrawled_asins(self):
		cursor = self.conn.db.cursor()
		sql = "SELECT production_code FROM %s"%(self.tbl_uncrawled);
		try:
			cursor.execute(sql)
			results = cursor.fetchall()
			for row in results:
				yield row[0];
		except:
			print("Error: unable to fecth data")

	def del_uncrawled_asin(self, asin_id):
		cursor = self.conn.db.cursor()
		sql = "DELETE FROM %s WHERE production_code = '%d'"%(self.tbl_uncrawled, asin_id);
		try:
		   cursor.execute(sql)
		   self.conn.db.commit()
		except:
			print('删除数据失败!');
			self.conn.db.rollback()

	def insert_uncrawled_asin(self,asin_id):
		cursor = self.conn.db.cursor()
		sql = "REPLACE INTO %s(production_code) VALUES (%d)"%(self.tbl_uncrawled, asin_id);
		try:
			cursor.execute(sql)
			self.conn.db.commit()
		except:
			print('插入数据失败!');
			self.conn.db.rollback()

	def insert_asin(self, dao_asin):
		ret = 0;
		cursor = self.conn.db.cursor()
		sql = "REPLACE INTO %s(%s) VALUES (%s)"%(self.tbl_ctn_detail, dao_asin.get_attr_names(), dao_asin.get_attr_values());
		try:
			cursor.execute(sql)
			self.conn.db.commit()
		except:
			print('插入数据失败!');
			self.conn.db.rollback()
			ret = 1;
		return ret;

if __name__ == "__main__":
	dao = CottonPHDAO();
	for asin in range(12345,12445):
		dao.insert_uncrawled_asin(asin);
	sys.exit(0);
	for asin_id in dao.query_uncrawled_asins():
		print(asin_id);
		dao.del_uncrawled_asin(asin_id);
