#!/usr/bin/env python
# coding=utf-8

import sys
import pymysql

__all__ = ('','');

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
	def __init__(self):
		pass;

	def get_attr_names(self):
		pass;

	def get_attr_values(self):
		pass;

	def get_attr_str(self):
		pass;


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
		cursor = self.conn.db.cursor()
		sql = "REPLACE INTO %s%s VALUES %s"%(self.tbl_ctn_detail, dao_asin.get_attr_names, dao_asin.get_attr_values);
		try:
			cursor.execute(sql)
			self.conn.db.commit()
		except:
			print('插入数据失败!');
			self.conn.db.rollback()


if __name__ == "__main__":
	dao = CottonPHDAO();
	for asin in range(12345,12445):
		dao.insert_uncrawled_asin(asin);
	sys.exit(0);
	for asin_id in dao.query_uncrawled_asins():
		print(asin_id);
		dao.del_uncrawled_asin(asin_id);
