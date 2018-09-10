#!/usr/bin/env python
# coding=utf-8

from sqlalchemy import Column, String, Integer, Float, DateTime, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from sqlalchemy.ext.hybrid import hybrid_property, hybrid_method
import datetime
__all__ = ['DBOrmDao','Factory','ASINCrawlerInfo']

Base = declarative_base()

class ASINCrawlerInfo(Base):
	__tablename__ = 'asin_crawler_info';
	
	id = Column(Integer, primary_key=True, autoincrement=True)
	production_code = Column(Integer, nullable=False);
	source = Column(String(10), nullable=False);
	op_time = Column(DateTime,  nullable=False);

	def __init__(self, asin_spider_dict):
		self.production_code = int(asin_spider_dict['ph']);
		self.source = asin_spider_dict.get('source',None);
		self.op_time = asin_spider_dict.get('op_time',None);
	
class Factory(Base):
	__tablename__ = 'factory';
	
	id = Column(Integer, primary_key=True, autoincrement=True)
	factory_id = Column(Integer, unique=True, nullable=False);
	factory_name = Column(String(100), nullable=False);

	def __init__(self, factory_id, factory_name):
		self.factory_id = factory_id;
		self.factory_name = factory_name;

class ASIN2Crawler(Base):
	# 表的名字:
	__tablename__ = 'cotton_crawler'

	# 表的结构:
	id = Column(Integer, primary_key=True, autoincrement=True)
	production_code = Column(Integer, unique=True);
	source = Column(Integer,nullable=True);
	scan_num = Column(Integer);
	update_time = Column(String(20),nullable=True);
	spider_detail = Column(String(20),nullable=True);

	def __init__(self, production_code, **kw):
		self.production_code = production_code;
		self.source = kw.get('source',None);
		self.scan_num = kw.get('scan_num',0);
		self.update_time = kw.get('update_time',None);
		self.spider_detail = kw.get('spider_detail',None);
		
class ASINItem(Base):
	__tablename__ = "cotton_batch";
	
	# schema
	id = Column(Integer, primary_key=True, autoincrement=True);
	production_code = Column(Integer, unique=True);
	colour_w1 = Column(Float);
	colour_w2 = Column(Float);
	colour_w3 = Column(Float);
	colour_w4 = Column(Float);
	colour_w5 = Column(Float);
	colour_l1 = Column(Float);
	
	
	def __init__(self, production_code):
		self.production_code = production_code;

	@hybrid_property
	def factory_pipeline(self):
		return func.cast(self.factory_id * 10 + self.pipeline_id, Integer);

	@hybrid_property
	def factory_id(self):
		return func.cast(func.floor(self.production_code/1000000), Integer); 

	@hybrid_property
	def pipeline_id(self):
		prefix = func.cast(func.floor(self.production_code/1000), Integer);
		return func.cast(prefix % 10, Integer);

	@hybrid_property
	def year(self):
		prefix = func.cast(func.floor( self.production_code/10000),Integer);
		return func.cast(prefix % 100, Integer);

class DBOrmDao():
	
	def __init__(self, db_link="mysql+mysqlconnector://root:root@localhost:3306/ssm_demo_db"):
		self.engine = create_engine(db_link);
		self.DBSession = sessionmaker(bind=self.engine);
		self.session = self.DBSession();
	
	def qry_max_asin_id(self, year=17):
		query = self.session.query( ASINItem.factory_id, \
				ASINItem.year, \
				ASINItem.pipeline_id, \
				func.max(ASINItem.production_code).label('max_asin_id')) \
			.filter(ASINItem.year == year) \
			.group_by(ASINItem.factory_pipeline) \
			.order_by(ASINItem.factory_pipeline);
		
		for row in query:
			print(row);
	
	def qry_lastest_asin(self, year=17):
		stmt = self.session.query( ASINItem.factory_id.label('factory_id'), \
				ASINItem.year.label('year'), \
				ASINItem.pipeline_id.label('pipeline_id'), \
				func.max(ASINItem.production_code).label('max_asin_id')) \
			.filter(ASINItem.year == year) \
			.group_by(ASINItem.factory_pipeline).subquery();
		
		query = self.session.query(Factory.factory_id, stmt.c.year, stmt.c.pipeline_id, stmt.c.max_asin_id) \
			.outerjoin(stmt, Factory.factory_id == stmt.c.factory_id) \
			.order_by(Factory.factory_id);

		for row in query:
			dic_ret = {};
			dic_ret['factory_id'] = row[0];
			dic_ret['year'] = row[1];
			dic_ret['pipeline_id'] = row[2];
			dic_ret['max_asin_id'] = row[3]
			yield dic_ret;				

	def insert_asin(self, asin_spider):
		#TODO insert asin
		#asin = ASINItem(asin_spider);
		#self.session.add(asin);
		#insert update_time
		info = ASINCrawlerInfo(asin_spider);
		self.session.add(info);
		self.session.commit();

	def qry_factory_list(self):
		fty_list = self.session.query(Factory).all();
		for fty in fty_list:
			print("%d\t%s"%(fty.factory_id, fty.factory_name));

	def insert_factory_by_file(self, file_name='./factory_list'):	
		fd = open(file_name, 'r');
		for line in fd:
			line = line.strip();
			arr = line.split('\t');
			factory = Factory(arr[0],arr[1]);
			self.session.add(factory);
		self.session.commit()
		fd.close();
	
	def __del__(self):
		self.session.close();	

def main():
	# 初始化数据库连接:
	engine = create_engine('mysql+mysqlconnector://root:root@localhost:3306/ssm_demo_db')
	# 创建DBSession类型:
	DBSession = sessionmaker(bind=engine)

	# 创建session对象:
	session = DBSession()

	"""
	asin = session.query(ASIN2Crawler).filter(ASIN2Crawler.production_code==32091171290).one();
	print(asin.id);
	print(asin.production_code);
	print(asin.source);
	print(type(asin.scan_num));
	print(asin.update_time);
	print(asin.spider_detail);
	"""


	# 创建新User对象:
	#asin = ASIN2Crawler(123);
	# 添加到session:
	#session.add(asin)
	#session.merge(asin);

	#import datetime
	#info = ASINCrawlerInfo(123,'emianwang',datetime.datetime.now());
	#session.add(info);
	# 提交即保存到数据库:
	#session.commit()

	query = session.query(ASINItem.factory_id, ASINItem.year, func.max(ASINItem.production_code).label('max_asin_id')) \
		.filter(ASINItem.year == 17) \
		.group_by(ASINItem.factory_pipeline) \
		.order_by(ASINItem.factory_pipeline);
	for row in query:
		print(row);

	# 关闭session:
	session.close()

if __name__ == "__main__":
	#main();
	dao = DBOrmDao();
	#dao.qry_max_asin_id();
	for i in dao.qry_lastest_asin():
		print(i);
	#dao.qry_factory_list();
