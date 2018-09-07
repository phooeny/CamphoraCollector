#!/usr/bin/env python
# coding=utf-8

from sqlalchemy import Column, String, Integer, DateTime, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()

class ASINCrawlerInfo(Base):
	__tablename__ = 'asin_crawler_info';
	
	id = Column(Integer, primary_key=True, autoincrement=True)
	production_code = Column(Integer, nullable=False);
	source = Column(String(10),nullable=False);
	op_time = Column(DateTime, nullable=False);

	def __init__(self, production_code, source, op_time):
		self.production_code = production_code;
		self.source = source;
		self.op_time = op_time;
	
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

#fd = open('./factory_list', 'r');
#for line in fd:
#	line = line.strip();
#	arr = line.split('\t');
#	factory = Factory(arr[0],arr[1]);
#	session.add(factory);

# 创建新User对象:
#asin = ASIN2Crawler(123);
# 添加到session:
#session.add(asin)
#session.merge(asin);

import datetime
info = ASINCrawlerInfo(123,'emianwang',datetime.datetime.now());
session.add(info);
# 提交即保存到数据库:
session.commit()

# 关闭session:
session.close()
