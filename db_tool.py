import sqlite3 as sqlite
import time
import re
import hashlib

class DataBase:

	def __init__(self, db_file=None):
		if None != db_file and len(db_file)>=1:
			self.db_file = db_file;
		else:
			self.db_file = 'cis.db';

		self.conn = sqlite.connect(self.db_file);
		self.ptn_user_name = re.compile(r'[a-zA-Z0-9]{3,}');
		self.ptn_psw = re.compile(r'[a-zA-Z0-9]{6,}');
		self.ptn_batch_num = re.compile(r'[0-9]{6,}');
		self.catton_column_info = {
                        'batch_num':'text PRIMARY KEY NOT NULL',
                        'color_11':"text NOT NULL DEFAULT ''",
			'color_21':"text NOT NULL DEFAULT ''",
			'color_31':"text NOT NULL DEFAULT ''",
			'color_41':"text NOT NULL DEFAULT ''",
			'color_51':"text NOT NULL DEFAULT ''",
			'color_12':"text NOT NULL DEFAULT ''",
			'color_22':"text NOT NULL DEFAULT ''",
			'color_32':"text NOT NULL DEFAULT ''",
			'color_13':"text NOT NULL DEFAULT ''",
			'color_23':"text NOT NULL DEFAULT ''",
			'color_33':"text NOT NULL DEFAULT ''",
			'color_14':"text NOT NULL DEFAULT ''",
			'color_24':"text NOT NULL DEFAULT ''",
			'length_avg':"text NOT NULL DEFAULT ''",
			'length_32':"text NOT NULL DEFAULT ''",
			'length_31':"text NOT NULL DEFAULT ''",
			'length_30':"text NOT NULL DEFAULT ''",
			'length_29':"text NOT NULL DEFAULT ''",
			'length_28':"text NOT NULL DEFAULT ''",
			'length_27':"text NOT NULL DEFAULT ''",
			'length_26':"text NOT NULL DEFAULT ''",
			'length_25':"text NOT NULL DEFAULT ''",
			'micronaire_avg':"text NOT NULL DEFAULT ''",
			'micronaire_c1':"text NOT NULL DEFAULT ''",
			'micronaire_b1':"text NOT NULL DEFAULT ''",
			'micronaire_a':"text NOT NULL DEFAULT ''",
			'micronaire_b2':"text NOT NULL DEFAULT ''",
			'micronaire_c2':"text NOT NULL DEFAULT ''",
			'breaking_tenacity_avg':"text NOT NULL DEFAULT ''",
			'breaking_tenacity_max':"text NOT NULL DEFAULT ''",
			'breaking_tenacity_min':"text NOT NULL DEFAULT ''",
			'length_uniformty_avg':"text NOT NULL DEFAULT ''",
			'length_uniformty_max':"text NOT NULL DEFAULT ''",
			'length_uniformty_min':"text NOT NULL DEFAULT ''",
			'preparation_p1':"text NOT NULL DEFAULT ''",
			'preparation_p2':"text NOT NULL DEFAULT ''",
			'preparation_p3':"text NOT NULL DEFAULT ''",
			'package_num':"text NOT NULL DEFAULT ''",
			'weight_gross':"text NOT NULL DEFAULT ''",
			'weight_tare':"text NOT NULL DEFAULT ''",
			'weight_net':"text NOT NULL DEFAULT ''",
			'weight_conditoned':"text NOT NULL DEFAULT ''",
			'huichao_avg':"text NOT NULL DEFAULT ''",
			'hanza_avg':"text NOT NULL DEFAULT ''",
			'jiagongleixing':"text NOT NULL DEFAULT ''",
			'jiagongqiye':"text NOT NULL DEFAULT ''",
			'cangku':"text NOT NULL DEFAULT ''",
			'chandi':"text NOT NULL DEFAULT ''",
                        }

	def create_db(self):
                cotton_crt_sql = 'create table cotton ('
                for key in self.catton_column_info.keys():
                        cotton_crt_sql += '%s %s,'%(key,self.catton_column_info[key]);
                cotton_crt_sql = cotton_crt_sql[0:-1];
                cotton_crt_sql = cotton_crt_sql + ' )';
                table_crt_sql = {
                        'user':'create table user (name varchar(10) UNIQUE,psw text NOT NULL, ttl integer NOT NULL)',
                        'cotton':cotton_crt_sql,
                        }
		for k in table_crt_sql:
			v = table_crt_sql[k];
			self.conn.execute(v);
			self.conn.commit();
			
	def get_user(self, usr):
                dic = {};
                if None == usr or not re.match(self.ptn_user_name, usr):
			return dic;
		cursor = self.conn.execute('select * from user');
		item = cursor.fetchone();
		if None == item:
                        return dic;
                dic['name'] = item[0];
                dic['ttl'] = item[2];
                return dic;
        
	def add_user(self, usr, psw, ttl):
		if None == usr or not re.match(self.ptn_user_name, usr) or None == psw or not re.match(self.ptn_psw,psw):
			return False;
		self.conn.execute('replace into user values (\'%s\',\'%s\',%d)'%(usr,psw,ttl));
		self.conn.commit();
		return True;

	def gen_psw(self,seed,ttl):
		local = time.localtime();
		st='NULL';
		if 0 == ttl:
			return seed;
		elif 1 == ttl:
			st = "%s-%dY"%(seed, local.tm_year);
		elif 2 == ttl:
			st = "%s-%dY-%dS"%(seed, local.tm_year, local.tm_mon/3);
		elif 3 == ttl:
			st = "%s-%dY-%dM"%(seed, local.tm_year, local.tm_mon);
		elif 4 == ttl:
			st = "%s-%s"%(seed, time.strftime("%Y-%W", local));
		elif 5 == ttl:
			st = "%s-%dY-%dM-%dD"%(seed, local.tm_year, local.tm_mon, local.tm_mday);
		elif 6 == ttl:
			st = "%s-%dY-%dM-%dD-%dH"%(seed, local.tm_year, local.tm_mon, local.tm_mday, local.tm_hour);
		#return "%06d"%(hash(st)%1000000)
		ret = hashlib.md5(st).hexdigest();
		return ret[0:6]
		
	def get_user_psw_list(self):
		ret = [];
		cursor = self.conn.execute('select * from user');
		#print cursor.description;
		#ret = cursor.fetchone();
		#print ret.keys()
		#ret = cursor.fetchall();
		#print ret;
		for row in cursor:
			#print row.keys()
			print "%s\t%s\t%d"%(row[0],self.gen_psw(row[1],row[2]),row[2]);
                        ret.append([row[0],self.gen_psw(row[1],row[2]),row[2],row[1]]);
                return ret;

	def check_user_psw(self,user,psw):
		if None == user or not re.match(self.ptn_user_name, user):
			return False;
		cursor = self.conn.execute('select * from user where name=\'%s\''%(user));
		for row in cursor:
                        #print self.gen_psw(row[1],row[2]);
			if psw == self.gen_psw(row[1],row[2]):
				return True;
		return False;

	def add_cotton(self, dic_info):
                if None == dic_info:
                        return False;
                if 'batch_num' not in dic_info or len(dic_info['batch_num']) < 1:
                        return False;
                if None != self.get_cotton_by_batch_num(dic_info['batch_num']):
                        return False;
                column_name = '';
                val = '';
		for k in self.catton_column_info.keys():
                        if k not in dic_info:
                                continue;
                        column_name += '%s,'%(k);
                        val += '\'%s\','%(dic_info[k]);
                if len(column_name)<1 or len(val) < 1:
                        return False;
                column_name = column_name[0:-1];
                val = val[0:-1];
                sql = "replace into cotton (%s)values (%s)"%(column_name,val);
                self.conn.execute(sql);
                self.conn.commit();
                return True;
                        

	def get_cotton_by_batch_num(self, batch_num):
		if None==batch_num or not re.match(self.ptn_batch_num,batch_num):
			return None;
		cursor = self.conn.execute('select * from cotton where batch_num=\'%s\''%(batch_num));
		#ret = cursor.fetchall();
		#print ret;
		ret = cursor.fetchone();
		if None == ret:
			return None;
		ret_dic = {};
		col_name_list = [tu[0] for tu in cursor.description];
		for pos in range(len(col_name_list)):
			ret_dic[col_name_list[pos]]=ret[pos];
		return ret_dic;

if __name__ == '__main__':
	db = DataBase();
	db.create_db();
	#db.add_user('scmm666','5658968',0);
	#db.add_user('scm8888','5658968',0);
	#db.add_user('scmm999','5658968',0);
	#db.add_user('scmm1','5658968',1);
	#db.add_user('scmm2','5658968',2);
	#db.add_user('scmm3','5658968',3);
	#db.add_user('scmm4','5658968',4);
	db.add_user('scmm5','5658968',5);
	#db.add_user('scmm6','5658968',6);
	db.get_user_psw_list();
	#print db.check_user_psw('zjn0','asd123');
	#print db.check_user_psw('zjn1','607993');
	#item = {'batch_num':'11112222',}
	#db.add_cotton(item);
	#print db.get_cotton_by_batch_num(item['batch_num']);
