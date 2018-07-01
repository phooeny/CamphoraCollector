#_*_coding:utf-8;_*_
#qpy:3
#qpy:console

import time
import hashlib

def gen_psw(seed,ttl):
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
	ret = hashlib.md5(st).hexdigest();
	return ret[0:6]

user_list = [('scmm666','5658968',0),
('scm8888','5658968',0),
('scmm999','5658968',0),
('scmm1','5658968',1),
('scmm2','5658968',2),
('scmm3','5658968',3),
('scmm4','5658968',4),
('scmm5','5658968',5),
('scmm6','5658968',6)];

ttl_level = {
		0:'forever',
		1:'year',
		2:'season',
		3:'month',
		4:'week',
		5:'day',
		6:'hour'};

if __name__ == '__main__':
	for item in user_list:
		psw = gen_psw(item[1],item[2]);
		ttl = ttl_level[item[2]];
		print "%s\t%s\t%s\t"%(item[0],psw,ttl);
