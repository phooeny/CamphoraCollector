CREATE TABLE `cotton_crawler` (
	  `id` bigint(10) unsigned NOT NULL AUTO_INCREMENT,
	  `production_code` bigint(10) NOT NULL,
	  `source` int(11) DEFAULT NULL,
	  PRIMARY KEY (`id`),
	  UNIQUE KEY `production_code` (`production_code`)
) ENGINE=InnoDB AUTO_INCREMENT=4404 DEFAULT CHARSET=utf8;
