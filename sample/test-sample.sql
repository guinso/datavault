-- Adminer 4.3.1 MySQL dump

SET NAMES utf8;
SET time_zone = '+00:00';
SET foreign_key_checks = 0;
SET sql_mode = 'NO_AUTO_VALUE_ON_ZERO';

SET NAMES utf8mb4;

DROP TABLE IF EXISTS `hub_invoice_order_rev0`;
CREATE TABLE `hub_invoice_order_rev0` (
  `invoice_order_hash_key` char(32) COLLATE utf8mb4_unicode_ci NOT NULL,
  `load_date` datetime NOT NULL,
  `record_source` char(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`invoice_order_hash_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


DROP TABLE IF EXISTS `hub_invoice_rev0`;
CREATE TABLE `hub_invoice_rev0` (
  `invoice_hash_key` char(32) COLLATE utf8mb4_unicode_ci NOT NULL,
  `load_date` datetime NOT NULL,
  `record_source` char(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `invoice_no` char(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`invoice_hash_key`),
  UNIQUE KEY `invoice_no` (`invoice_no`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;



DROP TABLE IF EXISTS `link_invoice_order_item_rev0`;
CREATE TABLE `link_invoice_order_item_rev0` (
  `invoice_order_item_hash_key` char(32) COLLATE utf8mb4_unicode_ci NOT NULL,
  `load_date` datetime NOT NULL,
  `record_source` char(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `invoice_hash_key` char(32) COLLATE utf8mb4_unicode_ci NOT NULL,
  `invoice_order_hash_key` char(32) COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`invoice_order_item_hash_key`),
  KEY `invoice_hash_key` (`invoice_hash_key`),
  KEY `invoice_order_hash_key` (`invoice_order_hash_key`),
  CONSTRAINT `link_invoice_order_item_rev0_ibfk_1` FOREIGN KEY (`invoice_hash_key`) REFERENCES `hub_invoice_rev0` (`invoice_hash_key`),
  CONSTRAINT `link_invoice_order_item_rev0_ibfk_2` FOREIGN KEY (`invoice_order_hash_key`) REFERENCES `hub_invoice_order_rev0` (`invoice_order_hash_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


DROP TABLE IF EXISTS `sat_invoice_rev0`;
CREATE TABLE `sat_invoice_rev0` (
  `invoice_hash_key` char(32) COLLATE utf8mb4_unicode_ci NOT NULL,
  `load_date` datetime NOT NULL,
  `end_date` datetime DEFAULT NULL,
  `record_source` char(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `date_of_issue` date NOT NULL,
  `remark` text COLLATE utf8mb4_unicode_ci,
  `tax` decimal(10,2) NOT NULL,
  PRIMARY KEY (`invoice_hash_key`,`load_date`),
  KEY `invoice_hash_key` (`invoice_hash_key`),
  CONSTRAINT `sat_invoice_rev0_ibfk_1` FOREIGN KEY (`invoice_hash_key`) REFERENCES `hub_invoice_rev0` (`invoice_hash_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- 2017-08-02 09:25:56
