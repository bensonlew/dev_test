/*
Navicat MySQL Data Transfer

Source Server         : 192.168.10.51
Source Server Version : 50528
Source Host           : localhost:3306
Source Database       : biocluster

Target Server Type    : MYSQL
Target Server Version : 50528
File Encoding         : 65001

Date: 2015-11-17 16:15:07
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for `clientkey`
-- ----------------------------
DROP TABLE IF EXISTS `clientkey`;
CREATE TABLE `clientkey` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `client` varchar(255) NOT NULL,
  `key` varchar(255) DEFAULT NULL,
  `ipmask` varchar(255) DEFAULT NULL,
  `timelimit` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of clientkey
-- ----------------------------
INSERT INTO `clientkey` VALUES ('1', 'client01', '1ZYw71APsQ', null, '60');
INSERT INTO `clientkey` VALUES ('2', 'test', 'Aw21cADS3u', '172.16.3.0/24;127.0.0.1', null);

-- ----------------------------
-- Table structure for `command`
-- ----------------------------
DROP TABLE IF EXISTS `command`;
CREATE TABLE `command` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `t_id` int(11) NOT NULL,
  `w_id` int(11) NOT NULL,
  `command` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `t_id_index` (`t_id`) USING BTREE,
  KEY `w_id_index` (`w_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of command
-- ----------------------------

-- ----------------------------
-- Table structure for `logs`
-- ----------------------------
DROP TABLE IF EXISTS `logs`;
CREATE TABLE `logs` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `workflow_id` varchar(255) NOT NULL,
  `log` text,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of logs
-- ----------------------------

-- ----------------------------
-- Table structure for `module`
-- ----------------------------
DROP TABLE IF EXISTS `module`;
CREATE TABLE `module` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `m_id` varchar(255) NOT NULL,
  `p_id` int(11) NOT NULL,
  `name` varchar(255) NOT NULL,
  `path` varchar(255) NOT NULL,
  `workdir` varchar(255) NOT NULL,
  `start_time` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE CURRENT_TIMESTAMP,
  `is_end` tinyint(4) NOT NULL DEFAULT '0',
  `end_time` timestamp NULL DEFAULT NULL,
  `is_error` tinyint(4) NOT NULL DEFAULT '0',
  `error` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `p_id_index` (`p_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of module
-- ----------------------------

-- ----------------------------
-- Table structure for `tool`
-- ----------------------------
DROP TABLE IF EXISTS `tool`;
CREATE TABLE `tool` (
  `id` int(11) NOT NULL,
  `t_id` varchar(255) NOT NULL,
  `p_id` int(11) NOT NULL,
  `p_type` varchar(255) NOT NULL,
  `name` varchar(255) NOT NULL,
  `path` varchar(255) NOT NULL,
  `workdir` varchar(255) NOT NULL,
  `start_time` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE CURRENT_TIMESTAMP,
  `run_time` timestamp NULL DEFAULT NULL,
  `is_end` tinyint(4) NOT NULL DEFAULT '0',
  `end_time` timestamp NULL DEFAULT NULL,
  `is_error` tinyint(4) NOT NULL DEFAULT '0',
  `error` varchar(255) DEFAULT NULL,
  `host` varchar(255) NOT NULL,
  `jobid` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `p_id` (`p_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of tool
-- ----------------------------

-- ----------------------------
-- Table structure for `tostop`
-- ----------------------------
DROP TABLE IF EXISTS `tostop`;
CREATE TABLE `tostop` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `workflow_id` varchar(255) NOT NULL,
  `reson` varchar(255) NOT NULL,
  `client` varchar(255) NOT NULL,
  `time` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  `ip` varchar(255) DEFAULT NULL,
  `done` tinyint(4) NOT NULL DEFAULT '0',
  `stoptime` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of tostop
-- ----------------------------

-- ----------------------------
-- Table structure for `workflow`
-- ----------------------------
DROP TABLE IF EXISTS `workflow`;
CREATE TABLE `workflow` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `client` varchar(255) NOT NULL,
  `workflow_id` varchar(255) NOT NULL,
  `json` text NOT NULL,
  `ip` varchar(255) DEFAULT NULL,
  `add_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `has_run` tinyint(4) NOT NULL DEFAULT '0',
  `run_time` timestamp NULL DEFAULT NULL,
  `is_end` tinyint(4) NOT NULL DEFAULT '0',
  `end_time` timestamp NULL DEFAULT NULL,
  `server` varchar(255) DEFAULT NULL,
  `last_update` timestamp NULL DEFAULT NULL,
  `is_error` tinyint(4) DEFAULT '0',
  `error` varchar(255) DEFAULT NULL,
  `output` varchar(255) DEFAULT NULL,
  `pid` int(11) DEFAULT NULL,
  `workdir` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `workflow_id_index` (`workflow_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=38 DEFAULT CHARSET=utf8;
