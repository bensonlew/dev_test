/*
Navicat MySQL Data Transfer

Source Server         : 192.168.10.51
Source Server Version : 50528
Source Host           : localhost:3306
Source Database       : biocluster

Target Server Type    : MYSQL
Target Server Version : 50528
File Encoding         : 65001

Date: 2015-10-20 17:50:44
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for command
-- ----------------------------
DROP TABLE IF EXISTS `command`;
CREATE TABLE `command` (  -- 命令表
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `t_id` int(11) NOT NULL,   -- tool id
  `w_id` int(11) NOT NULL,   -- workflow id
  `command` varchar(255) DEFAULT NULL,  -- 命令内容
  PRIMARY KEY (`id`),
  KEY `t_id_index` (`t_id`) USING BTREE,
  KEY `w_id_index` (`w_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for clientkey
-- ----------------------------
DROP TABLE IF EXISTS `clientkey`;
CREATE TABLE `clientkey` (   -- 远程客户端授权表
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `client` varchar(255) NOT NULL,   -- 客户端名称
  `key` varchar(255) DEFAULT NULL,  -- 客户授权key
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for module
-- ----------------------------
DROP TABLE IF EXISTS `module`;
CREATE TABLE `module` (  -- module
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `m_id` varchar(255) NOT NULL,  -- 运行中的id
  `p_id` int(11) NOT NULL,       -- 上级workflow id
  `name` varchar(255) NOT NULL,  -- fullname
  `path` varchar(255) NOT NULL,  --  path
  `workdir` varchar(255) NOT NULL, -- 工作目录
  `start_time` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE CURRENT_TIMESTAMP, -- 开始时间
  `is_end` tinyint(4) NOT NULL DEFAULT '0',  -- 是否运行结束
  `end_time` timestamp NULL DEFAULT NULL,   -- 运行结束时间
  `is_error` tinyint(4) NOT NULL DEFAULT '0',  -- 运行出错
  `error` varchar(255) DEFAULT NULL,  -- 错误信息
  PRIMARY KEY (`id`),
  KEY `p_id_index` (`p_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for tool
-- ----------------------------
DROP TABLE IF EXISTS `tool`;
CREATE TABLE `tool` (   -- tool
  `id` int(11) NOT NULL, 
  `t_id` varchar(255) NOT NULL,  -- 运行中的id
  `p_id` int(11) NOT NULL,  -- 上级workflow/module id
  `p_type` varchar(255) NOT NULL, -- 上级种类workflow/module
  `name` varchar(255) NOT NULL, -- fullname
  `path` varchar(255) NOT NULL, -- path
  `workdir` varchar(255) NOT NULL, -- 工作目录
  `start_time` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE CURRENT_TIMESTAMP, -- 开始时间 
  `run_time` timestamp NULL DEFAULT NULL, -- 远程开始允许时间
  `is_end` tinyint(4) NOT NULL DEFAULT '0',  -- 是否运行结束
  `end_time` timestamp NULL DEFAULT NULL, -- 运行结束时间
  `is_error` tinyint(4) NOT NULL DEFAULT '0', -- 运行出错
  `error` varchar(255) DEFAULT NULL, -- 错误信息
  `host` varchar(255) NOT NULL, -- 远程host名
  `jobid` varchar(255) DEFAULT NULL, -- jobid
  PRIMARY KEY (`id`),
  KEY `p_id` (`p_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for workflow
-- ----------------------------
DROP TABLE IF EXISTS `workflow`;
CREATE TABLE `workflow` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `client` varchar(255) NOT NULL,  -- 提交流程客户端
  `workflow_id` varchar(255) NOT NULL, -- 流程id
  `json` text NOT NULL,  -- json调用内容
  `add_time` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE CURRENT_TIMESTAMP, -- 添加时间
  `has_run` tinyint(4) NOT NULL DEFAULT '0',-- 是否开始运行
  `run_time` timestamp NULL DEFAULT NULL,  -- 开始运行时间
  `is_end` tinyint(4) NOT NULL DEFAULT '0', -- 是否允许结束
  `end_time` timestamp NULL DEFAULT NULL, -- 结束时间
  `server` varchar(255) DEFAULT NULL,  -- 运行服务器
  `last_update` timestamp NULL DEFAULT NULL, -- 最近更新时间 超过90s表示任务中断
  `is_error` tinyint(4) DEFAULT '0', -- 发生错误
  `error` varchar(255) DEFAULT NULL, -- 错误信息
  `output` varchar(255) DEFAULT NULL, -- 输出目录
  `ip` varchar(255) DEFAULT NULL, -- client ip地址
  `pid` varchar(255) DEFAULT NULL, -- 进程pid
  `workdir` varchar(255) DEFAULT NULL, -- 工作目录
  PRIMARY KEY (`id`),
  UNIQUE KEY `workflow_id_index` (`workflow_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
