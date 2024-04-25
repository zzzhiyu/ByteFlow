create database byte_flow;


-- {source_name}_{db_type}_{表名}_[df/di/ri/rf]会构成ods表，要进行规范
CREATE TABLE `byte_flow`.`data_source` (
    `source_name` varchar(64) NOT NULL COMMENT '源名称, 如销售源: sale',
    `db_type` varchar(40) NOT NULL COMMENT '游戏库类型, 如日志库: log',
    `server_id` int NOT NULL COMMENT '服务器ID(进行分库时,可以根据id区分库)',
    `desc` varchar(128) DEFAULT NULL COMMENT '描述信息',
    `engine` varchar(32) NOT NULL COMMENT '引擎类型(mysql, sqlserver)',
    `db_name` varchar(64) NOT NULL COMMENT '库名称',
    `host` varchar(128) NOT NULL COMMENT 'ip地址',
    `port` int NOT NULL COMMENT '端口',
    `user` varchar(64) NOT NULL COMMENT '用户',
    `passwd` varchar(64) NOT NULL COMMENT '密码',
    `flag` tinyint DEFAULT '0' COMMENT '数据库是否有效 0:无效, 1:有效',
    `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`source_name`,`db_type`,`server_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

-- 配置一行信息后，以后遇到对应的表,会自动生成对应的任务，无需要继续配置
CREATE TABLE `byte_flow`.`doris_table_conf` (
     `table_name` varchar(64) NOT NULL COMMENT '源表名称',
     `is_partition` tinyint(1) NOT NULL COMMENT '是否为分区表',
     `server_id_pk` tinyint(1) NOT NULL COMMENT '数据源服务器id是否为主键',
     `table_mode` varchar(32) NOT NULL COMMENT '表模型(unique key or duplicate key)',
     `partition_col` varchar(1024) DEFAULT NULL COMMENT '分区列(多列用,分开)',
     `time_update_col` varchar(1024) DEFAULT NULL COMMENT '更新列(多列用,分开)',
     `hash_bucket_col` varchar(1024) NOT NULL COMMENT 'hash分桶列(多列用,分开)',
     `bloom_filter_col` varchar(1024) DEFAULT NULL COMMENT '布隆过滤列(多列用,分开)',
     `time_unit` varchar(32) DEFAULT NULL COMMENT '分区时间单元: DAY,WEEK, MONTH',
     `bucket_num` int NOT NULL COMMENT '分桶数量',
     `is_truncate` tinyint(1) NOT NULL COMMENT '是否先进行清空后拉取',
     `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
     `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
     PRIMARY KEY (`table_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
