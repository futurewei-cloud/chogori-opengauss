create database finance_min;

\c finance_min;

SET AUTOCOMMIT TO ON;

-- 创建表client
CREATE TABLE client
(
    c_id INT PRIMARY KEY,
    c_name VARCHAR(100) NOT NULL,
    c_mail CHAR(30),
    c_id_card CHAR(20) NOT NULL,
    c_phone CHAR(20) NOT NULL,
    c_password CHAR(20) NOT NULL
);

-- 创建表bank_card
CREATE TABLE bank_card
(
    b_number CHAR(30) PRIMARY KEY,
    b_type CHAR(20),
    b_c_id INT NOT NULL
);

-- 创建表finances_product
CREATE TABLE finances_product
(
    p_name VARCHAR(100) NOT NULL,
    p_id INT PRIMARY KEY,
    p_description CLOB,
    p_amount INT,
    p_year INT
);

-- 创建表insurance
CREATE TABLE insurance
(
    i_name VARCHAR(100) NOT NULL,
    i_id INT PRIMARY KEY,
    i_amount INT,
    i_person CHAR(20),
    i_year INT,
    i_project VARCHAR(200)
);

-- 创建表fund
CREATE TABLE fund
(
    f_name VARCHAR(100) NOT NULL,
    f_id INT PRIMARY KEY,
    f_type CHAR(20),
    f_amount INT,
    risk_level CHAR(20) NOT NULL,
    f_manager INT NOT NULL
);

-- 创建表property
CREATE TABLE property
(
    pro_c_id INT NOT NULL,
    pro_id INT PRIMARY KEY,
    pro_status CHAR(20),
    pro_quantity INT,
    pro_income INT,
    pro_purchase_time DATE
);

-- 插入数据
INSERT INTO client(c_id,c_name,c_mail,c_id_card,c_phone,c_password) VALUES (1,'张一','zhangyi@huawei.com','340211199301010001','18815650001','gaussdb_001');
INSERT INTO client(c_id,c_name,c_mail,c_id_card,c_phone,c_password) VALUES (2,'张二','zhanger@huawei.com','340211199301010002','18815650002','gaussdb_002');
INSERT INTO client(c_id,c_name,c_mail,c_id_card,c_phone,c_password) VALUES (3,'张三','zhangsan@huawei.com','340211199301010003','18815650003','gaussdb_003');
INSERT INTO client(c_id,c_name,c_mail,c_id_card,c_phone,c_password) VALUES (4,'张四','zhangsi@huawei.com','340211199301010004','18815650004','gaussdb_004');

INSERT INTO bank_card(b_number,b_type,b_c_id) VALUES ('6222021302020000001','信用卡',1);
INSERT INTO bank_card(b_number,b_type,b_c_id) VALUES ('6222021302020000002','信用卡',3);
INSERT INTO bank_card(b_number,b_type,b_c_id) VALUES ('6222021302020000003','信用卡',5);
INSERT INTO bank_card(b_number,b_type,b_c_id) VALUES ('6222021302020000004','信用卡',7);


INSERT INTO finances_product(p_name,p_id,p_description,p_amount,p_year) VALUES ('债券',1,'以国债、金融债、央行票据、企业债为主要投资方向的银行理财产品。',50000,6);
INSERT INTO finances_product(p_name,p_id,p_description,p_amount,p_year) VALUES ('信贷资产',2,'一般指银行作为委托人将通过发行理财产品募集资金委托给信托公司，信托公司作为受托人成立信托计划，将信托资产购买理财产品发售银行或第三方信贷资产。',50000,6);
INSERT INTO finances_product(p_name,p_id,p_description,p_amount,p_year) VALUES ('股票',3,'与股票挂钩的理财产品。目前市场上主要以港股挂钩居多',50000,6);

INSERT INTO insurance(i_name,i_id,i_amount,i_person,i_year,i_project) VALUES ('健康保险',1,2000,'老人',30,'平安保险');
INSERT INTO insurance(i_name,i_id,i_amount,i_person,i_year,i_project) VALUES ('人寿保险',2,3000,'老人',30,'平安保险');
INSERT INTO insurance(i_name,i_id,i_amount,i_person,i_year,i_project) VALUES ('意外保险',3,5000,'所有人',30,'平安保险');
INSERT INTO insurance(i_name,i_id,i_amount,i_person,i_year,i_project) VALUES ('医疗保险',4,2000,'所有人',30,'平安保险');

INSERT INTO fund(f_name,f_id,f_type,f_amount,risk_level,f_manager) VALUES ('股票',1,'股票型',10000,'高',1);
INSERT INTO fund(f_name,f_id,f_type,f_amount,risk_level,f_manager) VALUES ('投资',2,'债券型',10000,'中',2);
INSERT INTO fund(f_name,f_id,f_type,f_amount,risk_level,f_manager) VALUES ('国债',3,'货币型',10000,'低',3);
INSERT INTO fund(f_name,f_id,f_type,f_amount,risk_level,f_manager) VALUES ('沪深300指数',4,'指数型',10000,'中',4);

INSERT INTO property(pro_c_id,pro_id,pro_status,pro_quantity,pro_income,pro_purchase_time) VALUES (5,1,'可用',4,8000,'2018-07-01');
INSERT INTO property(pro_c_id,pro_id,pro_status,pro_quantity,pro_income,pro_purchase_time) VALUES (10,2,'可用',4,8000,'2018-07-01');
