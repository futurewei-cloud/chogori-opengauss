
SET AUTOCOMMIT TO ON;

-- 创建表student
CREATE TABLE student
(
    std_id INT PRIMARY KEY,
    std_name VARCHAR(20) NOT NULL,
    std_sex VARCHAR(6),
    std_birth DATE,
    std_in DATE NOT NULL,
    std_address VARCHAR(100)
);

-- 创建表teacher
CREATE TABLE teacher
(
    tec_id INT PRIMARY KEY,
    tec_name VARCHAR(20) NOT NULL,
    tec_job VARCHAR(15),
    tec_sex VARCHAR(6),
    tec_age INT,
    tec_in DATE NOT NULL
);

-- 创建表class
CREATE TABLE class
(
    cla_id INT PRIMARY KEY,
    cla_name VARCHAR(20) NOT NULL,
    cla_teacher INT NOT NULL
);

-- 创建表school_department
CREATE TABLE school_department
(
    depart_id INT PRIMARY KEY,
    depart_name VARCHAR(30) NOT NULL,
    depart_teacher INT NOT NULL
);

-- 创建表course
CREATE TABLE course
(
    cor_id INT PRIMARY KEY,
    cor_name VARCHAR(30) NOT NULL,
    cor_type VARCHAR(20),
    credit DOUBLE PRECISION
);

-- 插入数据
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (1,'张一','男','1993-01-01','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (2,'张二','男','1993-01-02','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (3,'张三','男','1993-01-03','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (4,'张四','男','1993-01-04','2011-09-01','江苏省南京市雨花台区');

INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (1,'张一','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (2,'张二','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (3,'张三','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (4,'张四','讲师','男',35,'2009-07-01');

INSERT INTO class(cla_id,cla_name,cla_teacher) VALUES (1,'计算机',1);
INSERT INTO class(cla_id,cla_name,cla_teacher) VALUES (2,'自动化',3);
INSERT INTO class(cla_id,cla_name,cla_teacher) VALUES (3,'飞行器设计',5);
INSERT INTO class(cla_id,cla_name,cla_teacher) VALUES (4,'大学物理',7);

INSERT INTO school_department(depart_id,depart_name,depart_teacher) VALUES (1,'计算机学院',2);
INSERT INTO school_department(depart_id,depart_name,depart_teacher) VALUES (2,'自动化学院',4);
INSERT INTO school_department(depart_id,depart_name,depart_teacher) VALUES (3,'航空宇航学院',6);
INSERT INTO school_department(depart_id,depart_name,depart_teacher) VALUES (4,'艺术学院',8);

INSERT INTO course(cor_id,cor_name,cor_type,credit) VALUES (1,'数据库系统概论','必修',3);
INSERT INTO course(cor_id,cor_name,cor_type,credit) VALUES (2,'艺术设计概论','选修',1);
INSERT INTO course(cor_id,cor_name,cor_type,credit) VALUES (3,'力学制图','必修',4);
INSERT INTO course(cor_id,cor_name,cor_type,credit) VALUES (4,'飞行器设计历史','选修',1);

