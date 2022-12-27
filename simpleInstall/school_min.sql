
-- 创建表student
CREATE TABLE student
(
    std_id INT ,
    std_name VARCHAR(20) NOT NULL,
    std_sex VARCHAR(6),
    std_birth DATE,
    std_in DATE NOT NULL,
    std_address VARCHAR(100)
);

-- 创建表teacher
CREATE TABLE teacher
(
    tec_id INT,
    tec_name VARCHAR(20) NOT NULL,
    tec_job VARCHAR(15),
    tec_sex VARCHAR(6),
    tec_age INT,
    tec_in DATE NOT NULL
);

-- 创建表class
CREATE TABLE class
(
    cla_id INT,
    cla_name VARCHAR(20) NOT NULL,
    cla_teacher INT NOT NULL
);

-- 创建表school_department
CREATE TABLE school_department
(
    depart_id INT,
    depart_name VARCHAR(30) NOT NULL,
    depart_teacher INT NOT NULL
);

-- 创建表course
CREATE TABLE course
(
    cor_id INT,
    cor_name VARCHAR(30) NOT NULL,
    cor_type VARCHAR(20),
    credit DOUBLE PRECISION
);

-- 插入数据
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (1,'张一','男','1993-01-01','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (2,'张二','男','1993-01-02','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (3,'张三','男','1993-01-03','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (4,'张四','男','1993-01-04','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (5,'张五','男','1993-01-05','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (6,'张六','男','1993-01-06','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (7,'张七','男','1993-01-07','2011-09-01','江苏省南京市雨花台区');
INSERT INTO student(std_id,std_name,std_sex,std_birth,std_in,std_address) VALUES (8,'张八','男','1993-01-08','2011-09-01','江苏省南京市雨花台区');


INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (1,'张一','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (2,'张二','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (3,'张三','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (4,'张四','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (5,'张五','讲师','男',35,'2009-07-01');
INSERT INTO teacher(tec_id,tec_name,tec_job,tec_sex,tec_age,tec_in) VALUES (6,'张六','讲师','男',35,'2009-07-01');


INSERT INTO class(cla_id,cla_name,cla_teacher) VALUES (1,'计算机',1);
INSERT INTO class(cla_id,cla_name,cla_teacher) VALUES (2,'自动化',3);
INSERT INTO class(cla_id,cla_name,cla_teacher) VALUES (3,'飞行器设计',5);
INSERT INTO class(cla_id,cla_name,cla_teacher) VALUES (4,'大学物理',7);
INSERT INTO class(cla_id,cla_name,cla_teacher) VALUES (5,'高等数学',9);
INSERT INTO class(cla_id,cla_name,cla_teacher) VALUES (6,'大学化学',12);
INSERT INTO class(cla_id,cla_name,cla_teacher) VALUES (7,'表演',14);
INSERT INTO class(cla_id,cla_name,cla_teacher) VALUES (8,'服装设计',16);
INSERT INTO class(cla_id,cla_name,cla_teacher) VALUES (9,'工业设计',18);

INSERT INTO school_department(depart_id,depart_name,depart_teacher) VALUES (1,'计算机学院',2);
INSERT INTO school_department(depart_id,depart_name,depart_teacher) VALUES (2,'自动化学院',4);
INSERT INTO school_department(depart_id,depart_name,depart_teacher) VALUES (3,'航空宇航学院',6);
INSERT INTO school_department(depart_id,depart_name,depart_teacher) VALUES (4,'艺术学院',8);
INSERT INTO school_department(depart_id,depart_name,depart_teacher) VALUES (5,'理学院',11);
INSERT INTO school_department(depart_id,depart_name,depart_teacher) VALUES (6,'人工智能学院',13);
INSERT INTO school_department(depart_id,depart_name,depart_teacher) VALUES (7,'工学院',15);
INSERT INTO school_department(depart_id,depart_name,depart_teacher) VALUES (8,'管理学院',17);
INSERT INTO school_department(depart_id,depart_name,depart_teacher) VALUES (9,'农学院',22);
INSERT INTO school_department(depart_id,depart_name,depart_teacher) VALUES (10,'医学院',28);

INSERT INTO course(cor_id,cor_name,cor_type,credit) VALUES (1,'数据库系统概论','必修',3);
INSERT INTO course(cor_id,cor_name,cor_type,credit) VALUES (2,'艺术设计概论','选修',1);
INSERT INTO course(cor_id,cor_name,cor_type,credit) VALUES (3,'力学制图','必修',4);
INSERT INTO course(cor_id,cor_name,cor_type,credit) VALUES (4,'飞行器设计历史','选修',1);
INSERT INTO course(cor_id,cor_name,cor_type,credit) VALUES (5,'马克思主义','必修',2);
INSERT INTO course(cor_id,cor_name,cor_type,credit) VALUES (6,'大学历史','必修',2);
INSERT INTO course(cor_id,cor_name,cor_type,credit) VALUES (7,'人力资源管理理论','必修',2.5);
INSERT INTO course(cor_id,cor_name,cor_type,credit) VALUES (8,'线性代数','必修',4);
INSERT INTO course(cor_id,cor_name,cor_type,credit) VALUES (9,'JAVA程序设计','必修',3);
