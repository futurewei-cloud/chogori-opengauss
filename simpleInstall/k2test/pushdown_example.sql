create table usersq02(name varchar(20), address text, age int, PRIMARY KEY(name));

insert into usersq02 values('user01', 'location-A', 25);
insert into usersq02 values('user02', 'location-B', 30);
insert into usersq02 values('user03', 'location-A', 25);
insert into usersq02 values('user04', 'location-A', 30);

select * from usersq02 where name = 'user01';
select * from usersq02 where name = 'user02';

Select * from usersq02 where age = 25;
select * from usersq02 where age = 30;
select * from usersq02 where age < 30;
select * from usersq02 where age <= 30;
Select * from usersq02 where age > 25;
Select * from usersq02 where age >= 25;
select * from usersq02 where age = 25 and name = 'user01';
