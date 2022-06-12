set hive.mapred.mode=nonstrict;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;

create table `masking_test` as select cast(key as int) as key, value from src;

create view `v0` as select * from `masking_test`;

explain
select * from `v0`;

select * from `v0`;

create table `masking_test_subq` as select cast(key as int) as key, value from src;

create view `v1` as select * from `masking_test_subq`;

explain
select * from `v1`
limit 20;

select * from `v1`
limit 20;

create view `masking_test_view` as select key from `v0`;

explain
select key from `masking_test_view`;

select key from `masking_test_view`;

explain
select `v0`.value from `v0` join `masking_test_view` on `v0`.key = `masking_test_view`.key;

select `v0`.value from `v0` join `masking_test_view` on `v0`.key = `masking_test_view`.key;
