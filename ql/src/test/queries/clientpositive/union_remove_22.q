set hive.mapred.mode=nonstrict;
set hive.stats.autogather=false;
set hive.optimize.union.remove=true;

set hive.merge.sparkfiles=false;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;
set mapred.input.dir.recursive=true;

-- SORT_QUERY_RESULTS

-- This is to test the union->selectstar->filesink optimization
-- Union of 2 map-reduce subqueries is performed followed by select and a file sink
-- However, some columns are repeated. So, union cannot be removed.
-- It does not matter, whether the output is merged or not. In this case, merging is turned
-- off
-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.23)
-- Since this test creates sub-directories for the output table outputTbl1, it might be easier
-- to run the test only on hadoop 23. The union is removed, the select (which selects columns from
-- both the sub-qeuries of the union) is pushed above the union.

create table inputTbl1(key string, val string) stored as textfile;
create table outputTbl1(key string, `values` bigint, values2 bigint) stored as textfile;

load data local inpath '../../data/files/T1.txt' into table inputTbl1;

explain
insert overwrite table outputTbl1
SELECT a.key, a.`values`, a.`values`
FROM (
  SELECT key, count(1) as `values` from inputTbl1 group by key
  UNION ALL
  SELECT key, count(1) as `values` from inputTbl1 group by key
) a;

insert overwrite table outputTbl1
SELECT a.key, a.`values`, a.`values`
FROM (
  SELECT key, count(1) as `values` from inputTbl1 group by key
  UNION ALL
  SELECT key, count(1) as `values` from inputTbl1 group by key
) a;

desc formatted outputTbl1;

select * from outputTbl1;

explain
insert overwrite table outputTbl1
SELECT a.key, concat(a.`values`, a.`values`), concat(a.`values`, a.`values`)
FROM (
  SELECT key, count(1) as `values` from inputTbl1 group by key
  UNION ALL
  SELECT key, count(1) as `values` from inputTbl1 group by key
) a;

insert overwrite table outputTbl1
SELECT a.key, concat(a.`values`, a.`values`), concat(a.`values`, a.`values`)
FROM (
  SELECT key, count(1) as `values` from inputTbl1 group by key
  UNION ALL
  SELECT key, count(1) as `values` from inputTbl1 group by key
) a;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
select * from outputTbl1;
