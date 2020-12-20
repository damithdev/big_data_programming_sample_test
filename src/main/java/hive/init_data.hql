create database matchdb;

use matchdb;

create external table matchdata(matchdate string,home_team string,away_team string,home_score int, away_score int,tourname string,city string, country string,netrual string) row format delimited fields terminated by ',' stored as textfile;

load data inpath 'hdfs://82f592dc7597:9000/matchresults/results_copy_1.csv' into table matchdata;