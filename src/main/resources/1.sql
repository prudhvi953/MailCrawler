create database if not exists test;
create table link (
  url varchar(2000) default null,
  isDownloaded tinyint(1) default null,
  urlid int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (urlid)
);
