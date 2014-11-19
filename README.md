MailCrawler - For crawling and downloading mails

Quickstart guide

0. Requirements

To run MailCrawler you need:
 - Maven
 - MySQL server
 
1. Install maven which can be downloaded from http://maven.apache.org/download.cgi
2. Install MySQL server which will be available at http://www.mysql.com/downloads/
  Make sure that MySQL server is running
3. Create required database and table for running application SQL can be found in resources directory.
4. Change the logger file name in log4j.properties in resources directory.
5. Provide credentials for connecting to database in db.properties file in conf direcotry.
5. run maven package
