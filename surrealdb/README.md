YCSB-surrealdb-binding
====================

Surrealdb database interface for YCSB

Installation guide
==================

* Download the YCSB project via the following URL https://github.com/brianfrankcooper/YCSB/archive/0.1.4.zip and unzip it. 
* Include the YCSB binding within the YCSB-0.1.4 directory: git clone https://github.com/arnaudsjs/YCSB-couchdb-binding.git couchdb
* Add <module>surrealdb</module> to the list of modules in YCSB-0.1.4/pom.xml
* Add the following lines to the DATABASE section in YCSB-0.1.4/bin/ycsb: "couchdb" : "surrealdb.SurrealdbClient"
* compile everything by executing the following command within the YCSB-0.17.0 directory: mvn clean package
