

# Hippo

[![Build Status](https://travis-ci.org/jiayuasu/hippo-postgresql.svg?branch=master)](https://travis-ci.org/jiayuasu/hippo-postgresql)

``PostgreSQL version: 9.6.1``

Hippo is a fast, yet scalable, sparse database indexing approach. In contrast to existing tree index structures, Hippo avoids storing a pointer to each tuple in the indexed table to reduce the storage space occupied by the index. Hippo only stores disk page ranges that represent the indexed database table and maintains histogram-based summaries for the page ranges. The summaries are brief histograms which represent the data distribution of one or more pages. The main contributions of Hippo are as follows:

<img src="http://faculty.engineering.asu.edu/sarwat/wp-content/uploads/2016/04/hippo-logo-2.png" width="250" align="right" >

* **Low Indexing Overhead**
 
* **Competitive Query Performance**
 
* **Fast Index Maintenance**



# Play around with Hippo index

For the ease of testing, we have implemented Hippo index into PostgreSQL kernel as one of the backend access methods. This verision has been tested on Ubuntu Linux LTS 14.04.


## Download the source code
```
$ git clone https://github.com/DataSystemsLab/hippo-postgresql.git
```
## Build and Installation
Once you've synced with GitHub, the folder should contain the source code for PostgreSQL. The build and installation steps are exactly same with official PostgreSQL.

```
$ cd SourceFolder
$ ./configure
$ make
$ su
$ make install
$ adduser postgres
$ mkdir /usr/local/pgsql/data
$ chown postgres /usr/local/pgsql/data
$ su - postgres
$ /usr/local/pgsql/bin/initdb -D /usr/local/pgsql/data
$ /usr/local/pgsql/bin/postgres -D /usr/local/pgsql/data >logfile 2>&1 &
$ /usr/local/pgsql/bin/createdb test
$ /usr/local/pgsql/bin/psql test
```

You may need to install some required packages to pass the installation if use Ubuntu 14.04. Please try the following command:

```
$ sudo apt-get install build-essential libreadline-dev zlib1g-dev flex bison libxml2-dev libxslt-dev libssl-dev

```



## PostgreSQL Regression Test

After the installation, you have to make sure the source code on your machine pass all the PostgreSQL Regression Tests (168 in total).

```
$ cd SourceFolder

$ make check
```

## Usage in SQL

Here list some SQL commands of Hippo index. For more details, please see the following Hippo index test SQL script:
```
./src/test/regress/sql/hippo.sql (Default)

./src/test/regress/sql/hippo_random.sql
```

### Build Hippo
```
ALTER TABLE hippo_tbl ALTER COLUMN randomNumber SET STATISTICS 500;

ANALYZE hippo_tbl;

CREATE INDEX hippo_idx ON hippo_tbl USING hippo(randomNumber) WITH (density = 20);

```

### Query Hippo

```
SELECT * FROM hippo_tbl WHERE randomNumber > 1000 AND randomNumber < 2000;
```

### Insert new records into Hippo

```
INSERT INTO hippo_tbl ... ... ...;
```

### Delete old records from Hippo

```
DELETE FROM hippo_tbl WHERE randomNumber > 1000 AND randomNumber < 2000;

VACUUM;
```

### Drop Hippo
```
DROP INDEX hippo_idx;
```
### Currently supported data type

Integer

### Currently supported operator

```
<, <=, =, >=, >
```



## Notes

Please read Hippo index test SQL script for some examples.

* Read and change Hippo index test SQL script:

```
./src/test/regress/sql/hippo.sql (Default)

./src/test/regress/sql/hippo_random.sql
```
* View Hippo index test SQL script output:

```
./src/test/regress/results/hippo.out (Default)

./src/test/regress/results/hippo_random.out
```

# Hippo Video Demonstration
Want to have a try? Do not hesitate! 

Watch this video (No need for headsets) and learn how to get started: [Hippo Video Demonstration (on remote computer)](http://www.public.asu.edu/~jiayu2/hippo/hippo-icde17-demo-video.mp4) or [Hippo Video Demonstration (on Youtube)](https://www.youtube.com/watch?v=wWaOK2-9k9A).

# Publication

* **Hippo in Action: Scalable Indexing of a Billion New York City Taxi Trips and Beyond** (Demo paper) [(PDF)](http://www.public.asu.edu/~jiayu2/hippo/publication/ICDE17_demo_739.pdf)

  Jia Yu, Raha Moraffah, Mohamed Sarwat. In Proceedings of the IEEE International Conference on Data Engineering, ICDE 2017, San Diego, CA, USA, April 2017
  
*  **Two Birds, One Stone: A Fast, yet Lightweight, Indexing Scheme for Modern Database Systems** (Research paper) [(PDF)](http://www.vldb.org/pvldb/vol10/p385-yu.pdf)

  Jia Yu, Mohamed Sarwat. In proceedings of the Very Large Data Bases Endowment, PVLDB 2016, Volume 10, Issue 4.  To be presented in VLDB 2017, Munich, Germany, August 2017

# Contact

## Contributors
* [Jia Yu](http://www.public.asu.edu/~jiayu2/) (Email: jiayu2@asu.edu)

* [Mohamed Sarwat](http://faculty.engineering.asu.edu/sarwat/) (Email: msarwat@asu.edu)

## DataSys Lab
Hippo index is one of the projects under [DataSys Lab](http://www.datasyslab.org/) at Arizona State University. The mission of DataSys Lab is designing and developing experimental data management systems (e.g., database systems).

***
# PostgreSQL Database Management System


This directory contains the source code distribution of the PostgreSQL
database management system.

PostgreSQL is an advanced object-relational database management system
that supports an extended subset of the SQL standard, including
transactions, foreign keys, subqueries, triggers, user-defined types
and functions.  This distribution also contains C language bindings.

PostgreSQL has many language interfaces, many of which are listed here:

	http://www.postgresql.org/download

See the file INSTALL for instructions on how to build and install
PostgreSQL.  That file also lists supported operating systems and
hardware platforms and contains information regarding any other
software packages that are required to build or run the PostgreSQL
system.  Copyright and license information can be found in the
file COPYRIGHT.  A comprehensive documentation set is included in this
distribution; it can be read as described in the installation
instructions.

The latest version of this software may be obtained at
http://www.postgresql.org/download/.  For more information look at our
web site located at http://www.postgresql.org/.