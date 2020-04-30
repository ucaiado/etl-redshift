Data Warehouse using AWS
===========================

This project is part of the [Data Engineering Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027) program, from Udacity. I manipulate data for a music streaming app called Sparkify, where I write an ETL pipeline that loads data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

Currently, the startup has grown its user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. This data initially  is placed in staging tables and, from them, the data is split into 5 new tables, each one containing parts of the data from the logs files:

- users: Dimension table. Users in the app.
- songs: Dimension table. Songs in music database.
- artists: Dimension table. Artists in music database.
- time: Dimension table. Timestamps of records in songplays broken down into units.
- songplays: Fact table. Records in log data associated with song plays.

The database schema proposed would help them analyze the data they’ve been collecting on songs and user activity on their app using simple SQL queries on the tables


### Install
To set up your python environment to run the code in this repository, start by
 creating a new environment with Anaconda and install the dependencies.

```shell
$ conda create --name ngym36 python=3.6
$ source activate ngym36
$ pip install -r requirements.txt
```

### Run
In a terminal or command window, navigate to the top-level project directory (that contains this README). You need to set up a [Redshift](https://aws.amazon.com/pt/redshift/) cluster. So, start by renaming the file `confs/dwh.template.cfg` to  `confs/dwh.cfg` and fill in the `KEY` and `SECRET` in the AWS section. Then, the following command:

```shell
$ python iac.py -i
$ python iac.py -r
$ watch -n 15 'python iac.py -s'
```

The above instructions are going to create the IAM role, the Redshift cluster, and check the status of this cluster every 15 seconds. Fill in the other fields from your `dwh.cfg` that shows up in the commands console outputs. After Amazon finally launch your cluster, run:

```shell
$ python iac.py -t
$ python create_tables.py
$ python etl.py
```

The first command opens a TCP port to your cluster so that you can manipulate data from outside. The second command creates all the tables, and the last one, insert all the data in the staging, dimension, and fact tables. Finally, CLEAN UP your resources using the commands below:

```shell
$ python iac.py -d
$ watch -n 15 'python iac.py -s'
```

Wait for the second command to fail to find the cluster. **Redshift is expensive.**

### License
The contents of this repository are covered under the [MIT License](LICENSE).
