-- Create the transactionsdb database if it doesn't exist
CREATE DATABASE IF NOT EXISTS transactionsdb;

-- Use the transactionsdb database
USE transactionsdb;

-- Create the transactions table
CREATE EXTERNAL TABLE IF NOT EXISTS transactions (
    transaction_id STRING,
    date_time TIMESTAMP,
    amount DOUBLE,
    currency STRING,
    merchant_details STRING,
    customer_id STRING,
    transaction_type STRING,
    location STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:8020/user/hive/warehouse/transactionsdb.db/transactions';

-- Create the customers table
CREATE EXTERNAL TABLE IF NOT EXISTS customers (
    customer_id STRING,
    account_history STRING,
    age INT,
    location STRING,
    behavioral_pattern_avg DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:8020/user/hive/warehouse/transactionsdb.db/customers';

-- Create the cust_external_data table
CREATE EXTERNAL TABLE IF NOT EXISTS cust_external_data (
    customer_id STRING,
    credit_score INT,
    fraud_report INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:8020/user/hive/warehouse/transactionsdb.db/cust_external_data';


-- Create the blacklist table
CREATE EXTERNAL TABLE IF NOT EXISTS blacklist (
    blacklist_info STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:8020/user/hive/warehouse/transactionsdb.db/blacklist';