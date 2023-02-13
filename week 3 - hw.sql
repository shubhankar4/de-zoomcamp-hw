-- Creating fhv_external_table
CREATE OR REPLACE EXTERNAL TABLE `taxi-rides-ny.fhv.external_fhv_tripdata`
    OPTIONS (
      format = 'CSV',
      uris = ['gs://dtc_data_lake_taxi-rides-ny/fhv/fhv_tripdata_2019-*.csv.gz']
);

-- Q1
SELECT COUNT(*) FROM taxi-rides-ny.fhv.external_fhv_tripdata;

-- Q2
-- Query on External Table
SELECT COUNT(*) FROM taxi-rides-ny.fhv.external_fhv_tripdata;


CREATE OR REPLACE TABLE taxi-rides-ny.fhv.fhv_tripdata AS
SELECT * FROM taxi-rides-ny.fhv.external_fhv_tripdata;

-- Query on Materialized Table
SELECT COUNT(*) FROM taxi-rides-ny.fhv.fhv_tripdata;

-- Q3
SELECT COUNT(*)
FROM taxi-rides-ny.fhv.fhv_tripdata
WHERE PUlocationID IS NULL AND DOlocationID IS NULL;

-- Q4
-- Partition by `pickup_datetime` and Cluster on `affiliated_base_number`

-- Q5 Non Partitioned Table
SELECT DISTINCT(affiliated_base_number)
FROM taxi-rides-ny.fhv.fhv_tripdata
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

-- Q5 Partitioned table
CREATE OR REPLACE TABLE taxi-rides-ny.fhv.fhv_tripdata_partitioned
PARTITION BY
  DATE(pickup_datetime) AS
SELECT * FROM taxi-rides-ny.fhv.fhv_tripdata;

SELECT DISTINCT(affiliated_base_number)
FROM taxi-rides-ny.fhv.fhv_tripdata_partitioned
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

-- Q6
-- Big Query

-- Q7
-- False - Tables <1GB don't show significant improvement with partitioning and clustering;
--         doing so in a small table could even lead to increased cost due to the additional
--         metadata reads and maintenance needed for these features.
