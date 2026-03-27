-- ============================================================
-- Flink SQL Streaming Job
-- Kafka (JSON) -> Star Schema -> PostgreSQL
-- ============================================================

SET 'execution.checkpointing.interval' = '10s';
SET 'parallelism.default' = '1';

-- ============================================================
-- Source: Kafka topic with raw JSON messages
-- ============================================================
CREATE TABLE kafka_source (
    `id`                     INT,
    `customer_first_name`    STRING,
    `customer_last_name`     STRING,
    `customer_age`           INT,
    `customer_email`         STRING,
    `customer_country`       STRING,
    `customer_postal_code`   STRING,
    `customer_pet_type`      STRING,
    `customer_pet_name`      STRING,
    `customer_pet_breed`     STRING,
    `seller_first_name`      STRING,
    `seller_last_name`       STRING,
    `seller_email`           STRING,
    `seller_country`         STRING,
    `seller_postal_code`     STRING,
    `product_name`           STRING,
    `product_category`       STRING,
    `product_price`          DOUBLE,
    `product_quantity`       INT,
    `sale_date`              STRING,
    `sale_customer_id`       INT,
    `sale_seller_id`         INT,
    `sale_product_id`        INT,
    `sale_quantity`          INT,
    `sale_total_price`       DOUBLE,
    `store_name`             STRING,
    `store_location`         STRING,
    `store_city`             STRING,
    `store_state`            STRING,
    `store_country`          STRING,
    `store_phone`            STRING,
    `store_email`            STRING,
    `pet_category`           STRING,
    `product_weight`         DOUBLE,
    `product_color`          STRING,
    `product_size`           STRING,
    `product_brand`          STRING,
    `product_material`       STRING,
    `product_description`    STRING,
    `product_rating`         DOUBLE,
    `product_reviews`        INT,
    `product_release_date`   STRING,
    `product_expiry_date`    STRING,
    `supplier_name`          STRING,
    `supplier_contact`       STRING,
    `supplier_email`         STRING,
    `supplier_phone`         STRING,
    `supplier_address`       STRING,
    `supplier_city`          STRING,
    `supplier_country`       STRING
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'mock_data',
    'properties.bootstrap.servers'  = 'kafka:9092',
    'properties.group.id'           = 'flink-star-schema',
    'format'                        = 'json',
    'scan.startup.mode'             = 'earliest-offset',
    'json.fail-on-missing-field'    = 'false',
    'json.ignore-parse-errors'      = 'true'
);

-- ============================================================
-- Sinks: JDBC tables (upsert via PRIMARY KEY)
-- ============================================================

CREATE TABLE dim_customer_sink (
    `customer_id`  INT,
    `first_name`   STRING,
    `last_name`    STRING,
    `age`          INT,
    `email`        STRING,
    `country`      STRING,
    `postal_code`  STRING,
    `pet_type`     STRING,
    `pet_name`     STRING,
    `pet_breed`    STRING,
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url'       = 'jdbc:postgresql://postgres:5432/starschema',
    'table-name'= 'dim_customer',
    'driver'    = 'org.postgresql.Driver',
    'username'  = 'admin',
    'password'  = 'admin'
);

CREATE TABLE dim_seller_sink (
    `seller_id`    INT,
    `first_name`   STRING,
    `last_name`    STRING,
    `email`        STRING,
    `country`      STRING,
    `postal_code`  STRING,
    PRIMARY KEY (seller_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url'       = 'jdbc:postgresql://postgres:5432/starschema',
    'table-name'= 'dim_seller',
    'driver'    = 'org.postgresql.Driver',
    'username'  = 'admin',
    'password'  = 'admin'
);

CREATE TABLE dim_product_sink (
    `product_id`   INT,
    `name`         STRING,
    `category`     STRING,
    `price`        DOUBLE,
    `weight`       DOUBLE,
    `color`        STRING,
    `size`         STRING,
    `brand`        STRING,
    `material`     STRING,
    `description`  STRING,
    `rating`       DOUBLE,
    `reviews`      INT,
    `release_date` STRING,
    `expiry_date`  STRING,
    `pet_category` STRING,
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url'       = 'jdbc:postgresql://postgres:5432/starschema',
    'table-name'= 'dim_product',
    'driver'    = 'org.postgresql.Driver',
    'username'  = 'admin',
    'password'  = 'admin'
);

CREATE TABLE dim_store_sink (
    `store_name`  STRING,
    `location`    STRING,
    `city`        STRING,
    `state`       STRING,
    `country`     STRING,
    `phone`       STRING,
    `email`       STRING,
    PRIMARY KEY (store_name) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url'       = 'jdbc:postgresql://postgres:5432/starschema',
    'table-name'= 'dim_store',
    'driver'    = 'org.postgresql.Driver',
    'username'  = 'admin',
    'password'  = 'admin'
);

CREATE TABLE dim_supplier_sink (
    `supplier_name` STRING,
    `contact`       STRING,
    `email`         STRING,
    `phone`         STRING,
    `address`       STRING,
    `city`          STRING,
    `country`       STRING,
    PRIMARY KEY (supplier_name) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url'       = 'jdbc:postgresql://postgres:5432/starschema',
    'table-name'= 'dim_supplier',
    'driver'    = 'org.postgresql.Driver',
    'username'  = 'admin',
    'password'  = 'admin'
);

CREATE TABLE fact_sales_sink (
    `customer_id`      INT,
    `seller_id`        INT,
    `product_id`       INT,
    `store_name`       STRING,
    `supplier_name`    STRING,
    `sale_date`        STRING,
    `quantity`         INT,
    `total_price`      DOUBLE,
    `product_quantity` INT
) WITH (
    'connector' = 'jdbc',
    'url'       = 'jdbc:postgresql://postgres:5432/starschema',
    'table-name'= 'fact_sales',
    'driver'    = 'org.postgresql.Driver',
    'username'  = 'admin',
    'password'  = 'admin'
);

-- ============================================================
-- Streaming transformations: flat JSON -> star schema
-- ============================================================
EXECUTE STATEMENT SET
BEGIN

INSERT INTO dim_customer_sink
SELECT sale_customer_id,
       customer_first_name,
       customer_last_name,
       customer_age,
       customer_email,
       customer_country,
       customer_postal_code,
       customer_pet_type,
       customer_pet_name,
       customer_pet_breed
FROM   kafka_source;

INSERT INTO dim_seller_sink
SELECT sale_seller_id,
       seller_first_name,
       seller_last_name,
       seller_email,
       seller_country,
       seller_postal_code
FROM   kafka_source;

INSERT INTO dim_product_sink
SELECT sale_product_id,
       product_name,
       product_category,
       product_price,
       product_weight,
       product_color,
       product_size,
       product_brand,
       product_material,
       product_description,
       product_rating,
       product_reviews,
       product_release_date,
       product_expiry_date,
       pet_category
FROM   kafka_source;

INSERT INTO dim_store_sink
SELECT store_name,
       store_location,
       store_city,
       store_state,
       store_country,
       store_phone,
       store_email
FROM   kafka_source;

INSERT INTO dim_supplier_sink
SELECT supplier_name,
       supplier_contact,
       supplier_email,
       supplier_phone,
       supplier_address,
       supplier_city,
       supplier_country
FROM   kafka_source;

INSERT INTO fact_sales_sink
SELECT sale_customer_id,
       sale_seller_id,
       sale_product_id,
       store_name,
       supplier_name,
       sale_date,
       sale_quantity,
       sale_total_price,
       product_quantity
FROM   kafka_source;

END;
