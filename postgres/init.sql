CREATE TABLE dim_customer (
    customer_id   INT PRIMARY KEY,
    first_name    VARCHAR(100),
    last_name     VARCHAR(100),
    age           INT,
    email         VARCHAR(200),
    country       VARCHAR(100),
    postal_code   VARCHAR(20),
    pet_type      VARCHAR(50),
    pet_name      VARCHAR(100),
    pet_breed     VARCHAR(100)
);

CREATE TABLE dim_seller (
    seller_id     INT PRIMARY KEY,
    first_name    VARCHAR(100),
    last_name     VARCHAR(100),
    email         VARCHAR(200),
    country       VARCHAR(100),
    postal_code   VARCHAR(20)
);

CREATE TABLE dim_product (
    product_id    INT PRIMARY KEY,
    name          VARCHAR(200),
    category      VARCHAR(100),
    price         DOUBLE PRECISION,
    weight        DOUBLE PRECISION,
    color         VARCHAR(50),
    size          VARCHAR(20),
    brand         VARCHAR(100),
    material      VARCHAR(100),
    description   TEXT,
    rating        DOUBLE PRECISION,
    reviews       INT,
    release_date  VARCHAR(20),
    expiry_date   VARCHAR(20),
    pet_category  VARCHAR(50)
);

CREATE TABLE dim_store (
    store_name    VARCHAR(200) PRIMARY KEY,
    location      VARCHAR(200),
    city          VARCHAR(100),
    state         VARCHAR(100),
    country       VARCHAR(100),
    phone         VARCHAR(50),
    email         VARCHAR(200)
);

CREATE TABLE dim_supplier (
    supplier_name VARCHAR(200) PRIMARY KEY,
    contact       VARCHAR(200),
    email         VARCHAR(200),
    phone         VARCHAR(50),
    address       VARCHAR(200),
    city          VARCHAR(100),
    country       VARCHAR(100)
);

CREATE TABLE fact_sales (
    sale_id          SERIAL PRIMARY KEY,
    customer_id      INT,
    seller_id        INT,
    product_id       INT,
    store_name       VARCHAR(200),
    supplier_name    VARCHAR(200),
    sale_date        VARCHAR(20),
    quantity         INT,
    total_price      DOUBLE PRECISION,
    product_quantity INT
);
