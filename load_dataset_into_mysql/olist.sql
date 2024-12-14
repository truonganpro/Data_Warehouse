SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS payments;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS sellers;
DROP TABLE IF EXISTS geolocation;
DROP TABLE IF EXISTS order_reviews;
DROP TABLE IF EXISTS product_category_name_translation;

SET FOREIGN_KEY_CHECKS = 1;
-- Tạo cơ sở dữ liệu nếu chưa tồn tại
CREATE DATABASE IF NOT EXISTS olist;
USE olist;

-- Xóa và tạo lại bảng `product_category_name_translation`
DROP TABLE IF EXISTS product_category_name_translation;
CREATE TABLE product_category_name_translation (
    product_category_name VARCHAR(64),
    product_category_name_english VARCHAR(64),
    PRIMARY KEY (product_category_name)
);

-- Xóa và tạo lại bảng `geolocation`
DROP TABLE IF EXISTS geolocation;
CREATE TABLE geolocation (
    geolocation_zip_code_prefix INT,
    geolocation_lat FLOAT,
    geolocation_lng FLOAT,
    geolocation_city NVARCHAR(64),
    geolocation_state VARCHAR(64)
);

-- Xóa và tạo lại bảng `sellers`
DROP TABLE IF EXISTS sellers;
CREATE TABLE sellers (
    seller_id VARCHAR(64),
    seller_zip_code_prefix INT,
    seller_city VARCHAR(64),
    seller_state VARCHAR(64),
    PRIMARY KEY (seller_id)
);

-- Xóa và tạo lại bảng `customers`
DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
    customer_id VARCHAR(64),
    customer_unique_id VARCHAR(32),
    customer_zip_code_prefix INT,
    customer_city VARCHAR(64),
    customer_state VARCHAR(64),
    PRIMARY KEY (customer_id)
);

-- Xóa và tạo lại bảng `products`
DROP TABLE IF EXISTS products;
CREATE TABLE products (
    product_id VARCHAR(64),
    product_category_name VARCHAR(64),
    product_name_length INT,
    product_description_length FLOAT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm FLOAT,
    product_height_cm FLOAT,
    product_width_cm FLOAT,
    PRIMARY KEY (product_id),
    FOREIGN KEY (product_category_name) REFERENCES product_category_name_translation(product_category_name)
);

-- Xóa và tạo lại bảng `orders`
DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    order_id VARCHAR(64),
    customer_id VARCHAR(64),
    order_status VARCHAR(32),
    order_purchase_timestamp DATETIME,
    order_approved_at DATETIME,
    order_delivered_carrier_date DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATETIME,
    PRIMARY KEY (order_id),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Xóa và tạo lại bảng `order_items`
DROP TABLE IF EXISTS order_items;
CREATE TABLE order_items (
    order_id VARCHAR(64),
    order_item_id INT,
    product_id VARCHAR(64),
    seller_id VARCHAR(64),
    shipping_limit_date DATETIME,
    price FLOAT,
    freight_value FLOAT,
    PRIMARY KEY (order_id, order_item_id),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (seller_id) REFERENCES sellers(seller_id)
);

-- Xóa và tạo lại bảng `payments`
DROP TABLE IF EXISTS payments;
CREATE TABLE payments (
    order_id VARCHAR(64),
    payment_sequential INT,
    payment_type VARCHAR(32),
    payment_installments INT,
    payment_value FLOAT,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- Xóa và tạo lại bảng `order_reviews`
DROP TABLE IF EXISTS order_reviews;
CREATE TABLE order_reviews (
    review_id VARCHAR(64),
    order_id VARCHAR(64),
    review_score INT,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date DATETIME,
    review_answer_timestamp DATETIME,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);