-- Nạp dữ liệu cho bảng `product_category_name_translation`
LOAD DATA LOCAL INFILE '/tmp/dataset/product_category_name_translation.csv'
INTO TABLE product_category_name_translation
FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Nạp dữ liệu cho bảng `sellers`
LOAD DATA LOCAL INFILE '/tmp/dataset/olist_sellers_dataset.csv'
INTO TABLE sellers
FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Nạp dữ liệu cho bảng `customers`
LOAD DATA LOCAL INFILE '/tmp/dataset/olist_customers_dataset.csv'
INTO TABLE customers
FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Nạp dữ liệu cho bảng `products`
LOAD DATA LOCAL INFILE '/tmp/dataset/olist_products_dataset.csv'
INTO TABLE products
FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Nạp dữ liệu cho bảng `orders`
LOAD DATA LOCAL INFILE '/tmp/dataset/olist_orders_dataset.csv'
INTO TABLE orders
FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Nạp dữ liệu cho bảng `order_items`
SET FOREIGN_KEY_CHECKS = 0;

LOAD DATA LOCAL INFILE '/tmp/dataset/olist_order_items_dataset.csv'
INTO TABLE order_items
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SET FOREIGN_KEY_CHECKS = 1;

-- Nạp dữ liệu cho bảng `payments`
SET FOREIGN_KEY_CHECKS = 0;
LOAD DATA LOCAL INFILE '/tmp/dataset/olist_order_payments_dataset.csv'
INTO TABLE payments
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
SET FOREIGN_KEY_CHECKS = 1;

-- Nạp dữ liệu cho bảng `order_reviews`
LOAD DATA LOCAL INFILE '/tmp/dataset/olist_order_reviews_dataset.csv'
INTO TABLE order_reviews
FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Nạp dữ liệu cho bảng `geolocation`
LOAD DATA LOCAL INFILE '/tmp/dataset/olist_geolocation_dataset.csv'
INTO TABLE geolocation
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;