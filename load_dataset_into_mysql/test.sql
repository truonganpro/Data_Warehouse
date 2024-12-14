DROP TABLE IF EXISTS cube_sale;
CREATE TABLE cube_sale (
    order_id VARCHAR(64) NOT NULL, -- ID đơn hàng
    customer_id VARCHAR(64) NOT NULL, -- ID khách hàng
    order_status VARCHAR(20), -- Trạng thái đơn hàng
    order_purchase_timestamp DATETIME, -- Thời gian mua
    order_approved_at DATETIME, -- Thời gian xác nhận
    order_delivered_carrier_date DATETIME, -- Ngày giao hàng cho nhà vận chuyển
    order_delivered_customer_date DATETIME, -- Ngày giao hàng cho khách hàng
    order_estimated_delivery_date DATETIME, -- Ngày giao hàng dự kiến
    payment_sequential INT, -- Số thứ tự thanh toán
    payment_type VARCHAR(20), -- Loại thanh toán (VD: credit_card, boleto)
    payment_installments INT, -- Số kỳ thanh toán
    payment_value DECIMAL(10, 2), -- Giá trị thanh toán
    review_id VARCHAR(64), -- ID đánh giá
    review_score INT, -- Điểm đánh giá
    review_comment_title TEXT, -- Tiêu đề nhận xét
    review_comment_message TEXT, -- Nội dung nhận xét
    review_creation_date DATE, -- Ngày tạo đánh giá
    review_answer_timestamp DATETIME, -- Thời gian phản hồi đánh giá
    customer_unique_id VARCHAR(64), -- ID khách hàng duy nhất
    customer_zip_code_prefix INT, -- Mã vùng khách hàng
    customer_city VARCHAR(100), -- Thành phố khách hàng
    customer_state VARCHAR(2), -- Bang khách hàng (2 ký tự)
    geolocation_zip_code_prefix_customer INT, -- Mã vùng địa lý khách hàng
    geolocation_lat_customer DECIMAL(10, 7), -- Vĩ độ khách hàng
    geolocation_lng_customer DECIMAL(10, 7), -- Kinh độ khách hàng
    geolocation_city_customer VARCHAR(100), -- Thành phố địa lý khách hàng
    geolocation_state_customer VARCHAR(2), -- Bang địa lý khách hàng (2 ký tự)
    LAT_LONG VARCHAR(50), -- Chuỗi tọa độ (vĩ độ - kinh độ)
    order_item_id INT, -- Mã sản phẩm trong đơn hàng
    product_id VARCHAR(64), -- Mã sản phẩm
    seller_id VARCHAR(64), -- Mã nhà cung cấp
    shipping_limit_date DATETIME, -- Hạn giao hàng
    price DECIMAL(10, 2), -- Giá sản phẩm
    freight_value DECIMAL(10, 2), -- Phí vận chuyển
    product_category_name VARCHAR(64), -- Loại sản phẩm
    product_name_length INT, -- Độ dài tên sản phẩm
    product_description_length INT, -- Độ dài mô tả sản phẩm
    product_photos_qty INT, -- Số lượng ảnh sản phẩm
    product_weight_g DECIMAL(10, 2), -- Cân nặng sản phẩm (gram)
    product_length_cm DECIMAL(10, 2), -- Chiều dài sản phẩm (cm)
    product_height_cm DECIMAL(10, 2), -- Chiều cao sản phẩm (cm)
    product_width_cm DECIMAL(10, 2), -- Chiều rộng sản phẩm (cm)
    seller_zip_code_prefix INT, -- Mã vùng nhà cung cấp
    seller_city VARCHAR(100), -- Thành phố nhà cung cấp
    seller_state VARCHAR(2), -- Bang nhà cung cấp (2 ký tự)
    geolocation_zip_code_prefix_seller INT, -- Mã vùng địa lý nhà cung cấp
    geolocation_lat_seller DECIMAL(10, 7), -- Vĩ độ nhà cung cấp
    geolocation_lng_seller DECIMAL(10, 7), -- Kinh độ nhà cung cấp
    geolocation_city_seller VARCHAR(100), -- Thành phố địa lý nhà cung cấp
    geolocation_state_seller VARCHAR(2), -- Bang địa lý nhà cung cấp (2 ký tự)
    LAT_LONG_seller VARCHAR(50), -- Chuỗi tọa độ nhà cung cấp (vĩ độ - kinh độ)
    PRIMARY KEY (order_id) -- Khóa chính
);