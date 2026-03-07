-- ClickHouse Ecommerce Sample Schema
-- Creates a database with denormalized tables for an ecommerce company

CREATE DATABASE IF NOT EXISTS ecommerce;

-- ============================================================================
-- CUSTOMERS TABLE
-- Denormalized customer data with profile and engagement metrics
-- ============================================================================
CREATE TABLE IF NOT EXISTS ecommerce.customers
(
    customer_id UUID,
    email String,
    first_name String,
    last_name String,
    phone_number Nullable(String),
    date_of_birth Nullable(Date),
    gender LowCardinality(String),
    registration_date DateTime,
    last_login_date DateTime,
    account_status LowCardinality(String),
    email_verified UInt8,
    phone_verified UInt8,

    -- Address info (denormalized)
    shipping_address_line1 String,
    shipping_address_line2 Nullable(String),
    shipping_city LowCardinality(String),
    shipping_state LowCardinality(String),
    shipping_postal_code String,
    shipping_country LowCardinality(String),

    -- Marketing preferences
    marketing_opt_in UInt8,
    preferred_channel LowCardinality(String),
    customer_segment LowCardinality(String),

    -- Engagement metrics
    total_orders UInt32,
    total_spent Decimal(12, 2),
    average_order_value Decimal(10, 2),
    loyalty_points UInt32,
    loyalty_tier LowCardinality(String),

    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),

    -- Skip indexes
    INDEX idx_email email TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_customer_segment customer_segment TYPE set(20) GRANULARITY 4,
    INDEX idx_loyalty_tier loyalty_tier TYPE set(10) GRANULARITY 4,
    INDEX idx_shipping_country shipping_country TYPE set(50) GRANULARITY 4,
    INDEX idx_account_status account_status TYPE set(10) GRANULARITY 4,
    INDEX idx_total_spent total_spent TYPE minmax GRANULARITY 4,
    INDEX idx_name_search (first_name, last_name) TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 4
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(created_at)
ORDER BY (customer_id, created_at)
TTL created_at + INTERVAL 1 DAY;

-- Projection: customers by segment and tier
ALTER TABLE ecommerce.customers ADD PROJECTION IF NOT EXISTS proj_by_segment
(
    SELECT *
    ORDER BY (customer_segment, loyalty_tier, customer_id)
);

-- Projection: customers by country
ALTER TABLE ecommerce.customers ADD PROJECTION IF NOT EXISTS proj_by_country
(
    SELECT *
    ORDER BY (shipping_country, shipping_state, shipping_city, customer_id)
);

-- ============================================================================
-- ORDERS TABLE
-- Denormalized order data with customer and product details
-- ============================================================================
CREATE TABLE IF NOT EXISTS ecommerce.orders
(
    order_id UUID,
    order_number String,
    customer_id UUID,

    -- Customer snapshot (denormalized)
    customer_email String,
    customer_first_name String,
    customer_last_name String,
    customer_segment LowCardinality(String),

    -- Order details
    order_status LowCardinality(String),
    order_date DateTime,
    shipped_date Nullable(DateTime),
    delivered_date Nullable(DateTime),

    -- Financial
    subtotal Decimal(10, 2),
    tax_amount Decimal(10, 2),
    shipping_cost Decimal(10, 2),
    discount_amount Decimal(10, 2),
    total_amount Decimal(10, 2),
    currency LowCardinality(String),

    -- Payment
    payment_method LowCardinality(String),
    payment_status LowCardinality(String),
    transaction_id Nullable(String),

    -- Shipping
    shipping_method LowCardinality(String),
    shipping_carrier LowCardinality(String),
    tracking_number Nullable(String),
    shipping_address_city LowCardinality(String),
    shipping_address_state LowCardinality(String),
    shipping_address_country LowCardinality(String),

    -- Items (denormalized as arrays)
    item_product_ids Array(String),
    item_product_names Array(String),
    item_quantities Array(UInt16),
    item_unit_prices Array(Decimal(10, 2)),
    item_categories Array(LowCardinality(String)),

    -- Attribution
    source_channel LowCardinality(String),
    campaign_id Nullable(String),
    coupon_code Nullable(String),

    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),

    -- Skip indexes
    INDEX idx_order_number order_number TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_customer_email customer_email TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_order_status order_status TYPE set(10) GRANULARITY 4,
    INDEX idx_payment_method payment_method TYPE set(20) GRANULARITY 4,
    INDEX idx_payment_status payment_status TYPE set(10) GRANULARITY 4,
    INDEX idx_shipping_country shipping_address_country TYPE set(50) GRANULARITY 4,
    INDEX idx_source_channel source_channel TYPE set(20) GRANULARITY 4,
    INDEX idx_total_amount total_amount TYPE minmax GRANULARITY 4,
    INDEX idx_product_ids item_product_ids TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_product_names item_product_names TYPE tokenbf_v1(512, 3, 0) GRANULARITY 4,
    INDEX idx_categories item_categories TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_coupon coupon_code TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_tracking tracking_number TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(order_date)
ORDER BY (order_date, customer_id, order_id)
TTL created_at + INTERVAL 1 DAY;

-- Projection: orders by customer
ALTER TABLE ecommerce.orders ADD PROJECTION IF NOT EXISTS proj_by_customer
(
    SELECT *
    ORDER BY (customer_id, order_date, order_id)
);

-- Projection: orders by status
ALTER TABLE ecommerce.orders ADD PROJECTION IF NOT EXISTS proj_by_status
(
    SELECT *
    ORDER BY (order_status, order_date, order_id)
);

-- Projection: orders by payment method
ALTER TABLE ecommerce.orders ADD PROJECTION IF NOT EXISTS proj_by_payment
(
    SELECT *
    ORDER BY (payment_method, payment_status, order_date, order_id)
);

-- ============================================================================
-- PAGE VIEWS TABLE
-- User browsing behavior and session tracking
-- ============================================================================
CREATE TABLE IF NOT EXISTS ecommerce.page_views
(
    view_id UUID,
    session_id UUID,
    customer_id Nullable(UUID),
    anonymous_id String,

    -- Page info
    page_url String,
    page_path String,
    page_title String,
    page_type LowCardinality(String),

    -- Product context (if product page)
    product_id Nullable(String),
    product_name Nullable(String),
    product_category LowCardinality(Nullable(String)),
    product_price Nullable(Decimal(10, 2)),

    -- Search context (if search page)
    search_query Nullable(String),
    search_results_count Nullable(UInt32),

    -- Traffic source
    referrer_url Nullable(String),
    referrer_domain Nullable(String),
    utm_source LowCardinality(Nullable(String)),
    utm_medium LowCardinality(Nullable(String)),
    utm_campaign Nullable(String),

    -- Device info
    device_type LowCardinality(String),
    browser LowCardinality(String),
    browser_version String,
    os LowCardinality(String),
    os_version String,
    screen_resolution String,

    -- Location
    ip_address String,
    geo_country LowCardinality(String),
    geo_region LowCardinality(String),
    geo_city String,

    -- Performance metrics
    page_load_time_ms UInt32,
    time_on_page_seconds UInt32,
    scroll_depth_percent UInt8,

    -- Engagement
    clicks_count UInt16,
    add_to_cart_clicked UInt8,
    buy_now_clicked UInt8,

    view_timestamp DateTime,
    created_at DateTime DEFAULT now(),

    -- Skip indexes
    INDEX idx_page_url page_url TYPE tokenbf_v1(512, 3, 0) GRANULARITY 4,
    INDEX idx_page_path page_path TYPE tokenbf_v1(512, 3, 0) GRANULARITY 4,
    INDEX idx_page_type page_type TYPE set(20) GRANULARITY 4,
    INDEX idx_device_type device_type TYPE set(10) GRANULARITY 4,
    INDEX idx_browser browser TYPE set(30) GRANULARITY 4,
    INDEX idx_os os TYPE set(20) GRANULARITY 4,
    INDEX idx_geo_country geo_country TYPE set(50) GRANULARITY 4,
    INDEX idx_product_id product_id TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_search_query search_query TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 4,
    INDEX idx_ip_address ip_address TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_page_load page_load_time_ms TYPE minmax GRANULARITY 4,
    INDEX idx_utm_source utm_source TYPE set(50) GRANULARITY 4,
    INDEX idx_referrer_domain referrer_domain TYPE set(100) GRANULARITY 4
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(view_timestamp)
ORDER BY (view_timestamp, session_id, view_id)
TTL created_at + INTERVAL 1 DAY;

-- Projection: page views by session
ALTER TABLE ecommerce.page_views ADD PROJECTION IF NOT EXISTS proj_by_session
(
    SELECT *
    ORDER BY (session_id, view_timestamp, view_id)
);

-- Projection: page views by device type and country
ALTER TABLE ecommerce.page_views ADD PROJECTION IF NOT EXISTS proj_by_device_country
(
    SELECT *
    ORDER BY (device_type, geo_country, view_timestamp, view_id)
);

-- ============================================================================
-- SHOPPING CART TABLE
-- Cart state tracking with product details
-- ============================================================================
CREATE TABLE IF NOT EXISTS ecommerce.shopping_cart
(
    cart_id UUID,
    session_id UUID,
    customer_id Nullable(UUID),
    anonymous_id String,

    -- Cart status
    cart_status LowCardinality(String),
    cart_created_at DateTime,
    cart_updated_at DateTime,
    cart_abandoned_at Nullable(DateTime),
    cart_converted_at Nullable(DateTime),
    converted_order_id Nullable(UUID),

    -- Cart items (denormalized)
    item_product_ids Array(String),
    item_product_names Array(String),
    item_product_categories Array(LowCardinality(String)),
    item_quantities Array(UInt16),
    item_unit_prices Array(Decimal(10, 2)),
    item_total_prices Array(Decimal(10, 2)),
    item_added_timestamps Array(DateTime),

    -- Cart totals
    items_count UInt16,
    unique_items_count UInt8,
    subtotal Decimal(10, 2),
    estimated_tax Decimal(10, 2),
    estimated_shipping Decimal(10, 2),
    discount_amount Decimal(10, 2),
    estimated_total Decimal(10, 2),
    currency LowCardinality(String),

    -- Applied promotions
    coupon_codes Array(String),
    promotion_ids Array(String),

    -- Attribution
    source_channel LowCardinality(String),
    landing_page_url String,
    utm_source LowCardinality(Nullable(String)),
    utm_medium LowCardinality(Nullable(String)),
    utm_campaign Nullable(String),

    -- Device info
    device_type LowCardinality(String),
    browser LowCardinality(String),

    -- Recovery attempts
    recovery_emails_sent UInt8,
    last_recovery_email_at Nullable(DateTime),

    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),

    -- Skip indexes
    INDEX idx_cart_status cart_status TYPE set(10) GRANULARITY 4,
    INDEX idx_cart_device device_type TYPE set(10) GRANULARITY 4,
    INDEX idx_cart_source source_channel TYPE set(20) GRANULARITY 4,
    INDEX idx_cart_total estimated_total TYPE minmax GRANULARITY 4,
    INDEX idx_cart_product_ids item_product_ids TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_cart_categories item_product_categories TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_cart_coupons coupon_codes TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_cart_browser browser TYPE set(30) GRANULARITY 4,
    INDEX idx_landing_page landing_page_url TYPE tokenbf_v1(512, 3, 0) GRANULARITY 4
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(cart_created_at)
ORDER BY (cart_created_at, cart_id)
TTL created_at + INTERVAL 1 DAY;

-- Projection: carts by status
ALTER TABLE ecommerce.shopping_cart ADD PROJECTION IF NOT EXISTS proj_by_cart_status
(
    SELECT *
    ORDER BY (cart_status, cart_created_at, cart_id)
);

-- Projection: carts by device
ALTER TABLE ecommerce.shopping_cart ADD PROJECTION IF NOT EXISTS proj_by_device
(
    SELECT *
    ORDER BY (device_type, source_channel, cart_created_at, cart_id)
);

-- ============================================================================
-- MATERIALIZED VIEW 1: Hourly Sales Summary
-- Aggregates order data by hour for real-time dashboards
-- ============================================================================
CREATE TABLE IF NOT EXISTS ecommerce.hourly_sales_summary
(
    hour DateTime,
    orders_count UInt64,
    total_revenue Decimal(18, 2),
    avg_order_value Decimal(10, 2),
    unique_customers UInt64,
    total_items_sold UInt64,
    source_channel LowCardinality(String),
    payment_method LowCardinality(String),
    shipping_country LowCardinality(String)
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMMDD(hour)
ORDER BY (hour, source_channel, payment_method, shipping_country)
TTL hour + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS ecommerce.mv_hourly_sales_summary
TO ecommerce.hourly_sales_summary
AS SELECT
    toStartOfHour(order_date) AS hour,
    count() AS orders_count,
    sum(total_amount) AS total_revenue,
    avg(total_amount) AS avg_order_value,
    uniqExact(customer_id) AS unique_customers,
    sum(length(item_quantities)) AS total_items_sold,
    source_channel,
    payment_method,
    shipping_address_country AS shipping_country
FROM ecommerce.orders
GROUP BY
    hour,
    source_channel,
    payment_method,
    shipping_address_country;

-- ============================================================================
-- MATERIALIZED VIEW 2: Cart Abandonment Tracking
-- Tracks abandoned carts for recovery campaigns
-- ============================================================================
CREATE TABLE IF NOT EXISTS ecommerce.cart_abandonment_stats
(
    date Date,
    hour UInt8,
    device_type LowCardinality(String),
    source_channel LowCardinality(String),
    total_carts UInt64,
    abandoned_carts UInt64,
    converted_carts UInt64,
    total_abandoned_value Decimal(18, 2),
    avg_abandoned_cart_value Decimal(10, 2),
    avg_items_in_abandoned_cart Float32
)
ENGINE = SummingMergeTree()
PARTITION BY date
ORDER BY (date, hour, device_type, source_channel)
TTL date + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS ecommerce.mv_cart_abandonment_stats
TO ecommerce.cart_abandonment_stats
AS SELECT
    toDate(cart_created_at) AS date,
    toHour(cart_created_at) AS hour,
    device_type,
    source_channel,
    count() AS total_carts,
    countIf(cart_status = 'abandoned') AS abandoned_carts,
    countIf(cart_status = 'converted') AS converted_carts,
    sumIf(estimated_total, cart_status = 'abandoned') AS total_abandoned_value,
    ifNotFinite(avgIf(estimated_total, cart_status = 'abandoned'), 0) AS avg_abandoned_cart_value,
    ifNotFinite(avgIf(items_count, cart_status = 'abandoned'), 0) AS avg_items_in_abandoned_cart
FROM ecommerce.shopping_cart
GROUP BY
    date,
    hour,
    device_type,
    source_channel;

-- ============================================================================
-- REFRESHABLE MATERIALIZED VIEW: Customer Lifetime Value
-- Periodically refreshes customer value calculations
-- ============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS ecommerce.mv_customer_lifetime_value
REFRESH EVERY 5 MINUTE
ENGINE = ReplacingMergeTree()
ORDER BY customer_id
TTL last_order_date + INTERVAL 1 DAY
AS SELECT
    c.customer_id,
    c.email,
    c.first_name,
    c.last_name,
    c.customer_segment,
    c.loyalty_tier,
    c.registration_date,
    count(o.order_id) AS total_orders,
    sum(o.total_amount) AS lifetime_value,
    avg(o.total_amount) AS avg_order_value,
    min(o.order_date) AS first_order_date,
    max(o.order_date) AS last_order_date,
    dateDiff('day', min(o.order_date), max(o.order_date)) AS customer_tenure_days,
    dateDiff('day', max(o.order_date), now()) AS days_since_last_order,
    CASE
        WHEN dateDiff('day', max(o.order_date), now()) <= 30 THEN 'active'
        WHEN dateDiff('day', max(o.order_date), now()) <= 90 THEN 'at_risk'
        ELSE 'churned'
    END AS engagement_status
FROM ecommerce.customers c
LEFT JOIN ecommerce.orders o ON c.customer_id = o.customer_id
GROUP BY
    c.customer_id,
    c.email,
    c.first_name,
    c.last_name,
    c.customer_segment,
    c.loyalty_tier,
    c.registration_date;

-- ============================================================================
-- REFRESHABLE MATERIALIZED VIEW: Product Performance
-- Refreshes product-level metrics every 10 minutes
-- ============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS ecommerce.mv_product_performance
REFRESH EVERY 10 MINUTE
ENGINE = ReplacingMergeTree()
ORDER BY product_id
TTL last_order_date + INTERVAL 1 DAY
AS SELECT
    pid AS product_id,
    pname AS product_name,
    pcat AS product_category,
    count() AS times_ordered,
    sum(qty) AS total_quantity_sold,
    sum(price * qty) AS total_revenue,
    avg(price) AS avg_unit_price,
    uniqExact(customer_id) AS unique_buyers,
    min(order_date) AS first_order_date,
    max(order_date) AS last_order_date
FROM ecommerce.orders
ARRAY JOIN
    item_product_ids AS pid,
    item_product_names AS pname,
    item_categories AS pcat,
    item_quantities AS qty,
    item_unit_prices AS price
GROUP BY pid, pname, pcat;

-- ============================================================================
-- REFRESHABLE MATERIALIZED VIEW: Channel Attribution Summary
-- Refreshes channel performance metrics every 15 minutes
-- ============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS ecommerce.mv_channel_attribution
REFRESH EVERY 15 MINUTE
ENGINE = ReplacingMergeTree()
ORDER BY (source_channel, date)
TTL date + INTERVAL 1 DAY
AS SELECT
    source_channel,
    toDate(order_date) AS date,
    count() AS orders,
    uniqExact(customer_id) AS unique_customers,
    sum(total_amount) AS revenue,
    avg(total_amount) AS avg_order_value,
    avg(length(item_product_ids)) AS avg_items_per_order,
    sumIf(total_amount, coupon_code IS NOT NULL) AS coupon_revenue,
    countIf(coupon_code IS NOT NULL) AS coupon_orders
FROM ecommerce.orders
GROUP BY source_channel, date;

-- ============================================================================
-- REFRESHABLE MATERIALIZED VIEW: Page Performance by Device
-- Refreshes page load performance metrics every 5 minutes
-- ============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS ecommerce.mv_page_performance
REFRESH EVERY 5 MINUTE
ENGINE = ReplacingMergeTree()
ORDER BY (device_type, browser, page_type, hour)
TTL hour + INTERVAL 1 DAY
AS SELECT
    device_type,
    browser,
    page_type,
    toStartOfHour(view_timestamp) AS hour,
    count() AS page_views,
    avg(page_load_time_ms) AS avg_load_time_ms,
    quantile(0.95)(page_load_time_ms) AS p95_load_time_ms,
    avg(time_on_page_seconds) AS avg_time_on_page,
    avg(scroll_depth_percent) AS avg_scroll_depth,
    countIf(add_to_cart_clicked = 1) AS add_to_cart_events,
    countIf(buy_now_clicked = 1) AS buy_now_events
FROM ecommerce.page_views
GROUP BY device_type, browser, page_type, hour;

-- ============================================================================
-- REFRESHABLE MATERIALIZED VIEW: Cart Conversion Funnel
-- Refreshes conversion funnel metrics every 10 minutes
-- ============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS ecommerce.mv_cart_conversion_funnel
REFRESH EVERY 10 MINUTE
ENGINE = ReplacingMergeTree()
ORDER BY (date, source_channel, device_type)
TTL date + INTERVAL 1 DAY
AS SELECT
    toDate(cart_created_at) AS date,
    source_channel,
    device_type,
    count() AS total_carts,
    countIf(cart_status = 'active') AS active_carts,
    countIf(cart_status = 'abandoned') AS abandoned_carts,
    countIf(cart_status = 'converted') AS converted_carts,
    countIf(cart_status = 'expired') AS expired_carts,
    round(countIf(cart_status = 'converted') / count() * 100, 2) AS conversion_rate,
    round(countIf(cart_status = 'abandoned') / count() * 100, 2) AS abandonment_rate,
    avg(items_count) AS avg_items_per_cart,
    avgIf(estimated_total, cart_status = 'converted') AS avg_converted_value,
    avgIf(estimated_total, cart_status = 'abandoned') AS avg_abandoned_value,
    avgIf(recovery_emails_sent, cart_status = 'abandoned') AS avg_recovery_emails
FROM ecommerce.shopping_cart
GROUP BY date, source_channel, device_type;
