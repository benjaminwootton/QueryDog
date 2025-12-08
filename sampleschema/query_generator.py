#!/usr/bin/env python3
"""
Query Load Generator for ClickHouse

Generates realistic query load against the ecommerce schema at approximately
15 queries per second. Uses 20 different query patterns with randomized
parameters to simulate real application usage.

Includes INSERT, UPDATE (ALTER UPDATE), and DELETE operations.
"""

import os
import random
import time
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Callable

import clickhouse_connect
from faker import Faker
from dotenv import load_dotenv

# Initialize Faker
fake = Faker(['en_US', 'en_GB'])

# Load environment variables
load_dotenv()

# ClickHouse connection settings
CLICKHOUSE_CONFIG = {
    'host': os.getenv('CLICKHOUSE_HOST', 'localhost'),
    'port': int(os.getenv('CLICKHOUSE_PORT_HTTP', '8443')),
    'username': os.getenv('CLICKHOUSE_USER', 'default'),
    'password': os.getenv('CLICKHOUSE_PASSWORD', ''),
    'database': os.getenv('CLICKHOUSE_DATABASE', 'ecommerce'),
    'secure': os.getenv('CLICKHOUSE_SECURE', '1') == '1',
}

# Target queries per second
TARGET_QPS = 15

# Random value pools for query parameters
COUNTRIES = ['US', 'UK', 'CA', 'AU', 'DE', 'FR', 'JP', 'BR']
ORDER_STATUSES = ['pending', 'confirmed', 'processing', 'shipped', 'delivered', 'cancelled', 'refunded']
PAYMENT_METHODS = ['credit_card', 'debit_card', 'paypal', 'apple_pay', 'google_pay', 'bank_transfer', 'crypto']
CART_STATUSES = ['active', 'abandoned', 'converted', 'expired']
DEVICE_TYPES = ['desktop', 'mobile', 'tablet']
PAGE_TYPES = ['home', 'category', 'product', 'search', 'cart', 'checkout', 'account', 'blog', 'about', 'contact']
SOURCE_CHANNELS = ['organic', 'paid_search', 'social', 'email', 'referral', 'direct', 'affiliate']
CUSTOMER_SEGMENTS = ['new', 'regular', 'vip', 'churned', 'at_risk', 'high_value']
LOYALTY_TIERS = ['bronze', 'silver', 'gold', 'platinum', 'diamond']
BROWSERS = ['chrome', 'safari', 'firefox', 'edge', 'opera', 'samsung_browser']
PRODUCT_CATEGORIES = ['electronics', 'clothing', 'home_garden', 'sports', 'books', 'beauty', 'toys', 'food', 'automotive']
CURRENCIES = ['USD', 'EUR', 'GBP', 'CAD', 'AUD']


def random_date_range(max_days_ago: int = 7) -> tuple:
    """Generate a random date range for queries."""
    end_date = datetime.now()
    days_ago = random.randint(1, max_days_ago)
    start_date = end_date - timedelta(days=days_ago)
    return start_date.strftime('%Y-%m-%d %H:%M:%S'), end_date.strftime('%Y-%m-%d %H:%M:%S')


def random_hour() -> int:
    """Random hour of day."""
    return random.randint(0, 23)


def random_limit() -> int:
    """Random result limit."""
    return random.choice([10, 25, 50, 100, 250, 500, 1000])


# =============================================================================
# QUERY PATTERNS - 20 different realistic queries
# =============================================================================

def query_orders_by_status() -> str:
    """Query 1: Orders count by status in date range."""
    start, end = random_date_range()
    return f"""
        SELECT
            order_status,
            count() as order_count,
            sum(total_amount) as total_revenue,
            avg(total_amount) as avg_order_value
        FROM orders
        WHERE order_date BETWEEN '{start}' AND '{end}'
        GROUP BY order_status
        ORDER BY order_count DESC
    """


def query_top_customers() -> str:
    """Query 2: Top customers by total spent."""
    start, end = random_date_range()
    limit = random_limit()
    return f"""
        SELECT
            customer_id,
            customer_email,
            customer_first_name,
            customer_last_name,
            count() as order_count,
            sum(total_amount) as total_spent,
            avg(total_amount) as avg_order
        FROM orders
        WHERE order_date BETWEEN '{start}' AND '{end}'
        GROUP BY customer_id, customer_email, customer_first_name, customer_last_name
        ORDER BY total_spent DESC
        LIMIT {limit}
    """


def query_hourly_sales() -> str:
    """Query 3: Hourly sales aggregation."""
    start, end = random_date_range(3)
    return f"""
        SELECT
            toStartOfHour(order_date) as hour,
            count() as orders,
            sum(total_amount) as revenue,
            uniq(customer_id) as unique_customers
        FROM orders
        WHERE order_date BETWEEN '{start}' AND '{end}'
        GROUP BY hour
        ORDER BY hour
    """


def query_cart_abandonment_rate() -> str:
    """Query 4: Cart abandonment rate by device."""
    start, end = random_date_range()
    return f"""
        SELECT
            device_type,
            count() as total_carts,
            countIf(cart_status = 'abandoned') as abandoned,
            countIf(cart_status = 'converted') as converted,
            round(countIf(cart_status = 'abandoned') / count() * 100, 2) as abandonment_rate
        FROM shopping_cart
        WHERE cart_created_at BETWEEN '{start}' AND '{end}'
        GROUP BY device_type
        ORDER BY total_carts DESC
    """


def query_page_views_by_type() -> str:
    """Query 5: Page views distribution by type."""
    start, end = random_date_range(1)
    return f"""
        SELECT
            page_type,
            count() as view_count,
            avg(time_on_page_seconds) as avg_time_on_page,
            avg(scroll_depth_percent) as avg_scroll_depth
        FROM page_views
        WHERE view_timestamp BETWEEN '{start}' AND '{end}'
        GROUP BY page_type
        ORDER BY view_count DESC
    """


def query_revenue_by_channel() -> str:
    """Query 6: Revenue by marketing channel."""
    start, end = random_date_range()
    return f"""
        SELECT
            source_channel,
            count() as orders,
            sum(total_amount) as revenue,
            avg(total_amount) as avg_order_value,
            sum(discount_amount) as total_discounts
        FROM orders
        WHERE order_date BETWEEN '{start}' AND '{end}'
        GROUP BY source_channel
        ORDER BY revenue DESC
    """


def query_customer_segments() -> str:
    """Query 7: Customer segment analysis."""
    segment = random.choice(CUSTOMER_SEGMENTS)
    limit = random_limit()
    return f"""
        SELECT
            customer_id,
            email,
            first_name,
            last_name,
            total_orders,
            total_spent,
            loyalty_tier
        FROM customers
        WHERE customer_segment = '{segment}'
        ORDER BY total_spent DESC
        LIMIT {limit}
    """


def query_orders_by_country() -> str:
    """Query 8: Orders by shipping country."""
    start, end = random_date_range()
    return f"""
        SELECT
            shipping_address_country,
            count() as order_count,
            sum(total_amount) as total_revenue,
            avg(shipping_cost) as avg_shipping_cost,
            countIf(order_status = 'delivered') as delivered_orders
        FROM orders
        WHERE order_date BETWEEN '{start}' AND '{end}'
        GROUP BY shipping_address_country
        ORDER BY total_revenue DESC
    """


def query_product_category_performance() -> str:
    """Query 9: Product category performance from order items."""
    start, end = random_date_range()
    return f"""
        SELECT
            arrayJoin(item_categories) as category,
            count() as order_count,
            sum(total_amount) as revenue
        FROM orders
        WHERE order_date BETWEEN '{start}' AND '{end}'
        GROUP BY category
        ORDER BY revenue DESC
    """


def query_payment_method_analysis() -> str:
    """Query 10: Payment method breakdown."""
    start, end = random_date_range()
    country = random.choice(COUNTRIES)
    return f"""
        SELECT
            payment_method,
            payment_status,
            count() as transaction_count,
            sum(total_amount) as total_amount,
            avg(total_amount) as avg_transaction
        FROM orders
        WHERE order_date BETWEEN '{start}' AND '{end}'
          AND shipping_address_country = '{country}'
        GROUP BY payment_method, payment_status
        ORDER BY transaction_count DESC
    """


def query_session_analysis() -> str:
    """Query 11: Session analysis with page views."""
    start, end = random_date_range(1)
    device = random.choice(DEVICE_TYPES)
    return f"""
        SELECT
            session_id,
            count() as page_views,
            min(view_timestamp) as session_start,
            max(view_timestamp) as session_end,
            sum(time_on_page_seconds) as total_time,
            max(add_to_cart_clicked) as added_to_cart
        FROM page_views
        WHERE view_timestamp BETWEEN '{start}' AND '{end}'
          AND device_type = '{device}'
        GROUP BY session_id
        ORDER BY page_views DESC
        LIMIT 100
    """


def query_browser_performance() -> str:
    """Query 12: Page load performance by browser."""
    start, end = random_date_range(1)
    return f"""
        SELECT
            browser,
            count() as views,
            avg(page_load_time_ms) as avg_load_time,
            quantile(0.95)(page_load_time_ms) as p95_load_time,
            max(page_load_time_ms) as max_load_time
        FROM page_views
        WHERE view_timestamp BETWEEN '{start}' AND '{end}'
        GROUP BY browser
        ORDER BY views DESC
    """


def query_abandoned_cart_value() -> str:
    """Query 13: Abandoned cart value analysis."""
    start, end = random_date_range()
    return f"""
        SELECT
            source_channel,
            count() as abandoned_carts,
            sum(estimated_total) as total_abandoned_value,
            avg(estimated_total) as avg_cart_value,
            avg(items_count) as avg_items
        FROM shopping_cart
        WHERE cart_status = 'abandoned'
          AND cart_created_at BETWEEN '{start}' AND '{end}'
        GROUP BY source_channel
        ORDER BY total_abandoned_value DESC
    """


def query_customer_loyalty_distribution() -> str:
    """Query 14: Customer loyalty tier distribution."""
    return """
        SELECT
            loyalty_tier,
            count() as customer_count,
            avg(total_spent) as avg_spent,
            avg(total_orders) as avg_orders,
            sum(loyalty_points) as total_points
        FROM customers
        GROUP BY loyalty_tier
        ORDER BY customer_count DESC
    """


def query_recent_orders() -> str:
    """Query 15: Recent orders with details."""
    status = random.choice(ORDER_STATUSES)
    limit = random_limit()
    return f"""
        SELECT
            order_id,
            order_number,
            customer_email,
            order_status,
            total_amount,
            payment_method,
            order_date
        FROM orders
        WHERE order_status = '{status}'
        ORDER BY order_date DESC
        LIMIT {limit}
    """


def query_conversion_funnel() -> str:
    """Query 16: Simple conversion analysis."""
    start, end = random_date_range(1)
    return f"""
        SELECT
            page_type,
            count() as views,
            sum(add_to_cart_clicked) as add_to_cart,
            sum(buy_now_clicked) as buy_now
        FROM page_views
        WHERE view_timestamp BETWEEN '{start}' AND '{end}'
          AND page_type IN ('product', 'cart', 'checkout')
        GROUP BY page_type
        ORDER BY views DESC
    """


def query_geographic_distribution() -> str:
    """Query 17: Geographic page view distribution."""
    start, end = random_date_range(1)
    return f"""
        SELECT
            geo_country,
            count() as views,
            uniq(session_id) as unique_sessions,
            avg(time_on_page_seconds) as avg_time
        FROM page_views
        WHERE view_timestamp BETWEEN '{start}' AND '{end}'
        GROUP BY geo_country
        ORDER BY views DESC
        LIMIT 20
    """


def query_search_analysis() -> str:
    """Query 18: Search query analysis."""
    start, end = random_date_range(1)
    return f"""
        SELECT
            search_query,
            count() as search_count,
            avg(search_results_count) as avg_results,
            avg(time_on_page_seconds) as avg_time_on_results
        FROM page_views
        WHERE page_type = 'search'
          AND search_query IS NOT NULL
          AND view_timestamp BETWEEN '{start}' AND '{end}'
        GROUP BY search_query
        ORDER BY search_count DESC
        LIMIT 50
    """


def query_shipping_analysis() -> str:
    """Query 19: Shipping method and carrier analysis."""
    start, end = random_date_range()
    return f"""
        SELECT
            shipping_method,
            shipping_carrier,
            count() as order_count,
            avg(shipping_cost) as avg_cost,
            countIf(order_status = 'delivered') as delivered,
            countIf(order_status = 'shipped') as in_transit
        FROM orders
        WHERE order_date BETWEEN '{start}' AND '{end}'
        GROUP BY shipping_method, shipping_carrier
        ORDER BY order_count DESC
    """


def query_daily_metrics() -> str:
    """Query 20: Daily metrics summary."""
    start, end = random_date_range(7)
    return f"""
        SELECT
            toDate(order_date) as date,
            count() as orders,
            uniq(customer_id) as customers,
            sum(total_amount) as revenue,
            avg(total_amount) as aov,
            sum(discount_amount) as discounts
        FROM orders
        WHERE order_date BETWEEN '{start}' AND '{end}'
        GROUP BY date
        ORDER BY date DESC
    """


# List of all SELECT query functions
SELECT_QUERY_FUNCTIONS: list[Callable[[], str]] = [
    query_orders_by_status,
    query_top_customers,
    query_hourly_sales,
    query_cart_abandonment_rate,
    query_page_views_by_type,
    query_revenue_by_channel,
    query_customer_segments,
    query_orders_by_country,
    query_product_category_performance,
    query_payment_method_analysis,
    query_session_analysis,
    query_browser_performance,
    query_abandoned_cart_value,
    query_customer_loyalty_distribution,
    query_recent_orders,
    query_conversion_funnel,
    query_geographic_distribution,
    query_search_analysis,
    query_shipping_analysis,
    query_daily_metrics,
]


# =============================================================================
# INSERT OPERATIONS - Generate new records
# =============================================================================

def generate_insert_customer() -> tuple[str, list]:
    """Generate INSERT for a new customer."""
    customer_id = str(uuid.uuid4())
    country = random.choice(['US', 'UK', 'CA', 'AU'])
    segment = random.choice(CUSTOMER_SEGMENTS)
    loyalty_tier = random.choice(LOYALTY_TIERS)
    total_orders = random.randint(0, 50) if segment in ['vip', 'high_value', 'regular'] else random.randint(0, 5)
    total_spent = round(random.uniform(0, 10000) if total_orders > 0 else 0, 2)

    query = """
        INSERT INTO customers (
            customer_id, email, first_name, last_name, phone_number, date_of_birth,
            gender, registration_date, last_login_date, account_status, email_verified,
            phone_verified, shipping_address_line1, shipping_address_line2, shipping_city,
            shipping_state, shipping_postal_code, shipping_country, marketing_opt_in,
            preferred_channel, customer_segment, total_orders, total_spent,
            average_order_value, loyalty_points, loyalty_tier, created_at, updated_at
        ) VALUES
    """
    values = [(
        customer_id,
        fake.email(),
        fake.first_name(),
        fake.last_name(),
        fake.phone_number() if random.random() > 0.2 else None,
        fake.date_of_birth(minimum_age=18, maximum_age=54),
        random.choice(['male', 'female', 'non_binary', 'prefer_not_to_say']),
        fake.date_time_between(start_date='-2y', end_date='now'),
        datetime.now(),
        random.choice(['active', 'inactive', 'suspended']),
        random.choice([0, 1]),
        random.choice([0, 1]),
        fake.street_address(),
        fake.secondary_address() if random.random() > 0.7 else None,
        fake.city(),
        fake.state_abbr() if country == 'US' else fake.state(),
        fake.zipcode() if country == 'US' else fake.postcode(),
        country,
        random.choice([0, 1]),
        random.choice(SOURCE_CHANNELS),
        segment,
        total_orders,
        Decimal(str(total_spent)),
        Decimal(str(round(total_spent / max(total_orders, 1), 2))),
        random.randint(0, 10000),
        loyalty_tier,
        datetime.now(),
        datetime.now(),
    )]
    return query, values, 'customers'


def generate_insert_order() -> tuple[str, list]:
    """Generate INSERT for a new order."""
    order_id = str(uuid.uuid4())
    customer_id = str(uuid.uuid4())
    order_date = fake.date_time_between(start_date='-7d', end_date='now')
    order_status = random.choice(ORDER_STATUSES)

    num_items = random.randint(1, 5)
    item_ids = [f'PROD-{random.randint(1, 100):04d}' for _ in range(num_items)]
    item_names = [fake.catch_phrase() for _ in range(num_items)]
    quantities = [random.randint(1, 3) for _ in range(num_items)]
    unit_prices = [Decimal(str(round(random.uniform(9.99, 499.99), 2))) for _ in range(num_items)]
    categories = [random.choice(PRODUCT_CATEGORIES) for _ in range(num_items)]

    subtotal = sum(float(p) * q for p, q in zip(unit_prices, quantities))
    tax_amount = round(subtotal * random.uniform(0.05, 0.12), 2)
    shipping_cost = round(random.uniform(0, 15.99), 2) if subtotal < 50 else 0
    discount_amount = round(subtotal * random.uniform(0, 0.2), 2) if random.random() > 0.7 else 0
    total_amount = round(subtotal + tax_amount + shipping_cost - discount_amount, 2)

    query = """
        INSERT INTO orders (
            order_id, order_number, customer_id, customer_email, customer_first_name,
            customer_last_name, customer_segment, order_status, order_date, shipped_date,
            delivered_date, subtotal, tax_amount, shipping_cost, discount_amount,
            total_amount, currency, payment_method, payment_status, transaction_id,
            shipping_method, shipping_carrier, tracking_number, shipping_address_city,
            shipping_address_state, shipping_address_country, item_product_ids,
            item_product_names, item_quantities, item_unit_prices, item_categories,
            source_channel, campaign_id, coupon_code, created_at, updated_at
        ) VALUES
    """
    values = [(
        order_id,
        f'ORD-{random.randint(10000000, 99999999)}',
        customer_id,
        fake.email(),
        fake.first_name(),
        fake.last_name(),
        random.choice(CUSTOMER_SEGMENTS),
        order_status,
        order_date,
        order_date + timedelta(days=random.randint(1, 3)) if order_status in ['shipped', 'delivered'] else None,
        order_date + timedelta(days=random.randint(4, 8)) if order_status == 'delivered' else None,
        Decimal(str(round(subtotal, 2))),
        Decimal(str(tax_amount)),
        Decimal(str(shipping_cost)),
        Decimal(str(discount_amount)),
        Decimal(str(total_amount)),
        random.choice(CURRENCIES),
        random.choice(PAYMENT_METHODS),
        'completed' if order_status not in ['pending', 'cancelled'] else 'pending',
        str(uuid.uuid4()) if order_status not in ['pending', 'cancelled'] else None,
        random.choice(['standard', 'express', 'overnight', 'economy']),
        random.choice(['fedex', 'ups', 'usps', 'dhl']),
        str(uuid.uuid4())[:12].upper() if order_status in ['shipped', 'delivered'] else None,
        fake.city(),
        fake.state_abbr(),
        random.choice(COUNTRIES),
        item_ids,
        item_names,
        quantities,
        unit_prices,
        categories,
        random.choice(SOURCE_CHANNELS),
        f'CAMP-{random.randint(1000, 9999)}' if random.random() > 0.5 else None,
        fake.lexify(text='????').upper() + str(random.randint(10, 99)) if discount_amount > 0 else None,
        datetime.now(),
        datetime.now(),
    )]
    return query, values, 'orders'


def generate_insert_page_view() -> tuple[str, list]:
    """Generate INSERT for a new page view."""
    view_id = str(uuid.uuid4())
    session_id = str(uuid.uuid4())
    page_type = random.choice(PAGE_TYPES)
    device_type = random.choice(DEVICE_TYPES)

    query = """
        INSERT INTO page_views (
            view_id, session_id, customer_id, anonymous_id, page_url, page_path,
            page_title, page_type, product_id, product_name, product_category,
            product_price, search_query, search_results_count, referrer_url,
            referrer_domain, utm_source, utm_medium, utm_campaign, device_type,
            browser, browser_version, os, os_version, screen_resolution, ip_address,
            geo_country, geo_region, geo_city, page_load_time_ms, time_on_page_seconds,
            scroll_depth_percent, clicks_count, add_to_cart_clicked, buy_now_clicked,
            view_timestamp, created_at
        ) VALUES
    """
    has_utm = random.random() > 0.6
    product = None
    if page_type == 'product':
        product = {'id': f'PROD-{random.randint(1, 100):04d}', 'name': fake.catch_phrase(),
                   'category': random.choice(PRODUCT_CATEGORIES), 'price': round(random.uniform(9.99, 499.99), 2)}

    values = [(
        view_id,
        session_id,
        str(uuid.uuid4()) if random.random() > 0.4 else None,
        str(uuid.uuid4()),
        f'https://shop.example.com/{page_type}/{fake.slug()}',
        f'/{page_type}/{fake.slug()}',
        f'{fake.catch_phrase()} | Example Shop',
        page_type,
        product['id'] if product else None,
        product['name'] if product else None,
        product['category'] if product else None,
        Decimal(str(product['price'])) if product else None,
        fake.word() + ' ' + fake.word() if page_type == 'search' else None,
        random.randint(0, 500) if page_type == 'search' else None,
        fake.url() if random.random() > 0.4 else None,
        fake.domain_name() if random.random() > 0.4 else None,
        random.choice(['google', 'facebook', 'instagram', 'twitter', 'email']) if has_utm else None,
        random.choice(['cpc', 'organic', 'social', 'email', 'referral']) if has_utm else None,
        f'campaign_{random.randint(1, 100)}' if has_utm else None,
        device_type,
        random.choice(BROWSERS),
        f'{random.randint(80, 120)}.0.{random.randint(0, 9999)}',
        random.choice(['windows', 'macos', 'ios', 'android', 'linux']),
        f'{random.randint(10, 15)}.{random.randint(0, 9)}',
        random.choice(['1920x1080', '1366x768', '1536x864', '2560x1440', '390x844']),
        fake.ipv4(),
        random.choice(COUNTRIES),
        fake.state_abbr(),
        fake.city(),
        random.randint(200, 5000),
        random.randint(5, 300),
        random.randint(10, 100),
        random.randint(0, 20),
        1 if page_type == 'product' and random.random() > 0.85 else 0,
        1 if page_type == 'product' and random.random() > 0.95 else 0,
        fake.date_time_between(start_date='-1d', end_date='now'),
        datetime.now(),
    )]
    return query, values, 'page_views'


def generate_insert_shopping_cart() -> tuple[str, list]:
    """Generate INSERT for a new shopping cart."""
    cart_id = str(uuid.uuid4())
    session_id = str(uuid.uuid4())
    cart_status = random.choice(CART_STATUSES)
    cart_created = fake.date_time_between(start_date='-1d', end_date='now')

    num_items = random.randint(1, 6)
    item_ids = [f'PROD-{random.randint(1, 100):04d}' for _ in range(num_items)]
    item_names = [fake.catch_phrase() for _ in range(num_items)]
    categories = [random.choice(PRODUCT_CATEGORIES) for _ in range(num_items)]
    quantities = [random.randint(1, 3) for _ in range(num_items)]
    unit_prices = [Decimal(str(round(random.uniform(9.99, 499.99), 2))) for _ in range(num_items)]
    total_prices = [Decimal(str(round(float(p) * q, 2))) for p, q in zip(unit_prices, quantities)]
    item_timestamps = [cart_created + timedelta(minutes=random.randint(0, 30)) for _ in range(num_items)]

    subtotal = sum(float(p) for p in total_prices)
    estimated_tax = round(subtotal * random.uniform(0.05, 0.12), 2)
    estimated_shipping = round(random.uniform(0, 12.99), 2) if subtotal < 50 else 0
    discount_amount = round(subtotal * random.uniform(0, 0.15), 2) if random.random() > 0.8 else 0
    estimated_total = round(subtotal + estimated_tax + estimated_shipping - discount_amount, 2)

    cart_updated = cart_created + timedelta(minutes=random.randint(1, 60))
    has_utm = random.random() > 0.5

    query = """
        INSERT INTO shopping_cart (
            cart_id, session_id, customer_id, anonymous_id, cart_status, cart_created_at,
            cart_updated_at, cart_abandoned_at, cart_converted_at, converted_order_id,
            item_product_ids, item_product_names, item_product_categories, item_quantities,
            item_unit_prices, item_total_prices, item_added_timestamps, items_count,
            unique_items_count, subtotal, estimated_tax, estimated_shipping, discount_amount,
            estimated_total, currency, coupon_codes, promotion_ids, source_channel,
            landing_page_url, utm_source, utm_medium, utm_campaign, device_type, browser,
            recovery_emails_sent, last_recovery_email_at, created_at, updated_at
        ) VALUES
    """
    values = [(
        cart_id,
        session_id,
        str(uuid.uuid4()) if random.random() > 0.5 else None,
        str(uuid.uuid4()),
        cart_status,
        cart_created,
        cart_updated,
        cart_updated + timedelta(hours=random.randint(1, 24)) if cart_status == 'abandoned' else None,
        cart_updated + timedelta(minutes=random.randint(5, 30)) if cart_status == 'converted' else None,
        str(uuid.uuid4()) if cart_status == 'converted' else None,
        item_ids,
        item_names,
        categories,
        quantities,
        unit_prices,
        total_prices,
        item_timestamps,
        sum(quantities),
        num_items,
        Decimal(str(round(subtotal, 2))),
        Decimal(str(estimated_tax)),
        Decimal(str(estimated_shipping)),
        Decimal(str(discount_amount)),
        Decimal(str(estimated_total)),
        random.choice(CURRENCIES),
        [fake.lexify(text='????').upper() + str(random.randint(10, 99))] if discount_amount > 0 else [],
        [f'PROMO-{random.randint(100, 999)}'] if random.random() > 0.8 else [],
        random.choice(SOURCE_CHANNELS),
        f'https://shop.example.com/{fake.slug()}',
        random.choice(['google', 'facebook', 'instagram', 'email']) if has_utm else None,
        random.choice(['cpc', 'social', 'email']) if has_utm else None,
        f'campaign_{random.randint(1, 50)}' if has_utm else None,
        random.choice(DEVICE_TYPES),
        random.choice(BROWSERS),
        random.randint(0, 3) if cart_status == 'abandoned' else 0,
        cart_updated + timedelta(hours=random.randint(1, 12)) if cart_status == 'abandoned' and random.random() > 0.5 else None,
        datetime.now(),
        datetime.now(),
    )]
    return query, values, 'shopping_cart'


INSERT_FUNCTIONS = [
    generate_insert_customer,
    generate_insert_order,
    generate_insert_page_view,
    generate_insert_shopping_cart,
]


# =============================================================================
# UPDATE OPERATIONS - Modify existing records using ALTER TABLE UPDATE
# =============================================================================

def generate_update_customer_status() -> str:
    """Update customer account status."""
    old_status = random.choice(['active', 'inactive', 'suspended', 'pending_verification'])
    new_status = random.choice(['active', 'inactive', 'suspended'])
    return f"""
        ALTER TABLE customers
        UPDATE account_status = '{new_status}', updated_at = now()
        WHERE account_status = '{old_status}'
        AND rand() % 100 < 5
    """


def generate_update_customer_loyalty() -> str:
    """Update customer loyalty points and tier."""
    segment = random.choice(CUSTOMER_SEGMENTS)
    points_add = random.randint(100, 1000)
    new_tier = random.choice(LOYALTY_TIERS)
    return f"""
        ALTER TABLE customers
        UPDATE loyalty_points = loyalty_points + {points_add},
               loyalty_tier = '{new_tier}',
               updated_at = now()
        WHERE customer_segment = '{segment}'
        AND rand() % 100 < 10
    """


def generate_update_order_status() -> str:
    """Update order status progression."""
    transitions = [
        ('pending', 'confirmed'),
        ('confirmed', 'processing'),
        ('processing', 'shipped'),
        ('shipped', 'delivered'),
    ]
    old_status, new_status = random.choice(transitions)
    return f"""
        ALTER TABLE orders
        UPDATE order_status = '{new_status}',
               updated_at = now()
        WHERE order_status = '{old_status}'
        AND rand() % 100 < 15
    """


def generate_update_cart_status() -> str:
    """Update shopping cart status."""
    return f"""
        ALTER TABLE shopping_cart
        UPDATE cart_status = 'abandoned',
               cart_abandoned_at = now(),
               updated_at = now()
        WHERE cart_status = 'active'
        AND cart_updated_at < now() - INTERVAL 2 HOUR
        AND rand() % 100 < 20
    """


UPDATE_FUNCTIONS = [
    generate_update_customer_status,
    generate_update_customer_loyalty,
    generate_update_order_status,
    generate_update_cart_status,
]


# =============================================================================
# DELETE OPERATIONS - Remove records using ALTER TABLE DELETE
# =============================================================================

def generate_delete_old_page_views() -> str:
    """Delete old page views."""
    hours_ago = random.randint(20, 24)
    return f"""
        ALTER TABLE page_views
        DELETE WHERE view_timestamp < now() - INTERVAL {hours_ago} HOUR
        AND rand() % 100 < 5
    """


def generate_delete_expired_carts() -> str:
    """Delete expired shopping carts."""
    return """
        ALTER TABLE shopping_cart
        DELETE WHERE cart_status = 'expired'
        AND cart_updated_at < now() - INTERVAL 24 HOUR
        AND rand() % 100 < 10
    """


def generate_delete_cancelled_orders() -> str:
    """Delete old cancelled orders."""
    days_ago = random.randint(5, 7)
    return f"""
        ALTER TABLE orders
        DELETE WHERE order_status = 'cancelled'
        AND order_date < now() - INTERVAL {days_ago} DAY
        AND rand() % 100 < 5
    """


def generate_delete_inactive_customers() -> str:
    """Delete old inactive customers."""
    return """
        ALTER TABLE customers
        DELETE WHERE account_status = 'suspended'
        AND last_login_date < now() - INTERVAL 7 DAY
        AND rand() % 100 < 3
    """


DELETE_FUNCTIONS = [
    generate_delete_old_page_views,
    generate_delete_expired_carts,
    generate_delete_cancelled_orders,
    generate_delete_inactive_customers,
]


def get_client():
    """Create ClickHouse client connection."""
    return clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)


def execute_random_select(client) -> tuple:
    """Execute a random SELECT query and return stats."""
    query_func = random.choice(SELECT_QUERY_FUNCTIONS)
    query = query_func()

    start_time = time.time()
    try:
        result = client.query(query)
        duration_ms = (time.time() - start_time) * 1000
        row_count = result.row_count
        return True, query_func.__name__, duration_ms, row_count, 'SELECT'
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        return False, query_func.__name__, duration_ms, str(e), 'SELECT'


def execute_random_insert(client) -> tuple:
    """Execute a random INSERT and return stats."""
    insert_func = random.choice(INSERT_FUNCTIONS)
    query, values, table_name = insert_func()

    start_time = time.time()
    try:
        # Use the client's insert method for proper data insertion
        columns = None
        # Extract column names from query
        if 'INSERT INTO' in query:
            col_start = query.index('(') + 1
            col_end = query.index(')')
            columns = [c.strip() for c in query[col_start:col_end].split(',')]

        client.insert(table_name, values, column_names=columns)
        duration_ms = (time.time() - start_time) * 1000
        return True, f'insert_{table_name}', duration_ms, len(values), 'INSERT'
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        return False, f'insert_{table_name}', duration_ms, str(e), 'INSERT'


def execute_random_update(client) -> tuple:
    """Execute a random UPDATE (ALTER TABLE UPDATE) and return stats."""
    update_func = random.choice(UPDATE_FUNCTIONS)
    query = update_func()

    start_time = time.time()
    try:
        client.command(query)
        duration_ms = (time.time() - start_time) * 1000
        return True, update_func.__name__, duration_ms, 'mutation', 'UPDATE'
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        return False, update_func.__name__, duration_ms, str(e), 'UPDATE'


def execute_random_delete(client) -> tuple:
    """Execute a random DELETE (ALTER TABLE DELETE) and return stats."""
    delete_func = random.choice(DELETE_FUNCTIONS)
    query = delete_func()

    start_time = time.time()
    try:
        client.command(query)
        duration_ms = (time.time() - start_time) * 1000
        return True, delete_func.__name__, duration_ms, 'mutation', 'DELETE'
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        return False, delete_func.__name__, duration_ms, str(e), 'DELETE'


def run_load_generator():
    """
    Main load generator loop.

    Target: ~15 operations per second with randomized timing
    Distribution:
    - 40% SELECT queries
    - 40% INSERT operations
    - 10% UPDATE operations (mutations)
    - 10% DELETE operations (mutations)
    """
    print("Starting query load generator...")
    print(f"Connecting to ClickHouse at {CLICKHOUSE_CONFIG['host']}...")

    client = get_client()
    print("Connected successfully!")
    print(f"\nTarget: ~{TARGET_QPS} operations per second")
    print("Distribution: 40% SELECT, 40% INSERT, 10% UPDATE, 10% DELETE")
    print("Press Ctrl+C to stop\n")

    # Calculate sleep interval for target QPS
    base_interval = 1.0 / TARGET_QPS

    total_ops = 0
    successful_ops = 0
    failed_ops = 0
    total_duration = 0
    start_time = time.time()

    # Stats by operation type
    op_stats = {
        'SELECT': {'count': 0, 'success': 0, 'errors': 0, 'total_ms': 0},
        'INSERT': {'count': 0, 'success': 0, 'errors': 0, 'total_ms': 0},
        'UPDATE': {'count': 0, 'success': 0, 'errors': 0, 'total_ms': 0},
        'DELETE': {'count': 0, 'success': 0, 'errors': 0, 'total_ms': 0},
    }

    # Stats by query/operation name
    query_stats = {}

    try:
        while True:
            # Randomly choose operation type based on distribution
            # 40% SELECT, 40% INSERT, 10% UPDATE, 10% DELETE
            op_roll = random.random()

            if op_roll < 0.4:
                # SELECT (40%)
                success, op_name, duration_ms, result, op_type = execute_random_select(client)
            elif op_roll < 0.8:
                # INSERT (40%)
                success, op_name, duration_ms, result, op_type = execute_random_insert(client)
            elif op_roll < 0.9:
                # UPDATE (10%)
                success, op_name, duration_ms, result, op_type = execute_random_update(client)
            else:
                # DELETE (10%)
                success, op_name, duration_ms, result, op_type = execute_random_delete(client)

            total_ops += 1
            total_duration += duration_ms
            op_stats[op_type]['count'] += 1
            op_stats[op_type]['total_ms'] += duration_ms

            if op_name not in query_stats:
                query_stats[op_name] = {'count': 0, 'total_ms': 0, 'errors': 0}

            if success:
                successful_ops += 1
                op_stats[op_type]['success'] += 1
                query_stats[op_name]['count'] += 1
                query_stats[op_name]['total_ms'] += duration_ms
                print(f"[{op_type}] {op_name}: {duration_ms:.1f}ms, {result}")
            else:
                failed_ops += 1
                op_stats[op_type]['errors'] += 1
                query_stats[op_name]['errors'] += 1
                print(f"[{op_type} ERR] {op_name}: {duration_ms:.1f}ms - {result}")

            # Print summary every 100 operations
            if total_ops % 100 == 0:
                elapsed = time.time() - start_time
                actual_qps = total_ops / elapsed
                avg_duration = total_duration / total_ops
                print(f"\n--- Summary after {total_ops} operations ---")
                print(f"Elapsed: {elapsed:.1f}s | OPS: {actual_qps:.1f} | Avg duration: {avg_duration:.1f}ms")
                print(f"Success: {successful_ops} | Failed: {failed_ops}")
                print(f"SELECT: {op_stats['SELECT']['count']} | INSERT: {op_stats['INSERT']['count']} | UPDATE: {op_stats['UPDATE']['count']} | DELETE: {op_stats['DELETE']['count']}")
                print("---\n")

            # Add randomness to interval for realistic load pattern
            sleep_time = base_interval * random.uniform(0.5, 1.5)

            # Account for operation execution time
            sleep_time = max(0, sleep_time - (duration_ms / 1000))
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        elapsed = time.time() - start_time
        print("\n\n=== Final Statistics ===")
        print(f"Total operations: {total_ops}")
        print(f"Successful: {successful_ops}")
        print(f"Failed: {failed_ops}")
        print(f"Total time: {elapsed:.1f}s")
        print(f"Average OPS: {total_ops / elapsed:.2f}")
        print(f"Average duration: {total_duration / max(total_ops, 1):.1f}ms")

        print("\n--- Operation Type Breakdown ---")
        for op_type, stats in op_stats.items():
            if stats['count'] > 0:
                avg = stats['total_ms'] / stats['count']
                pct = (stats['count'] / total_ops) * 100
                print(f"{op_type}: {stats['count']} ({pct:.1f}%), avg {avg:.1f}ms, {stats['errors']} errors")

        print("\n--- Query/Operation Breakdown ---")
        for name, stats in sorted(query_stats.items(), key=lambda x: x[1]['count'], reverse=True):
            if stats['count'] > 0:
                avg = stats['total_ms'] / stats['count']
                print(f"{name}: {stats['count']} ops, avg {avg:.1f}ms, {stats['errors']} errors")

        print("\nLoad generation stopped.")
    except Exception as e:
        print(f"\nError: {e}")
        raise
    finally:
        client.close()
        print("Connection closed.")


if __name__ == '__main__':
    run_load_generator()
