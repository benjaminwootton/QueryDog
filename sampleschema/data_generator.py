#!/usr/bin/env python3
"""
Ecommerce Data Generator for ClickHouse

Generates realistic ecommerce data at approximately 50 records per minute
across customers, orders, page_views, and shopping_cart tables.
Uses Faker for realistic data and introduces randomness in update frequencies.
"""

import os
import random
import time
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional

import clickhouse_connect
from dotenv import load_dotenv
from faker import Faker

# Load environment variables
load_dotenv()

# Initialize Faker with multiple locales for variety
fake = Faker(['en_US', 'en_GB', 'en_CA', 'en_AU'])

# ClickHouse connection settings
CLICKHOUSE_CONFIG = {
    'host': os.getenv('CLICKHOUSE_HOST', 'localhost'),
    'port': int(os.getenv('CLICKHOUSE_PORT_HTTP', '8443')),
    'username': os.getenv('CLICKHOUSE_USER', 'default'),
    'password': os.getenv('CLICKHOUSE_PASSWORD', ''),
    'database': os.getenv('CLICKHOUSE_DATABASE', 'ecommerce'),
    'secure': os.getenv('CLICKHOUSE_SECURE', '1') == '1',
}

# Constants for realistic data
GENDERS = ['male', 'female', 'non_binary', 'prefer_not_to_say', 'other']
ACCOUNT_STATUSES = ['active', 'inactive', 'suspended', 'pending_verification']
PREFERRED_CHANNELS = ['email', 'sms', 'push', 'mail', 'phone']
CUSTOMER_SEGMENTS = ['new', 'regular', 'vip', 'churned', 'at_risk', 'high_value']
LOYALTY_TIERS = ['bronze', 'silver', 'gold', 'platinum', 'diamond']
ORDER_STATUSES = ['pending', 'confirmed', 'processing', 'shipped', 'delivered', 'cancelled', 'refunded']
PAYMENT_METHODS = ['credit_card', 'debit_card', 'paypal', 'apple_pay', 'google_pay', 'bank_transfer', 'crypto']
PAYMENT_STATUSES = ['pending', 'completed', 'failed', 'refunded', 'chargeback']
SHIPPING_METHODS = ['standard', 'express', 'overnight', 'economy', 'pickup']
SHIPPING_CARRIERS = ['fedex', 'ups', 'usps', 'dhl', 'amazon_logistics', 'ontrac']
SOURCE_CHANNELS = ['organic', 'paid_search', 'social', 'email', 'referral', 'direct', 'affiliate']
PAGE_TYPES = ['home', 'category', 'product', 'search', 'cart', 'checkout', 'account', 'blog', 'about', 'contact']
DEVICE_TYPES = ['desktop', 'mobile', 'tablet']
BROWSERS = ['chrome', 'safari', 'firefox', 'edge', 'opera', 'samsung_browser']
OPERATING_SYSTEMS = ['windows', 'macos', 'ios', 'android', 'linux']
CART_STATUSES = ['active', 'abandoned', 'converted', 'expired']
CURRENCIES = ['USD', 'EUR', 'GBP', 'CAD', 'AUD']

# Product catalog (simplified)
PRODUCT_CATEGORIES = ['electronics', 'clothing', 'home_garden', 'sports', 'books', 'beauty', 'toys', 'food', 'automotive']
PRODUCTS = [
    {'id': f'PROD-{i:04d}', 'name': fake.catch_phrase(), 'category': random.choice(PRODUCT_CATEGORIES), 'price': round(random.uniform(9.99, 499.99), 2)}
    for i in range(1, 101)
]

# Track existing entities for relationships
existing_customers = []
existing_sessions = []


def get_client():
    """Create ClickHouse client connection."""
    return clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)


def generate_customer() -> dict:
    """Generate a realistic customer record."""
    customer_id = uuid.uuid4()
    registration_date = fake.date_time_between(start_date='-2y', end_date='now')
    last_login = fake.date_time_between(start_date=registration_date, end_date='now')

    # Generate realistic address
    country = random.choice(['US', 'UK', 'CA', 'AU'])
    if country == 'US':
        state = fake.state_abbr()
        city = fake.city()
        postal = fake.zipcode()
    elif country == 'UK':
        state = fake.county()
        city = fake.city()
        postal = fake.postcode()
    elif country == 'CA':
        state = fake.province_abbr()
        city = fake.city()
        postal = fake.postalcode()
    else:
        state = fake.state()
        city = fake.city()
        postal = fake.postcode()

    segment = random.choice(CUSTOMER_SEGMENTS)
    loyalty_tier = random.choice(LOYALTY_TIERS)
    total_orders = random.randint(0, 50) if segment in ['vip', 'high_value', 'regular'] else random.randint(0, 5)
    total_spent = round(random.uniform(0, 10000) if total_orders > 0 else 0, 2)

    customer = {
        'customer_id': customer_id,
        'email': fake.email(),
        'first_name': fake.first_name(),
        'last_name': fake.last_name(),
        'phone_number': fake.phone_number() if random.random() > 0.2 else None,
        'date_of_birth': fake.date_of_birth(minimum_age=18, maximum_age=54),  # max_age=54 ensures dates after 1970
        'gender': random.choice(GENDERS),
        'registration_date': registration_date,
        'last_login_date': last_login,
        'account_status': random.choices(ACCOUNT_STATUSES, weights=[0.85, 0.08, 0.02, 0.05])[0],
        'email_verified': random.choices([1, 0], weights=[0.9, 0.1])[0],
        'phone_verified': random.choices([1, 0], weights=[0.6, 0.4])[0],
        'shipping_address_line1': fake.street_address(),
        'shipping_address_line2': fake.secondary_address() if random.random() > 0.7 else None,
        'shipping_city': city,
        'shipping_state': state,
        'shipping_postal_code': postal,
        'shipping_country': country,
        'marketing_opt_in': random.choices([1, 0], weights=[0.7, 0.3])[0],
        'preferred_channel': random.choice(PREFERRED_CHANNELS),
        'customer_segment': segment,
        'total_orders': total_orders,
        'total_spent': Decimal(str(total_spent)),
        'average_order_value': Decimal(str(round(total_spent / max(total_orders, 1), 2))),
        'loyalty_points': random.randint(0, 10000),
        'loyalty_tier': loyalty_tier,
        'created_at': datetime.now(),
        'updated_at': datetime.now(),
    }

    existing_customers.append({'id': customer_id, 'email': customer['email'],
                               'first_name': customer['first_name'], 'last_name': customer['last_name'],
                               'segment': segment})
    return customer


def generate_order(customer: Optional[dict] = None) -> dict:
    """Generate a realistic order record."""
    if customer is None and existing_customers:
        customer = random.choice(existing_customers)
    elif customer is None:
        # Generate minimal customer info
        customer = {
            'id': uuid.uuid4(),
            'email': fake.email(),
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'segment': random.choice(CUSTOMER_SEGMENTS)
        }

    order_date = fake.date_time_between(start_date='-7d', end_date='now')
    order_status = random.choices(ORDER_STATUSES, weights=[0.05, 0.1, 0.15, 0.2, 0.4, 0.05, 0.05])[0]

    # Generate shipped/delivered dates based on status
    shipped_date = None
    delivered_date = None
    if order_status in ['shipped', 'delivered']:
        shipped_date = order_date + timedelta(days=random.randint(1, 3))
    if order_status == 'delivered':
        delivered_date = shipped_date + timedelta(days=random.randint(1, 5))

    # Generate order items
    num_items = random.randint(1, 5)
    selected_products = random.sample(PRODUCTS, num_items)
    quantities = [random.randint(1, 3) for _ in range(num_items)]

    subtotal = sum(p['price'] * q for p, q in zip(selected_products, quantities))
    tax_rate = random.uniform(0.05, 0.12)
    tax_amount = round(subtotal * tax_rate, 2)
    shipping_cost = round(random.uniform(0, 15.99), 2) if subtotal < 50 else 0
    discount_amount = round(subtotal * random.uniform(0, 0.2), 2) if random.random() > 0.7 else 0
    total_amount = round(subtotal + tax_amount + shipping_cost - discount_amount, 2)

    country = random.choice(['US', 'UK', 'CA', 'AU', 'DE', 'FR'])

    return {
        'order_id': uuid.uuid4(),
        'order_number': f'ORD-{fake.unique.random_number(digits=8)}',
        'customer_id': customer['id'],
        'customer_email': customer['email'],
        'customer_first_name': customer['first_name'],
        'customer_last_name': customer['last_name'],
        'customer_segment': customer['segment'],
        'order_status': order_status,
        'order_date': order_date,
        'shipped_date': shipped_date,
        'delivered_date': delivered_date,
        'subtotal': Decimal(str(round(subtotal, 2))),
        'tax_amount': Decimal(str(tax_amount)),
        'shipping_cost': Decimal(str(shipping_cost)),
        'discount_amount': Decimal(str(discount_amount)),
        'total_amount': Decimal(str(total_amount)),
        'currency': random.choices(CURRENCIES, weights=[0.6, 0.15, 0.1, 0.1, 0.05])[0],
        'payment_method': random.choice(PAYMENT_METHODS),
        'payment_status': 'completed' if order_status not in ['pending', 'cancelled'] else random.choice(['pending', 'failed']),
        'transaction_id': fake.uuid4() if order_status not in ['pending', 'cancelled'] else None,
        'shipping_method': random.choice(SHIPPING_METHODS),
        'shipping_carrier': random.choice(SHIPPING_CARRIERS),
        'tracking_number': fake.uuid4()[:12].upper() if order_status in ['shipped', 'delivered'] else None,
        'shipping_address_city': fake.city(),
        'shipping_address_state': fake.state_abbr() if country == 'US' else fake.state(),
        'shipping_address_country': country,
        'item_product_ids': [p['id'] for p in selected_products],
        'item_product_names': [p['name'] for p in selected_products],
        'item_quantities': quantities,
        'item_unit_prices': [Decimal(str(p['price'])) for p in selected_products],
        'item_categories': [p['category'] for p in selected_products],
        'source_channel': random.choice(SOURCE_CHANNELS),
        'campaign_id': f'CAMP-{random.randint(1000, 9999)}' if random.random() > 0.5 else None,
        'coupon_code': fake.lexify(text='????').upper() + str(random.randint(10, 99)) if discount_amount > 0 else None,
        'created_at': datetime.now(),
        'updated_at': datetime.now(),
    }


def generate_page_view() -> dict:
    """Generate a realistic page view record."""
    session_id = uuid.uuid4()
    customer_id = random.choice(existing_customers)['id'] if existing_customers and random.random() > 0.4 else None

    page_type = random.choices(PAGE_TYPES, weights=[0.15, 0.2, 0.3, 0.1, 0.08, 0.05, 0.04, 0.04, 0.02, 0.02])[0]

    # Generate product context if product page
    product = random.choice(PRODUCTS) if page_type == 'product' else None

    # Generate search context if search page
    search_query = fake.word() + ' ' + fake.word() if page_type == 'search' else None
    search_results = random.randint(0, 500) if search_query else None

    device_type = random.choices(DEVICE_TYPES, weights=[0.45, 0.45, 0.1])[0]
    browser = random.choice(BROWSERS)
    os = random.choice(OPERATING_SYSTEMS)

    # Generate UTM parameters
    has_utm = random.random() > 0.6
    utm_source = random.choice(['google', 'facebook', 'instagram', 'twitter', 'email', 'bing']) if has_utm else None
    utm_medium = random.choice(['cpc', 'organic', 'social', 'email', 'referral']) if has_utm else None
    utm_campaign = f'campaign_{random.randint(1, 100)}' if has_utm else None

    view_timestamp = fake.date_time_between(start_date='-1d', end_date='now')

    existing_sessions.append({'id': session_id, 'customer_id': customer_id})

    return {
        'view_id': uuid.uuid4(),
        'session_id': session_id,
        'customer_id': customer_id,
        'anonymous_id': fake.uuid4() if customer_id is None else fake.uuid4(),
        'page_url': f'https://shop.example.com/{page_type}/{fake.slug()}',
        'page_path': f'/{page_type}/{fake.slug()}',
        'page_title': f'{fake.catch_phrase()} | Example Shop',
        'page_type': page_type,
        'product_id': product['id'] if product else None,
        'product_name': product['name'] if product else None,
        'product_category': product['category'] if product else None,
        'product_price': Decimal(str(product['price'])) if product else None,
        'search_query': search_query,
        'search_results_count': search_results,
        'referrer_url': fake.url() if random.random() > 0.4 else None,
        'referrer_domain': fake.domain_name() if random.random() > 0.4 else None,
        'utm_source': utm_source,
        'utm_medium': utm_medium,
        'utm_campaign': utm_campaign,
        'device_type': device_type,
        'browser': browser,
        'browser_version': f'{random.randint(80, 120)}.0.{random.randint(0, 9999)}',
        'os': os,
        'os_version': f'{random.randint(10, 15)}.{random.randint(0, 9)}',
        'screen_resolution': random.choice(['1920x1080', '1366x768', '1536x864', '2560x1440', '390x844', '414x896']),
        'ip_address': fake.ipv4(),
        'geo_country': random.choice(['US', 'UK', 'CA', 'AU', 'DE', 'FR', 'JP', 'BR']),
        'geo_region': fake.state_abbr(),
        'geo_city': fake.city(),
        'page_load_time_ms': random.randint(200, 5000),
        'time_on_page_seconds': random.randint(5, 300),
        'scroll_depth_percent': random.randint(10, 100),
        'clicks_count': random.randint(0, 20),
        'add_to_cart_clicked': 1 if page_type == 'product' and random.random() > 0.85 else 0,
        'buy_now_clicked': 1 if page_type == 'product' and random.random() > 0.95 else 0,
        'view_timestamp': view_timestamp,
        'created_at': datetime.now(),
    }


def generate_shopping_cart() -> dict:
    """Generate a realistic shopping cart record."""
    session = random.choice(existing_sessions) if existing_sessions else {'id': uuid.uuid4(), 'customer_id': None}
    customer_id = session.get('customer_id') or (random.choice(existing_customers)['id'] if existing_customers and random.random() > 0.5 else None)

    cart_status = random.choices(CART_STATUSES, weights=[0.3, 0.4, 0.25, 0.05])[0]
    cart_created = fake.date_time_between(start_date='-1d', end_date='now')

    # Generate cart items
    num_items = random.randint(1, 6)
    selected_products = random.sample(PRODUCTS, num_items)
    quantities = [random.randint(1, 3) for _ in range(num_items)]
    item_timestamps = [cart_created + timedelta(minutes=random.randint(0, 30)) for _ in range(num_items)]

    subtotal = sum(p['price'] * q for p, q in zip(selected_products, quantities))
    estimated_tax = round(subtotal * random.uniform(0.05, 0.12), 2)
    estimated_shipping = round(random.uniform(0, 12.99), 2) if subtotal < 50 else 0
    discount_amount = round(subtotal * random.uniform(0, 0.15), 2) if random.random() > 0.8 else 0
    estimated_total = round(subtotal + estimated_tax + estimated_shipping - discount_amount, 2)

    cart_updated = cart_created + timedelta(minutes=random.randint(1, 60))
    cart_abandoned = cart_updated + timedelta(hours=random.randint(1, 24)) if cart_status == 'abandoned' else None
    cart_converted = cart_updated + timedelta(minutes=random.randint(5, 30)) if cart_status == 'converted' else None

    device_type = random.choices(DEVICE_TYPES, weights=[0.4, 0.5, 0.1])[0]

    has_utm = random.random() > 0.5

    return {
        'cart_id': uuid.uuid4(),
        'session_id': session['id'],
        'customer_id': customer_id,
        'anonymous_id': fake.uuid4(),
        'cart_status': cart_status,
        'cart_created_at': cart_created,
        'cart_updated_at': cart_updated,
        'cart_abandoned_at': cart_abandoned,
        'cart_converted_at': cart_converted,
        'converted_order_id': uuid.uuid4() if cart_status == 'converted' else None,
        'item_product_ids': [p['id'] for p in selected_products],
        'item_product_names': [p['name'] for p in selected_products],
        'item_product_categories': [p['category'] for p in selected_products],
        'item_quantities': quantities,
        'item_unit_prices': [Decimal(str(p['price'])) for p in selected_products],
        'item_total_prices': [Decimal(str(round(p['price'] * q, 2))) for p, q in zip(selected_products, quantities)],
        'item_added_timestamps': item_timestamps,
        'items_count': sum(quantities),
        'unique_items_count': num_items,
        'subtotal': Decimal(str(round(subtotal, 2))),
        'estimated_tax': Decimal(str(estimated_tax)),
        'estimated_shipping': Decimal(str(estimated_shipping)),
        'discount_amount': Decimal(str(discount_amount)),
        'estimated_total': Decimal(str(estimated_total)),
        'currency': random.choices(CURRENCIES, weights=[0.6, 0.15, 0.1, 0.1, 0.05])[0],
        'coupon_codes': [fake.lexify(text='????').upper() + str(random.randint(10, 99))] if discount_amount > 0 else [],
        'promotion_ids': [f'PROMO-{random.randint(100, 999)}'] if random.random() > 0.8 else [],
        'source_channel': random.choice(SOURCE_CHANNELS),
        'landing_page_url': f'https://shop.example.com/{random.choice(["", "sale/", "new/", "category/"])}{fake.slug()}',
        'utm_source': random.choice(['google', 'facebook', 'instagram', 'email']) if has_utm else None,
        'utm_medium': random.choice(['cpc', 'social', 'email']) if has_utm else None,
        'utm_campaign': f'campaign_{random.randint(1, 50)}' if has_utm else None,
        'device_type': device_type,
        'browser': random.choice(BROWSERS),
        'recovery_emails_sent': random.randint(0, 3) if cart_status == 'abandoned' else 0,
        'last_recovery_email_at': cart_abandoned + timedelta(hours=random.randint(1, 12)) if cart_status == 'abandoned' and random.random() > 0.5 else None,
        'created_at': datetime.now(),
        'updated_at': datetime.now(),
    }


def insert_customers(client, count: int = 1):
    """Insert customer records."""
    customers = [generate_customer() for _ in range(count)]

    columns = list(customers[0].keys())
    data = [[c[col] for col in columns] for c in customers]

    client.insert('customers', data, column_names=columns)
    print(f"Inserted {count} customer(s)")


def insert_orders(client, count: int = 1):
    """Insert order records."""
    orders = [generate_order() for _ in range(count)]

    columns = list(orders[0].keys())
    data = [[o[col] for col in columns] for o in orders]

    client.insert('orders', data, column_names=columns)
    print(f"Inserted {count} order(s)")


def insert_page_views(client, count: int = 1):
    """Insert page view records."""
    page_views = [generate_page_view() for _ in range(count)]

    columns = list(page_views[0].keys())
    data = [[pv[col] for col in columns] for pv in page_views]

    client.insert('page_views', data, column_names=columns)
    print(f"Inserted {count} page view(s)")


def insert_shopping_carts(client, count: int = 1):
    """Insert shopping cart records."""
    carts = [generate_shopping_cart() for _ in range(count)]

    columns = list(carts[0].keys())
    data = [[c[col] for col in columns] for c in carts]

    client.insert('shopping_cart', data, column_names=columns)
    print(f"Inserted {count} shopping cart(s)")


def run_generator():
    """
    Main generator loop.

    Target: ~50 records per minute across all tables
    Distribution (with randomness):
    - Page views: ~25-30 per minute (most frequent)
    - Shopping carts: ~10-15 per minute
    - Orders: ~5-8 per minute
    - Customers: ~2-5 per minute (least frequent)
    """
    print("Starting ecommerce data generator...")
    print(f"Connecting to ClickHouse at {CLICKHOUSE_CONFIG['host']}...")

    client = get_client()
    print("Connected successfully!")

    # Pre-populate some customers and sessions for realistic relationships
    print("Pre-populating initial customers...")
    insert_customers(client, 10)

    print("Pre-populating initial page views (for sessions)...")
    insert_page_views(client, 20)

    print("\nStarting continuous data generation (~50 records/minute)...")
    print("Press Ctrl+C to stop\n")

    iteration = 0

    try:
        while True:
            iteration += 1

            # Randomize the number of records for each table
            # This creates natural variation in data flow

            # Page views: Most frequent (5-8 per batch, ~25-30/min)
            page_view_count = random.randint(5, 8)

            # Shopping carts: Medium frequency (2-4 per batch, ~10-15/min)
            cart_count = random.randint(2, 4)

            # Orders: Lower frequency (1-2 per batch, ~5-8/min)
            order_count = random.randint(1, 2)

            # Customers: Least frequent (0-1 per batch, ~2-5/min)
            customer_count = random.randint(0, 1)

            # Insert records with some randomness in order
            operations = []
            if customer_count > 0:
                operations.append(('customers', customer_count))
            operations.append(('page_views', page_view_count))
            operations.append(('shopping_cart', cart_count))
            operations.append(('orders', order_count))

            # Shuffle to add variety
            random.shuffle(operations)

            total_inserted = 0
            for table, count in operations:
                if table == 'customers':
                    insert_customers(client, count)
                elif table == 'page_views':
                    insert_page_views(client, count)
                elif table == 'shopping_cart':
                    insert_shopping_carts(client, count)
                elif table == 'orders':
                    insert_orders(client, count)
                total_inserted += count

            print(f"[Iteration {iteration}] Total: {total_inserted} records inserted")

            # Sleep to achieve ~50 records/minute
            # With average of ~12 records per iteration, sleep ~15 seconds
            sleep_time = random.uniform(12, 18)
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("\n\nData generation stopped by user.")
    except Exception as e:
        print(f"\nError: {e}")
        raise
    finally:
        client.close()
        print("Connection closed.")


if __name__ == '__main__':
    run_generator()
