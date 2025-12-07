#!/usr/bin/env python3
"""
Query Load Generator for ClickHouse

Generates realistic query load against the ecommerce schema at approximately
15 queries per second. Uses 20 different query patterns with randomized
parameters to simulate real application usage.
"""

import os
import random
import time
from datetime import datetime, timedelta
from typing import Callable

import clickhouse_connect
from dotenv import load_dotenv

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


# List of all query functions
QUERY_FUNCTIONS: list[Callable[[], str]] = [
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


def get_client():
    """Create ClickHouse client connection."""
    return clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)


def execute_random_query(client) -> tuple:
    """Execute a random query and return stats."""
    query_func = random.choice(QUERY_FUNCTIONS)
    query = query_func()

    start_time = time.time()
    try:
        result = client.query(query)
        duration_ms = (time.time() - start_time) * 1000
        row_count = result.row_count
        return True, query_func.__name__, duration_ms, row_count
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        return False, query_func.__name__, duration_ms, str(e)


def run_load_generator():
    """
    Main load generator loop.

    Target: ~15 queries per second with randomized timing
    """
    print("Starting query load generator...")
    print(f"Connecting to ClickHouse at {CLICKHOUSE_CONFIG['host']}...")

    client = get_client()
    print("Connected successfully!")
    print(f"\nTarget: ~{TARGET_QPS} queries per second")
    print("Press Ctrl+C to stop\n")

    # Calculate sleep interval for target QPS
    base_interval = 1.0 / TARGET_QPS

    total_queries = 0
    successful_queries = 0
    failed_queries = 0
    total_duration = 0
    start_time = time.time()

    query_stats = {func.__name__: {'count': 0, 'total_ms': 0, 'errors': 0} for func in QUERY_FUNCTIONS}

    try:
        while True:
            success, query_name, duration_ms, result = execute_random_query(client)
            total_queries += 1
            total_duration += duration_ms

            if success:
                successful_queries += 1
                query_stats[query_name]['count'] += 1
                query_stats[query_name]['total_ms'] += duration_ms
                print(f"[OK] {query_name}: {duration_ms:.1f}ms, {result} rows")
            else:
                failed_queries += 1
                query_stats[query_name]['errors'] += 1
                print(f"[ERR] {query_name}: {duration_ms:.1f}ms - {result}")

            # Print summary every 100 queries
            if total_queries % 100 == 0:
                elapsed = time.time() - start_time
                actual_qps = total_queries / elapsed
                avg_duration = total_duration / total_queries
                print(f"\n--- Summary after {total_queries} queries ---")
                print(f"Elapsed: {elapsed:.1f}s | QPS: {actual_qps:.1f} | Avg duration: {avg_duration:.1f}ms")
                print(f"Success: {successful_queries} | Failed: {failed_queries}")
                print("---\n")

            # Add randomness to interval for realistic load pattern
            # Vary between 50% and 150% of base interval
            sleep_time = base_interval * random.uniform(0.5, 1.5)

            # Account for query execution time
            sleep_time = max(0, sleep_time - (duration_ms / 1000))
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        elapsed = time.time() - start_time
        print("\n\n=== Final Statistics ===")
        print(f"Total queries: {total_queries}")
        print(f"Successful: {successful_queries}")
        print(f"Failed: {failed_queries}")
        print(f"Total time: {elapsed:.1f}s")
        print(f"Average QPS: {total_queries / elapsed:.2f}")
        print(f"Average duration: {total_duration / max(total_queries, 1):.1f}ms")

        print("\n--- Query Type Breakdown ---")
        for name, stats in sorted(query_stats.items(), key=lambda x: x[1]['count'], reverse=True):
            if stats['count'] > 0:
                avg = stats['total_ms'] / stats['count']
                print(f"{name}: {stats['count']} queries, avg {avg:.1f}ms, {stats['errors']} errors")

        print("\nQuery load generation stopped.")
    except Exception as e:
        print(f"\nError: {e}")
        raise
    finally:
        client.close()
        print("Connection closed.")


if __name__ == '__main__':
    run_load_generator()
