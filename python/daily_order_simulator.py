"""
AtliQ Commerce — Daily Transaction Simulator
============================================
Purpose: keep the OLTP database "alive" so the nightly OLTP -> OLAP sync has
new/changed rows to pick up. Run it once a day (or a few times) before the
nightly pipeline. It inserts fresh orders for random existing customers, using
existing products, and records a payment for each one.

This is what makes the incremental (watermark) sync feel real: every run
bumps updated_at on new rows, and the pipeline should pick up ONLY those.

All timestamps are written in UTC, the same clock ADF uses for @utcnow() and
the watermark, so no row is skipped or picked up twice because of a time zone.

Like the real storefront, new rows carry the same kinds of mess as the seed data
(inconsistent status / payment-method text, the odd double-submitted line item or
retried payment), so your Silver cleaning keeps earning its keep every night.
Pass --clean to switch that off while you debug.

Optional: --update-existing N also moves N existing orders one step along
their lifecycle (Placed -> Shipped -> Delivered -> Returned) and bumps their
updated_at. That exercises the "update when newer" branch of your Silver MERGE.

Prereqs:
    pip install -r requirements.txt
    ODBC Driver 18 for SQL Server installed.
Set these environment variables (or copy .env.example to .env):
    AZ_SQL_SERVER   e.g. atliq-sql.database.windows.net
    AZ_SQL_DB       e.g. atliq_commerce
    AZ_SQL_USER     e.g. atliq_admin
    AZ_SQL_PASSWORD your password

Run:  python daily_order_simulator.py --orders 20
      python daily_order_simulator.py --orders 20 --update-existing 5
      python daily_order_simulator.py --orders 20 --clean
"""
import os
import random
import argparse
from datetime import datetime, timedelta, timezone

import pyodbc
from dotenv import load_dotenv

load_dotenv()

STATUSES = ["Placed", "Placed", "Shipped", "Delivered"]
METHODS = ["UPI", "Credit Card", "Debit Card", "Net Banking", "Wallet", "COD"]
# one lifecycle step per update; payments are untouched, so revenue still reconciles
NEXT_STATUS = {"placed": "Shipped", "shipped": "Delivered", "delivered": "Returned"}

# the storefront's own inconsistencies (same kinds as in the seed data)
STATUS_TEXT = {"Placed": ["placed", "PLACED", "Placed "], "Shipped": ["shipped", " Shipped"],
               "Delivered": ["delivered", "DELIVERED"], "Returned": ["returned", "Returned "]}
METHOD_TEXT = {"UPI": ["upi", "UPI "], "Credit Card": ["credit card", "CreditCard"],
               "Debit Card": ["debit card", "DebitCard"], "Net Banking": ["NetBanking", "net banking"],
               "Wallet": ["wallet", "WALLET"], "COD": ["cod", "Cash on Delivery"]}
MESS = dict(status_text=0.08, method_text=0.12, dup_line_item=0.01, dup_payment=0.015)


def get_conn():
    server = os.environ["AZ_SQL_SERVER"]
    database = os.environ["AZ_SQL_DB"]
    user = os.environ["AZ_SQL_USER"]
    password = os.environ["AZ_SQL_PASSWORD"]
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={server};DATABASE={database};UID={user};PWD={password};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)


def messy(value: str, variants: dict, rate: float, clean: bool) -> str:
    if clean or random.random() >= rate or value not in variants:
        return value
    return random.choice(variants[value])


def advance_existing(cur, n_updates: int, now: datetime, clean: bool) -> int:
    """Move up to n existing orders one lifecycle step and bump their updated_at.

    Existing status text may be inconsistent ('delivered', ' Shipped'), so match on
    the normalised value, exactly as your Silver layer does.
    """
    rows = cur.execute(
        """SELECT TOP (?) order_id, status FROM dbo.orders
               WHERE LOWER(LTRIM(RTRIM(status))) IN ('placed', 'shipped', 'delivered')
               ORDER BY NEWID()""",
        n_updates,
    ).fetchall()
    for order_id, status in rows:
        nxt = NEXT_STATUS[status.strip().lower()]
        cur.execute("UPDATE dbo.orders SET status = ?, updated_at = ? WHERE order_id = ?",
                    messy(nxt, STATUS_TEXT, MESS["status_text"], clean), now, order_id)
    return len(rows)


def simulate(n_orders: int, n_updates: int = 0, clean: bool = False):
    conn = get_conn()
    cur = conn.cursor()

    # pull existing ids so every FK is valid (never place orders for QA test accounts)
    customer_ids = [r[0] for r in cur.execute(
        "SELECT customer_id FROM dbo.customers WHERE LOWER(email) NOT LIKE '%@atliq-test.com%'").fetchall()]
    products = cur.execute("SELECT product_id, unit_price FROM dbo.products").fetchall()

    # UTC, the same clock as ADF's @utcnow() and the watermark in etl.control_table
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    today = now.date()
    created_orders = 0

    # 0) optional: advance existing orders first, so today's new orders stay as created
    updated_orders = advance_existing(cur, n_updates, now, clean) if n_updates > 0 else 0

    for _ in range(n_orders):
        customer_id = random.choice(customer_ids)
        status = random.choice(STATUSES)

        # 1) insert order header (amount filled in after items)
        cur.execute(
            """INSERT INTO dbo.orders (customer_id, order_date, status, order_amount, created_at, updated_at)
                   OUTPUT INSERTED.order_id
                   VALUES (?, ?, ?, 0, ?, ?)""",
            customer_id, today, messy(status, STATUS_TEXT, MESS["status_text"], clean), now, now,
        )
        order_id = cur.fetchone()[0]

        # 2) 1-4 line items (occasionally double-submitted by the app)
        order_total = 0
        for i, p in enumerate(random.sample(products, random.randint(1, 4))):
            product_id, unit_price = int(p[0]), float(p[1])
            qty = random.randint(1, 3)
            copies = 2 if (i == 0 and not clean and random.random() < MESS["dup_line_item"]) else 1
            for _ in range(copies):
                cur.execute(
                    """INSERT INTO dbo.order_items (order_id, product_id, quantity, item_price, created_at)
                           VALUES (?, ?, ?, ?, ?)""",
                    order_id, product_id, qty, unit_price, now,
                )
            order_total += qty * unit_price          # the header always carries the true amount

        # 3) update header amount
        cur.execute("UPDATE dbo.orders SET order_amount = ?, updated_at = ? WHERE order_id = ?",
                    order_total, now, order_id)

        # 4) payment for non-cancelled (the gateway occasionally retries within minutes)
        if status != "Cancelled":
            method = random.choice(METHODS)
            paid_times = [now]
            if not clean and random.random() < MESS["dup_payment"]:
                paid_times.append(now + timedelta(seconds=random.randint(30, 180)))
            for paid_at in paid_times:
                cur.execute(
                    """INSERT INTO dbo.payments (order_id, amount, method, paid_at, updated_at)
                           VALUES (?, ?, ?, ?, ?)""",
                    order_id, order_total, messy(method, METHOD_TEXT, MESS["method_text"], clean),
                    paid_at, paid_at,
                )
        created_orders += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"[{now:%Y-%m-%d %H:%M} UTC] Simulated {created_orders} new orders"
          f" and advanced {updated_orders} existing orders{' (clean mode)' if clean else ''}.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Insert simulated daily orders into AtliQ OLTP.")
    ap.add_argument("--orders", type=int, default=20, help="how many orders to create this run")
    ap.add_argument("--update-existing", type=int, default=0,
                    help="also move N existing orders one lifecycle step (default 0 = off)")
    ap.add_argument("--clean", action="store_true",
                    help="write perfectly clean rows (handy while debugging); default mirrors real mess")
    args = ap.parse_args()
    simulate(args.orders, args.update_existing, args.clean)
