#!/usr/bin/env python3
"""Continuous upstream activity for the ivm demo.

Runs small transactions against the upstream "shop" database: new orders
(which also draw down stock), order status changes and cancellations,
deletions of cancelled orders, restocks, events with JSON payloads and
customer changes, plus a periodic bulk import of a few thousand orders in a
single transaction, and a periodic "bad price update" (a product's price is
set to 0 for a while, which makes the demo's units_per_100 view report a
division-by-zero error until it is restored).
"""

import argparse
import json
import random
import time

import psycopg


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--conninfo", required=True, help="libpq connection string of the shop database")
    ap.add_argument("--rate", type=float, default=20.0, help="transactions per second (default 20)")
    ap.add_argument("--burst-every", type=float, default=120.0,
                    help="seconds between bulk imports, 0 to disable (default 120)")
    ap.add_argument("--burst-size", type=int, default=3000, help="orders per bulk import (default 3000)")
    ap.add_argument("--glitch-every", type=float, default=180.0,
                    help="seconds between bad price updates, 0 to disable (default 180)")
    ap.add_argument("--glitch-secs", type=float, default=20.0, help="how long a bad price lasts (default 20)")
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    conn = psycopg.connect(args.conninfo)
    cur = conn.cursor()
    cur.execute("SELECT coalesce(max(id), 0) + 1 FROM orders")
    next_order = cur.fetchone()[0]
    cur.execute("SELECT coalesce(max(id), 0) + 1 FROM events")
    next_event = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM customers")
    ncust = cur.fetchone()[0]
    cur.execute("SELECT id, price FROM products ORDER BY id")
    products = cur.fetchall()
    conn.commit()

    def recent_order():
        return rng.randint(max(1, next_order - 3000), max(1, next_order - 1))

    interval = 1.0 / args.rate
    last_burst = time.monotonic()
    last_glitch = time.monotonic()
    glitch = None  # (product id, original price, restore time)
    done = 0
    while True:
        t0 = time.monotonic()
        r = rng.random()
        try:
            if r < 0.45:  # a new order of 1-4 lines
                oid = next_order
                next_order += 1
                cur.execute("INSERT INTO orders VALUES (%s, %s, 'pending', now(), NULL)", (oid, rng.randint(1, ncust)))
                for line in range(1, rng.randint(1, 4) + 1):
                    pid, price = rng.choice(products)
                    qty = rng.randint(1, 3)
                    cur.execute("INSERT INTO order_items VALUES (%s, %s, %s, %s, %s)", (oid, line, pid, qty, price))
                    cur.execute("UPDATE products SET stock = greatest(stock - %s, 0) WHERE id = %s", (qty, pid))
            elif r < 0.70:  # move an order along
                cur.execute(
                    "UPDATE orders SET status = CASE status WHEN 'pending' THEN 'paid' WHEN 'paid' THEN 'shipped' "
                    "WHEN 'shipped' THEN 'delivered' ELSE status END WHERE id = %s",
                    (recent_order(),),
                )
            elif r < 0.77:  # cancel a pending order
                cur.execute("UPDATE orders SET status = 'cancelled', note = 'customer request' "
                            "WHERE id = %s AND status = 'pending'", (recent_order(),))
            elif r < 0.80:  # purge the oldest cancelled order
                cur.execute("SELECT id FROM orders WHERE status = 'cancelled' ORDER BY id LIMIT 1")
                row = cur.fetchone()
                if row:
                    cur.execute("DELETE FROM order_items WHERE order_id = %s", row)
                    cur.execute("DELETE FROM orders WHERE id = %s", row)
            elif r < 0.88:  # restock a product that is running low
                cur.execute("UPDATE products SET stock = stock + %s WHERE id = "
                            "(SELECT id FROM products WHERE stock < 30 ORDER BY random() LIMIT 1)",
                            (rng.randint(10, 40),))
            elif r < 0.97:  # a customer event with a JSON payload
                items = [{"product": rng.choice(products)[0], "price": rng.randint(1, 300)}
                         for _ in range(rng.randint(1, 3))]
                cur.execute("INSERT INTO events VALUES (%s, %s, %s, %s, now())",
                            (next_event, rng.randint(1, ncust), rng.choice(["view", "cart", "search", "review"]),
                             json.dumps({"items": items})))
                next_event += 1
            else:  # a customer changes tier and preferences
                cur.execute("UPDATE customers SET tier = %s, "
                            "prefs = jsonb_set(coalesce(prefs, '{}'), '{newsletter}', to_jsonb(%s::boolean)) "
                            "WHERE id = %s",
                            (rng.choice(["basic", "silver", "gold"]), rng.random() < 0.5, rng.randint(1, ncust)))
            conn.commit()
            done += 1
        except psycopg.Error as e:
            conn.rollback()
            print(f"loadgen: {e}", flush=True)

        if args.burst_every > 0 and time.monotonic() - last_burst >= args.burst_every:
            last_burst = time.monotonic()
            lo, hi = next_order, next_order + args.burst_size - 1
            cur.execute("INSERT INTO orders SELECT g, 1 + (g * 7919) %% %s, 'paid', now(), 'bulk import' "
                        "FROM generate_series(%s::bigint, %s::bigint) g", (ncust, lo, hi))
            cur.execute("INSERT INTO order_items SELECT g, 1, p.id, 1 + g %% 3, p.price "
                        "FROM generate_series(%s::bigint, %s::bigint) g JOIN products p ON p.id = 1 + (g * 31) %% 300",
                        (lo, hi))
            conn.commit()
            next_order = hi + 1
            print(f"loadgen: bulk import of {args.burst_size} orders ({done} transactions so far)", flush=True)

        now = time.monotonic()
        if glitch and now >= glitch[2]:
            cur.execute("UPDATE products SET price = %s WHERE id = %s", (glitch[1], glitch[0]))
            conn.commit()
            print(f"loadgen: restored the price of product {glitch[0]}", flush=True)
            glitch = None
        elif not glitch and args.glitch_every > 0 and now - last_glitch >= args.glitch_every:
            last_glitch = now
            pid, price = rng.choice(products)
            cur.execute("UPDATE products SET price = 0 WHERE id = %s", (pid,))
            conn.commit()
            glitch = (pid, price, now + args.glitch_secs)
            print(f"loadgen: bad price update: product {pid} now costs 0", flush=True)

        dt = time.monotonic() - t0
        if dt < interval:
            time.sleep(interval - dt)


if __name__ == "__main__":
    main()
