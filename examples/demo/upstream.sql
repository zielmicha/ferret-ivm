-- Upstream schema and seed data for the ivm demo (database "shop").

CREATE TABLE regions (id int PRIMARY KEY, name text NOT NULL);
CREATE TABLE customers (
    id int PRIMARY KEY,
    name text NOT NULL,
    region_id int NOT NULL REFERENCES regions,
    tier text NOT NULL,
    signed_up timestamptz NOT NULL,
    prefs jsonb
);
CREATE TABLE products (
    id int PRIMARY KEY,
    name text NOT NULL,
    category text NOT NULL,
    price numeric(10,2) NOT NULL,
    stock int NOT NULL
);
CREATE TABLE orders (
    id bigint PRIMARY KEY,
    customer_id int NOT NULL,
    status text NOT NULL,
    created timestamptz NOT NULL,
    note text
);
CREATE TABLE order_items (
    order_id bigint NOT NULL,
    line int NOT NULL,
    product_id int NOT NULL,
    qty int NOT NULL,
    unit_price numeric(10,2) NOT NULL,
    PRIMARY KEY (order_id, line)
);
CREATE TABLE events (
    id bigint PRIMARY KEY,
    customer_id int,
    kind text NOT NULL,
    payload jsonb,
    at timestamptz NOT NULL
);

-- h(x): a stable pseudo-random non-negative integer
CREATE FUNCTION h(x text) RETURNS bigint IMMUTABLE LANGUAGE sql AS $$ SELECT abs(hashtext(x)::bigint) $$;

INSERT INTO regions VALUES (1, 'north'), (2, 'south'), (3, 'east'), (4, 'west'), (5, 'central');

INSERT INTO customers
SELECT g, 'customer ' || g, 1 + h('region' || g) % 5, (ARRAY['basic', 'silver', 'gold'])[1 + h('tier' || g) % 3],
       now() - make_interval(hours => g),
       jsonb_build_object('newsletter', g % 2 = 0, 'tags', jsonb_build_array('t' || g % 4))
FROM generate_series(1, 2000) g;

INSERT INTO products
SELECT g, 'product ' || g, (ARRAY['books', 'games', 'tools', 'garden', 'music', 'toys'])[1 + h('cat' || g) % 6],
       5 + (h('price' || g) % 20000) / 100.0, 5 + h('stock' || g) % 60
FROM generate_series(1, 300) g;

INSERT INTO orders
SELECT g, 1 + h('cust' || g) % 2000,
       (ARRAY['pending', 'paid', 'shipped', 'delivered', 'delivered', 'delivered', 'cancelled'])[1 + h('status' || g) % 7],
       now() - make_interval(secs => 20000 - g), NULL
FROM generate_series(1, 20000) g;

INSERT INTO order_items
SELECT o, l, p.id, 1 + h('qty' || o || ':' || l) % 4, p.price
FROM generate_series(1, 20000) o
CROSS JOIN LATERAL generate_series(1, 1 + h('lines' || o)::int % 4) l
JOIN products p ON p.id = 1 + h('item' || o || ':' || l) % 300;

INSERT INTO events
SELECT g, 1 + g % 2000, (ARRAY['view', 'cart', 'search', 'review'])[1 + g % 4],
       jsonb_build_object('items', jsonb_build_array(
           jsonb_build_object('product', 1 + h('ev' || g) % 300, 'price', h('evp' || g) % 300))),
       now() - make_interval(secs => 5000 - g)
FROM generate_series(1, 5000) g;

CREATE PUBLICATION shop_pub FOR TABLE regions, customers, products, orders, order_items, events;
