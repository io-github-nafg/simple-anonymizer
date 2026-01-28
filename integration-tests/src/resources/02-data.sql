-- Test data for simple-anonymizer integration tests

INSERT INTO users (first_name, last_name, email) VALUES
    ('John', 'Doe', 'john.doe@example.com'),
    ('Jane', 'Smith', 'jane.smith@testmail.com'),
    ('Robert', 'Johnson', 'rjohnson@company.org'),
    ('Emily', 'Williams', 'emily.w@personal.net'),
    ('Michael', 'Brown', 'mbrown@workplace.com'),
    ('Sarah', 'Davis', 'sarah.davis@email.org'),
    ('David', 'Miller', 'dmiller@domain.com'),
    ('Jessica', 'Wilson', 'jwilson@business.net'),
    ('Christopher', 'Moore', 'cmoore@sample.org'),
    ('Amanda', 'Taylor', 'amanda.taylor@mail.com');

INSERT INTO categories (id, name, parent_id) VALUES
    (1, 'Electronics', NULL),
    (2, 'Clothing', NULL),
    (3, 'Books', NULL),
    (4, 'Smartphones', 1),
    (5, 'Laptops', 1),
    (6, 'Tablets', 1),
    (7, 'Mens Clothing', 2),
    (8, 'Womens Clothing', 2),
    (9, 'Fiction', 3),
    (10, 'Non-Fiction', 3);

INSERT INTO orders (user_id, total, status) VALUES
    (1, 299.99, 'completed'),
    (1, 149.50, 'completed'),
    (2, 89.99, 'pending'),
    (3, 1250.00, 'completed'),
    (3, 45.00, 'cancelled'),
    (4, 599.99, 'shipped'),
    (5, 199.99, 'completed'),
    (6, 79.50, 'pending'),
    (7, 349.00, 'completed'),
    (8, 425.75, 'shipped'),
    (9, 99.00, 'completed'),
    (10, 1599.99, 'processing');

INSERT INTO order_items (order_id, product_name, quantity) VALUES
    (1, 'iPhone 15 Pro', 1),
    (1, 'Phone Case', 2),
    (2, 'Wireless Earbuds', 1),
    (3, 'Cotton T-Shirt', 3),
    (4, 'MacBook Pro 14 inch', 1),
    (4, 'USB-C Cable', 2),
    (4, 'Laptop Sleeve', 1),
    (5, 'Programming Book', 1),
    (6, 'Smart Watch', 1),
    (6, 'Watch Band', 1),
    (7, 'Running Shoes', 1),
    (8, 'Summer Dress', 2),
    (9, 'Bluetooth Speaker', 1),
    (9, 'HDMI Cable', 1),
    (10, 'Fitness Tracker', 1),
    (11, 'Novel - Fiction', 2),
    (12, 'Gaming Laptop', 1);

INSERT INTO profiles (user_id, phones, settings) VALUES
    (1, '[{"type":"mobile","number":"555-0101"},{"type":"home","number":"555-0102"}]',
        '{"theme":"dark","notifications":true,"language":"en"}'),
    (2, '[{"type":"mobile","number":"555-0201"},{"type":"work","number":"555-0202"}]',
        '{"theme":"light","notifications":true,"language":"en"}'),
    (3, '[{"type":"mobile","number":"555-0301"}]',
        '{"theme":"dark","notifications":false,"language":"es"}'),
    (4, '[{"type":"mobile","number":"555-0401"},{"type":"home","number":"555-0402"},{"type":"work","number":"555-0403"}]',
        '{"theme":"auto","notifications":true,"language":"en"}'),
    (5, '[{"type":"mobile","number":"555-0501"}]',
        '{"theme":"light","notifications":true,"language":"fr"}'),
    (6, '[{"type":"work","number":"555-0601"}]',
        '{"theme":"dark","notifications":true,"language":"en"}'),
    (7, '[{"type":"mobile","number":"555-0701"},{"type":"mobile","number":"555-0702"}]',
        '{"theme":"light","notifications":false,"language":"de"}'),
    (8, '[{"type":"home","number":"555-0801"}]',
        '{"theme":"dark","notifications":true,"language":"en"}');
