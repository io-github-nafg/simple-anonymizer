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

-- Self-referencing FK: parent_id -> categories(id). Cross-table FK: owner_id -> users(id).
-- Physical insert order puts children before parents to stress batch FK handling.
--
-- Edge case: Fiction (id=9, owner_id=1) is a child of Books (id=3, owner_id=3).
-- When subsetting to owner_id IN (1,2), Fiction individually matches but its parent doesn't,
-- so Fiction should be excluded.
INSERT INTO categories (id, name, owner_id, parent_id) VALUES
    (4, 'Smartphones', 1, NULL),
    (5, 'Laptops', 1, NULL),
    (6, 'Tablets', 1, NULL),
    (7, 'Mens Clothing', 2, NULL),
    (8, 'Womens Clothing', 2, NULL),
    (9, 'Fiction', 1, NULL),
    (10, 'Non-Fiction', 3, NULL),
    (1, 'Electronics', 1, NULL),
    (2, 'Clothing', 2, NULL),
    (3, 'Books', 3, NULL);
UPDATE categories SET parent_id = 1 WHERE id = 4;
UPDATE categories SET parent_id = 1 WHERE id = 5;
UPDATE categories SET parent_id = 1 WHERE id = 6;
UPDATE categories SET parent_id = 2 WHERE id = 7;
UPDATE categories SET parent_id = 2 WHERE id = 8;
UPDATE categories SET parent_id = 3 WHERE id = 9;
UPDATE categories SET parent_id = 3 WHERE id = 10;
UPDATE categories SET parent_id = NULL WHERE id IN (1, 2, 3);

-- Employees with two self-referencing FKs (manager_id and mentor_id).
-- Physical row order puts subordinates before their managers/mentors.
INSERT INTO employees (id, name, manager_id, mentor_id) VALUES
    (4, 'Alice', NULL, NULL),
    (5, 'Bob', NULL, NULL),
    (6, 'Carol', NULL, NULL),
    (1, 'CEO', NULL, NULL),
    (2, 'VP Engineering', NULL, NULL),
    (3, 'VP Sales', NULL, NULL);
UPDATE employees SET manager_id = 1, mentor_id = 2 WHERE id = 4;
UPDATE employees SET manager_id = 1, mentor_id = 3 WHERE id = 5;
UPDATE employees SET manager_id = 2, mentor_id = 1 WHERE id = 6;

-- Composite-PK self-referencing FK (tree_nodes).
-- Children reference parents via (parent_group_id, parent_position) -> (group_id, position).
-- Physical row order puts children before parents.
INSERT INTO tree_nodes (group_id, position, label, parent_group_id, parent_position) VALUES
    (1, 2, 'Child A', NULL, NULL),
    (1, 3, 'Child B', NULL, NULL),
    (2, 1, 'Leaf X', NULL, NULL),
    (2, 2, 'Leaf Y', NULL, NULL),
    (1, 1, 'Root', NULL, NULL),
    (2, 3, 'Branch', NULL, NULL);
UPDATE tree_nodes SET parent_group_id = 1, parent_position = 1 WHERE group_id = 1 AND position = 2;
UPDATE tree_nodes SET parent_group_id = 1, parent_position = 1 WHERE group_id = 1 AND position = 3;
UPDATE tree_nodes SET parent_group_id = 2, parent_position = 3 WHERE group_id = 2 AND position = 1;
UPDATE tree_nodes SET parent_group_id = 2, parent_position = 3 WHERE group_id = 2 AND position = 2;

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

-- category_id FK to categories enables testing that subsetting propagates through
-- a self-referencing table to downstream tables.
--
-- Edge case: Poetry Anthology (order_id=3/user_id=2, category_id=9/Fiction).
-- When subsetting to users 1-2, this item's order qualifies but its category doesn't
-- (Fiction is excluded per the categories edge case above).
INSERT INTO order_items (order_id, product_name, quantity, category_id) VALUES
    (1, 'iPhone 15 Pro', 1, 4),
    (1, 'Phone Case', 2, 4),
    (2, 'Wireless Earbuds', 1, 1),
    (3, 'Cotton T-Shirt', 3, 7),
    (4, 'MacBook Pro 14 inch', 1, 5),
    (4, 'USB-C Cable', 2, 1),
    (4, 'Laptop Sleeve', 1, 5),
    (5, 'Programming Book', 1, 10),
    (6, 'Smart Watch', 1, 1),
    (6, 'Watch Band', 1, 1),
    (7, 'Running Shoes', 1, 7),
    (8, 'Summer Dress', 2, 8),
    (9, 'Bluetooth Speaker', 1, 1),
    (9, 'HDMI Cable', 1, 1),
    (10, 'Fitness Tracker', 1, 1),
    (11, 'Novel - Fiction', 2, 9),
    (12, 'Gaming Laptop', 1, 5),
    (3, 'Poetry Anthology', 1, 9);

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
