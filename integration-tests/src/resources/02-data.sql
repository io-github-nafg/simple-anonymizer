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

-- Children reference parents with higher IDs to exercise self-referencing FK handling.
-- Physical row order puts children before parents to trigger FK violations across batches.
INSERT INTO categories (id, name, parent_id) VALUES
    (4, 'Smartphones', NULL),
    (5, 'Laptops', NULL),
    (6, 'Tablets', NULL),
    (7, 'Mens Clothing', NULL),
    (8, 'Womens Clothing', NULL),
    (9, 'Fiction', NULL),
    (10, 'Non-Fiction', NULL),
    (1, 'Electronics', NULL),
    (2, 'Clothing', NULL),
    (3, 'Books', NULL);
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
