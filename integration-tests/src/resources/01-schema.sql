-- Schema for simple-anonymizer integration tests

CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(200)
);

CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    total DECIMAL(10,2),
    status VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(id),
    product_name VARCHAR(200),
    quantity INTEGER
);

CREATE TABLE IF NOT EXISTS categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    parent_id INTEGER REFERENCES categories(id)
);

CREATE TABLE IF NOT EXISTS employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    manager_id INTEGER REFERENCES employees(id),
    mentor_id INTEGER REFERENCES employees(id)
);

CREATE TABLE IF NOT EXISTS tree_nodes (
    group_id INTEGER NOT NULL,
    position INTEGER NOT NULL,
    label VARCHAR(100),
    parent_group_id INTEGER,
    parent_position INTEGER,
    PRIMARY KEY (group_id, position),
    FOREIGN KEY (parent_group_id, parent_position) REFERENCES tree_nodes(group_id, position)
);

CREATE TABLE IF NOT EXISTS profiles (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    phones JSONB,
    settings JSONB
);
