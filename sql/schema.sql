CREATE TABLE IF NOT EXISTS sales (
    sale_id INTEGER PRIMARY KEY CHECK (sale_id > 0),
    sale_date DATE NOT NULL CHECK (sale_date BETWEEN DATE '0001-01-01' AND DATE '9999-12-31'),
    customer_name TEXT NOT NULL CHECK (customer_name ~ '[^[:space:]]'),
    product TEXT NOT NULL CHECK (product ~ '[^[:space:]]'),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0 AND unit_price <= 9999999999.99),
    total_amount NUMERIC(18,2) NOT NULL CHECK (
        total_amount >= 0 AND total_amount <= 9999999999999999.99
        AND total_amount = quantity * unit_price
    )
);
