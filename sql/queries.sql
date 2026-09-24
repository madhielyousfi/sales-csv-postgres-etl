-- Overall revenue
SELECT COALESCE(SUM(total_amount), 0) AS total_revenue FROM sales;

-- Revenue by product
SELECT product, SUM(total_amount) AS revenue
FROM sales GROUP BY product ORDER BY revenue DESC;

-- Daily sales
SELECT sale_date, COUNT(*) AS sales_count, SUM(total_amount) AS revenue
FROM sales GROUP BY sale_date ORDER BY sale_date;
