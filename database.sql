-- 1. Create a table for Estimators (handling name variations)
CREATE TABLE estimators (
    id SERIAL PRIMARY KEY,
    primary_name VARCHAR(100) UNIQUE NOT NULL, -- The "Clean" name (e.g., "Scott Hutchings")
    active BOOLEAN DEFAULT TRUE
);

-- 2. Create the Sales Data table
CREATE TABLE monthly_sales (x
    id SERIAL PRIMARY KEY,
    estimator_id INTEGER REFERENCES estimators(id),
    report_date DATE NOT NULL, -- Converted from "19-06" to "2019-06-01"
    amt_bid NUMERIC(15, 2),
    qty_bid INTEGER,
    amt_booked NUMERIC(15, 2),
    qty_booked INTEGER,
    UNIQUE(estimator_id, report_date) -- Prevents uploading the same month twice
);