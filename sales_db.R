library(DBI)
library(RPostgres)

# 1. Connect to the system database to create your App DB

sys_con <- dbConnect(RPostgres::Postgres(), 
                     dbname = "postgres", 
                     host = "localhost", 
                     port = 5433,              # <--- Your new port
                     user = Sys.info()[["user"]], 
                     password = "")

# 2. Create the 'sales_db' database
tryCatch({
  dbExecute(sys_con, "CREATE DATABASE sales_db;")
  print("Database 'sales_db' created successfully.")
}, error = function(e) {
  print("Database 'sales_db' likely already exists. Continuing...")
})

dbDisconnect(sys_con)

# 3. Connect to the new 'sales_db' to build tables
con <- dbConnect(RPostgres::Postgres(), 
                 dbname = "sales_db", 
                 host = "localhost", 
                 port = 5433,              
                 user = Sys.info()[["user"]], 
                 password = "")

# 4. Create Tables (Embedded SQL to ensure it works)

dbExecute(con, "
  CREATE TABLE IF NOT EXISTS estimators (
    id SERIAL PRIMARY KEY,
    primary_name VARCHAR(100) UNIQUE NOT NULL,
    active BOOLEAN DEFAULT TRUE
  );
")

# Create Sales Data Table
dbExecute(con, "
  CREATE TABLE IF NOT EXISTS monthly_sales (
    id SERIAL PRIMARY KEY,
    estimator_id INTEGER REFERENCES estimators(id),
    report_date DATE NOT NULL,
    amt_bid NUMERIC(15, 2),
    qty_bid INTEGER,
    amt_booked NUMERIC(15, 2),
    qty_booked INTEGER,
    UNIQUE(estimator_id, report_date)
  );
")

print("Tables created successfully on Port 5433!")
dbDisconnect(con)