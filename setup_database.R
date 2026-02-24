library(DBI)
library(RPostgres)
library(tidyverse)
library(lubridate)
library(readr)

# --- CONFIGURATION ---
DB_PORT <- 5433                 # The port you selected
MAC_USER <- Sys.info()[["user"]] # Automatically gets your Mac username
CSV_FILE <- "Bid_SalesAnalysisByEstimatorByMonth_121125.csv"

# --- STEP 1: CREATE THE DATABASE ---
print("--- Step 1: Creating Database... ---")

# We must connect to the default 'postgres' database first to create a new one
con_sys <- dbConnect(RPostgres::Postgres(), 
                     dbname = "postgres", 
                     host = "localhost", 
                     port = DB_PORT, 
                     user = MAC_USER, 
                     password = "")

# Try to create the database (fails gracefully if it already exists)
tryCatch({
  dbExecute(con_sys, "CREATE DATABASE sales_db")
  print("SUCCESS: Database 'sales_db' created.")
}, error = function(e) {
  print("NOTE: Database 'sales_db' already exists. Moving on...")
})

dbDisconnect(con_sys)

# --- STEP 2: CREATE TABLES ---
print("--- Step 2: Creating Tables... ---")

# Now connect to the actual 'sales_db'
con <- dbConnect(RPostgres::Postgres(), 
                 dbname = "sales_db", 
                 host = "localhost", 
                 port = DB_PORT, 
                 user = MAC_USER, 
                 password = "")

# Create 'estimators' table
dbExecute(con, "
  CREATE TABLE IF NOT EXISTS estimators (
    id SERIAL PRIMARY KEY,
    primary_name VARCHAR(100) UNIQUE NOT NULL,
    active BOOLEAN DEFAULT TRUE
  );
")

# Create 'monthly_sales' table
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
print("SUCCESS: Tables created.")

# --- STEP 3: LOAD CSV DATA ---
print("--- Step 3: Loading Data... ---")

if (file.exists(CSV_FILE)) {
  # Read and Clean Data
  raw_data <- read_csv(CSV_FILE, show_col_types = FALSE)
  
  clean_data <- raw_data %>%
    mutate(report_date = ym(MoYear)) %>% 
    mutate(across(c(AmtBid, AmtBooked), ~ as.numeric(gsub("[\\$,]", "", .)))) %>%
    mutate(clean_name = case_when(
      Estimator == "Scott W. Hutchings" ~ "Scott Hutchings",
      Estimator == "SW" ~ "Scott Hutchings", 
      Estimator == "SH" ~ "Scott Hutchings",
      TRUE ~ Estimator
    ))
  
  # Update Estimators Table (Insert new names)
  unique_estimators <- unique(clean_data$clean_name)
  for(name in unique_estimators){
    exists <- dbGetQuery(con, "SELECT id FROM estimators WHERE primary_name = $1", params = list(name))
    if(nrow(exists) == 0){
      dbExecute(con, "INSERT INTO estimators (primary_name) VALUES ($1)", params = list(name))
    }
  }
  
  # Prepare Sales Data for Upload
  est_map <- dbGetQuery(con, "SELECT id, primary_name FROM estimators")
  
  upload_ready <- clean_data %>%
    left_join(est_map, by = c("clean_name" = "primary_name")) %>%
    select(estimator_id = id, report_date, amt_bid = AmtBid, qty_bid = QtyBid, 
           amt_booked = AmtBooked, qty_booked = QtyBooked)
  
  # Write to DB
  tryCatch({
    # We use 'on conflict do nothing' logic via a safe append approach
    # Since RPostgres dbWriteTable is simple, we just try/catch duplicates
    dbWriteTable(con, "monthly_sales", upload_ready, append = TRUE, row.names = FALSE)
    print("SUCCESS: Data uploaded to database.")
  }, error = function(e) {
    print("NOTE: Data might already exist. (Ignoring duplicate errors)")
  })
  
} else {
  print("ERROR: CSV file not found. Make sure the file name is correct.")
}

dbDisconnect(con)
print("--- SETUP COMPLETE ---")
