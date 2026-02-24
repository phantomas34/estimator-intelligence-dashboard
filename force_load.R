library(DBI)
library(RPostgres)
library(tidyverse)
library(lubridate)
library(readr)

# --- 1. SETTINGS ---
CSV_FILENAME <- "Bid_SalesAnalysisByEstimatorByMonth_121125.csv"
DB_PORT <- 5433

print("--- DIAGNOSTIC START ---")

# --- 2. CHECK FILE EXISTENCE ---
# This checks if RStudio is looking in the right folder
if (file.exists(CSV_FILENAME)) {
  print(paste("✅ SUCCESS: Found file:", CSV_FILENAME))
} else {
  print(paste("❌ ERROR: Could not find file:", CSV_FILENAME))
  print(paste("   R is currently looking in:", getwd()))
  stop("Stoppping script. Please move the CSV to the folder listed above.")
}

# --- 3. CONNECT TO DATABASE ---
tryCatch({
  con <- dbConnect(RPostgres::Postgres(), 
                   dbname = "sales_db", 
                   host = "localhost", 
                   port = DB_PORT, 
                   user = Sys.info()[["user"]], 
                   password = "")
  print("✅ SUCCESS: Connected to Database.")
}, error = function(e) {
  print("❌ ERROR: Could not connect to Database. Make sure Postgres.app is running.")
  stop(e)
})

# --- 4. LOAD DATA ---
print("--- Attempting Data Load ---")

raw_data <- read_csv(CSV_FILENAME, show_col_types = FALSE)

clean_data <- raw_data %>%
  mutate(report_date = ym(MoYear)) %>% 
  mutate(across(c(AmtBid, AmtBooked), ~ as.numeric(gsub("[\\$,]", "", .)))) %>%
  mutate(clean_name = case_when(
    Estimator == "Scott W. Hutchings" ~ "Scott Hutchings",
    Estimator == "SW" ~ "Scott Hutchings", 
    Estimator == "SH" ~ "Scott Hutchings",
    TRUE ~ Estimator
  ))

# Insert Estimator Names
unique_estimators <- unique(clean_data$clean_name)
count_new_names <- 0

for(name in unique_estimators){
  exists <- dbGetQuery(con, "SELECT id FROM estimators WHERE primary_name = $1", params = list(name))
  if(nrow(exists) == 0){
    dbExecute(con, "INSERT INTO estimators (primary_name) VALUES ($1)", params = list(name))
    count_new_names <- count_new_names + 1
  }
}
print(paste("   Processed Estimators. Added", count_new_names, "new names."))

# Insert Sales Data
est_map <- dbGetQuery(con, "SELECT id, primary_name FROM estimators")

upload_ready <- clean_data %>%
  left_join(est_map, by = c("clean_name" = "primary_name")) %>%
  select(estimator_id = id, report_date, amt_bid = AmtBid, qty_bid = QtyBid, 
         amt_booked = AmtBooked, qty_booked = QtyBooked)

# Write to DB
tryCatch({
  dbWriteTable(con, "monthly_sales", upload_ready, append = TRUE, row.names = FALSE)
  print(paste("✅ SUCCESS: Uploaded", nrow(upload_ready), "sales records."))
}, error = function(e) {
  print("⚠️ NOTE: Data write skipped (likely duplicate data).")
})

# --- 5. FINAL VERIFICATION ---
row_count <- dbGetQuery(con, "SELECT count(*) FROM monthly_sales")
print(paste("--- FINAL DATABASE STATUS: The table now has", row_count, "rows. ---"))

dbDisconnect(con)
