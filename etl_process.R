library(tidyverse)
library(lubridate)
library(DBI)
library(RPostgres)

# 1. Connect to DB
con <- dbConnect(RPostgres::Postgres(), dbname = "sales_db", 
                 host = "localhost", port = 5433, 
                 user = Sys.info()[["user"]], password = "")

# 2. Read and Clean CSV
raw_data <- read_csv("Bid_SalesAnalysisByEstimatorByMonth_121125.csv", show_col_types = FALSE)

clean_data <- raw_data %>%
  mutate(report_date = ym(MoYear)) %>% 
  mutate(across(c(AmtBid, AmtBooked), ~ as.numeric(gsub("[\\$,]", "", .)))) %>%
  mutate(clean_name = case_when(
    Estimator == "Scott W. Hutchings" ~ "Scott Hutchings",
    Estimator == "SW" ~ "Scott Hutchings",
    Estimator == "SH" ~ "Scott Hutchings",
    TRUE ~ Estimator
  ))

# 3. Update Estimators Table
unique_estimators <- unique(clean_data$clean_name)

for(name in unique_estimators){
  # Check if exists, if not, insert
  exists <- dbGetQuery(con, "SELECT id FROM estimators WHERE primary_name = $1", params = list(name))
  if(nrow(exists) == 0){
    dbExecute(con, "INSERT INTO estimators (primary_name) VALUES ($1)", params = list(name))
  }
}

# 4. Upload Sales Data
est_map <- dbGetQuery(con, "SELECT id, primary_name FROM estimators")

upload_ready <- clean_data %>%
  left_join(est_map, by = c("clean_name" = "primary_name")) %>%
  select(estimator_id = id, report_date, amt_bid = AmtBid, qty_bid = QtyBid, 
         amt_booked = AmtBooked, qty_booked = QtyBooked)

# Write to DB (Append mode)
tryCatch({
  dbWriteTable(con, "monthly_sales", upload_ready, append = TRUE, row.names = FALSE)
  print("Data Successfully Uploaded to Port 5433!")
}, error = function(e) {
  print("Data likely already exists. (Duplicate key error expected if running twice).")
})

dbDisconnect(con)