# --- global.R ---
library(shiny)
library(shinydashboard)
library(tidyverse)
library(lubridate)
library(plotly)
library(DBI)
library(RPostgres)
library(scales)
library(DT)
library(shinyWidgets)
library(cluster)
library(rmarkdown)
library(pool) # NEW: For connection pooling

# 1. ESTABLISH DATABASE CONNECTION POOL
# This creates a stable pool of connections that all users share efficiently.
db_pool <- dbPool(
  drv = RPostgres::Postgres(),
  dbname   = Sys.getenv("DB_NAME", "sales_db"),
  host     = Sys.getenv("DB_HOST", "localhost"),
  port     = as.numeric(Sys.getenv("DB_PORT", "5433")),
  user     = Sys.getenv("DB_USER", Sys.info()[["user"]]),
  password = Sys.getenv("DB_PASS", "")
)

# Ensure the pool closes safely when the app shuts down
onStop(function() {
  poolClose(db_pool)
})

# 2. GLOBAL MATH FUNCTIONS
# Moving the SPC math here keeps the server file clean and readable.
spc_limits <- function(df, metric_col, is_quarterly, by_person = FALSE, bl_start, bl_end) {
  grp_date <- function(d) if (is_quarterly) floor_date(d, "quarter") else floor_date(d, "month")
  
  if (by_person) {
    baseline_stats <- df %>%
      filter(report_date >= bl_start, report_date <= bl_end) %>%
      mutate(date_group = grp_date(report_date)) %>%
      group_by(primary_name, date_group) %>%
      summarise(Val = sum(.data[[metric_col]], na.rm = TRUE), .groups = "drop") %>%
      group_by(primary_name) %>%
      summarise(BL_Mean = mean(Val, na.rm = TRUE),
                BL_SD   = replace_na(sd(Val, na.rm = TRUE), 0),
                .groups = "drop")
    
    trend <- df %>%
      mutate(date_group = grp_date(report_date)) %>%
      group_by(primary_name, date_group) %>%
      summarise(Val = sum(.data[[metric_col]], na.rm = TRUE), .groups = "drop")
    
    trend %>%
      left_join(baseline_stats, by = "primary_name") %>%
      mutate(UCL     = BL_Mean + 3 * BL_SD,
             LCL     = pmax(0, BL_Mean - 3 * BL_SD),
             Anomaly = ifelse(Val > UCL | Val < LCL, "Anomaly", "Normal"))
  } else {
    baseline_stats <- df %>%
      filter(report_date >= bl_start, report_date <= bl_end) %>%
      mutate(date_group = grp_date(report_date)) %>%
      group_by(date_group) %>%
      summarise(Val = sum(.data[[metric_col]], na.rm = TRUE), .groups = "drop") %>%
      summarise(BL_Mean = mean(Val, na.rm = TRUE),
                BL_SD   = replace_na(sd(Val, na.rm = TRUE), 0))
    
    bl_mean <- baseline_stats$BL_Mean
    bl_sd   <- baseline_stats$BL_SD
    
    df %>%
      mutate(date_group = grp_date(report_date)) %>%
      group_by(date_group) %>%
      summarise(Val = sum(.data[[metric_col]], na.rm = TRUE), .groups = "drop") %>%
      mutate(BL_Mean = bl_mean,
             UCL     = bl_mean + 3 * bl_sd,
             LCL     = pmax(0, bl_mean - 3 * bl_sd),
             Anomaly = ifelse(Val > UCL | Val < LCL, "Anomaly", "Normal"))
  }
}

