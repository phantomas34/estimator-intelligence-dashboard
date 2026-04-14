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
library(openxlsx)       # Tier 1 Excel report
library(strucchange)    # ITS breakpoint detection
library(lmtest)         # ITS coefficient testing (coeftest, waldtest)

# --- DATABASE CONNECTION ---
connect_db <- function() {
  dbConnect(RPostgres::Postgres(),
            dbname   = Sys.getenv("DB_NAME", "sales_db"),
            host     = Sys.getenv("DB_HOST", "localhost"),
            port     = as.numeric(Sys.getenv("DB_PORT", "5433")),
            user     = Sys.getenv("DB_USER", Sys.info()[["user"]]),
            password = Sys.getenv("DB_PASS", ""))
}

# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------
ui <- dashboardPage(
  title = "Estimator Intelligence",
  skin  = "black",
  
  dashboardHeader(title = span("Estimator Intelligence",
                               style = "font-weight:bold;font-size:18px;")),
  
  dashboardSidebar(
    width = 260,
    sidebarMenu(
      menuItem("Executive Dashboard", tabName = "dashboard", icon = icon("chart-area")),
      menuItem("Intervention Analysis", tabName = "its",     icon = icon("flask")),
      menuItem("Data Management",     tabName = "admin",     icon = icon("database")),
      hr(),
      
      div(style = "padding:10px;",
          h5("Filter Controls",
             style = "color:#b8c7ce;text-transform:uppercase;font-size:12px;margin-bottom:10px;"),
          
          dateRangeInput("dateRange", "View Range",
                         start     = Sys.Date() %m-% months(24),
                         end       = Sys.Date(),
                         separator = " to "),
          
          # NEW: Baseline Period for SPC — users lock this to a stable historical
          # window; control limits are computed from here and projected forward.
          dateRangeInput("baselineRange", "SPC Baseline Period",
                         start     = Sys.Date() %m-% months(48),
                         end       = Sys.Date() %m-% months(25),
                         separator = " to "),
          
          radioGroupButtons("view_mode", "Breakdown Mode",
                            choices   = c("Aggregate", "Individual"),
                            selected  = "Aggregate",
                            status    = "success", size = "sm", justified = TRUE),
          br(),
          
          radioGroupButtons("time_granularity", "Time Scale",
                            choices   = c("Monthly", "Quarterly"),
                            selected  = "Monthly",
                            status    = "primary", size = "sm", justified = TRUE),
          br(),
          
          # NEW: Lag slider for win rate calculation
          sliderInput("win_lag", "Win Rate Lag (months)",
                      min = 0, max = 6, value = 3, step = 1,
                      ticks = FALSE),
          p(style = "font-size:10px;color:#b8c7ce;margin-top:-8px;",
            "Match bids to bookings N months later. 0 = same-month (old behavior)."),
          
          pickerInput("estimatorSelect", "Estimators:",
                      choices  = NULL, multiple = TRUE,
                      options  = list(
                        `actions-box`          = TRUE,
                        `live-search`          = TRUE,
                        `selected-text-format` = "count > 2",
                        `count-selected-text`  = "{0} Selected",
                        `none-selected-text`   = "All Estimators (Default)"
                      )),
          
          div(style = "margin-top:12px;display:flex;gap:6px;",
              actionButton("refresh", "Refresh",
                           icon = icon("sync"), style = "flex:1;font-size:12px;",
                           class = "btn-primary btn-sm btn-flat"),
              actionButton("reset_filters", "Reset",
                           icon = icon("undo"), style = "flex:1;font-size:12px;",
                           class = "btn-warning btn-sm btn-flat")
          )
      )
    )
  ),
  
  dashboardBody(
    tags$head(tags$style(HTML("
      .box.box-solid.box-primary>.box-header { background:#2c3e50; }
      .box.box-solid.box-primary { border:1px solid #2c3e50; }
      .content-wrapper,.right-side { background-color:#f4f6f9; }
      table.dataTable thead th { background-color:#f4f6f9; border-bottom:2px solid #ddd; }
      .plotly { margin-top:10px; }
      .insight-text { font-size:14px;line-height:1.5;color:#2c3e50; }
    "))),
    
    tabItems(
      
      # ── EXECUTIVE DASHBOARD ──────────────────────────────────────────────
      tabItem(tabName = "dashboard",
              
              # ROW 1: KPIs
              fluidRow(
                valueBoxOutput("kpi_total_bid",       width = 3),
                valueBoxOutput("kpi_total_booked",    width = 3),
                valueBoxOutput("kpi_win_rate_amt",    width = 3),
                valueBoxOutput("kpi_lag_win_rate",    width = 3)   # NEW: lag-adjusted KPI
              ),
              
              # ROW 2: CORRELATION INSIGHT
              fluidRow(
                box(title = "Data Science Insight: Pipeline Correlation",
                    status = "info", solidHeader = TRUE, width = 12,
                    uiOutput("correlation_insight"))
              ),
              
              # ROW 3: SPC CHARTS
              fluidRow(
                tabBox(title = textOutput("trend_box_title"),
                       width = 12, side = "right",
                       selected = "Control Chart: Revenue ($)",
                       tabPanel("Control Chart: Revenue ($)",
                                fluidRow(
                                  column(6,
                                         radioGroupButtons("trend_series", NULL,
                                                           choices  = c("Booked Only" = "booked", "Bid vs. Booked" = "both"),
                                                           selected = "booked",
                                                           status   = "default", size = "xs")
                                  ),
                                  # Trend line toggle — only meaningful in Individual mode
                                  column(6,
                                         materialSwitch("show_trendline",
                                                        label    = "Trend Lines (Individual)",
                                                        value    = FALSE,
                                                        status   = "primary",
                                                        right    = TRUE)
                                  )
                                ),
                                plotlyOutput("trend_dollars", height = "370px"),
                                p(class = "text-muted", style = "font-size:11px;",
                                  "SPC: Blue = Baseline Mean. Red dashed = UCL/LCL (±3 SD). Red dots = anomalies. Trend line shows linear trajectory per estimator (Individual mode only).")),
                       tabPanel("Control Chart: Quantity",
                                div(style = "padding:6px 0 0 4px;",
                                    materialSwitch("show_trendline_qty",
                                                   label  = "Trend Lines (Individual)",
                                                   value  = FALSE,
                                                   status = "primary",
                                                   right  = TRUE)
                                ),
                                plotlyOutput("trend_qty", height = "385px"),
                                p(class = "text-muted", style = "font-size:11px;",
                                  "Anomalies in jobs booked vs. the fixed Baseline Period mean. Trend line shows trajectory per estimator (Individual mode only.)"))
                )
              ),
              
              # ROW 4: ANNUAL & YOY
              fluidRow(
                box(title = "Annual Performance",
                    status = "primary", solidHeader = TRUE, width = 6,
                    # STAKEHOLDER: Annual Performance can be grouped by year/quarter/month
                    fluidRow(
                      column(12,
                             radioGroupButtons("annual_granularity", NULL,
                                               choices  = c("By Year" = "year", "By Quarter" = "quarter", "By Month" = "month"),
                                               selected = "year",
                                               status   = "default", size = "xs", justified = TRUE)
                      )
                    ),
                    plotlyOutput("annual_bar_chart", height = "370px")),
                
                tabBox(title = "Growth & Breakdown", width = 6, side = "right",
                       selected = "YoY Growth (%)",
                       tabPanel("YoY Growth (%)",
                                fluidRow(
                                  column(4, p(class="text-muted", style="font-size:11px;margin-top:10px;",
                                              "Aggregate: Red/Green | Individual: Colored by Person")),
                                  column(4, pickerInput("yoy_metric", NULL,
                                                        choices  = c("Booked Revenue ($)" = "amt_booked",
                                                                     "Bid Volume ($)"     = "amt_bid",
                                                                     "Jobs Booked (Qty)"  = "qty_booked",
                                                                     "Jobs Bid (Qty)"     = "qty_bid"),
                                                        selected = "amt_booked", width = "100%",
                                                        options  = list(style = "btn-default btn-sm"))),
                                  # STAKEHOLDER: grouped or ungrouped option for YoY
                                  column(4, radioGroupButtons("yoy_position", NULL,
                                                              choices  = c("Grouped" = "dodge", "Stacked" = "stack"),
                                                              selected = "dodge",
                                                              status   = "default", size = "xs", justified = TRUE))
                                ),
                                plotlyOutput("yoy_chart", height = "330px")),
                       tabPanel("Quarterly Deep Dive",
                                fluidRow(
                                  column(7, p(class="text-muted", style="font-size:11px;margin-top:10px;",
                                              "Revenue Breakdown by Quarter for Selected Year.")),
                                  column(5, selectInput("quarter_year_select", NULL,
                                                        choices = NULL, width = "100%"))
                                ),
                                plotlyOutput("quarterly_breakdown_chart", height = "350px"))
                )
              ),
              
              # ROW 5: NEW ANALYTICS TABS
              fluidRow(
                tabBox(title = "Advanced Analytics", width = 12, side = "right",
                       selected = "Bid Funnel",
                       
                       # NEW: Bid Funnel Chart
                       tabPanel("Bid Funnel",
                                plotlyOutput("bid_funnel_chart", height = "420px"),
                                p(class = "text-muted", style = "font-size:11px;",
                                  "Grouped bars show Total Bid $ vs. Total Booked $ per estimator. Win % annotated on each booked bar. Sorted by Total Bid descending.")),
                       
                       # NEW: Seasonality Heatmap
                       tabPanel("Seasonality Heatmap",
                                plotlyOutput("seasonality_heatmap", height = "420px"),
                                p(class = "text-muted", style = "font-size:11px;",
                                  "Booked revenue by calendar month and year. Darker = higher revenue. Reveals consistent seasonal peaks and troughs across the full history."))
                )
              ),
              
              # ROW 6: K-MEANS CLUSTERING
              fluidRow(
                box(title = "K-Means Behavioral Segmentation (3-Feature Model)",
                    status = "primary", solidHeader = TRUE, width = 12,
                    plotlyOutput("scatter_matrix", height = "400px"),
                    p(class = "text-muted", style = "font-size:11px;",
                      "Clusters on: Pipeline Effort (Total Bid $), Efficiency (Lag-Adjusted Win Rate %), Selectivity (Avg Job Size $). Point size = Total Booked. Hover for details."))
              ),
              
              # ROW 7: LEADERBOARD
              fluidRow(
                box(title = "Estimator Leaderboard — Click any column to sort",
                    status = "warning", solidHeader = TRUE, width = 12,
                    dataTableOutput("leaderboard_table"))
              )
      ),
      
      # ── INTERVENTION ANALYSIS (ITS) ──────────────────────────────────────
      tabItem(tabName = "its",
              
              # ROW 1: Controls
              fluidRow(
                box(title = "Intervention Settings", status = "primary",
                    solidHeader = TRUE, width = 4,
                    
                    pickerInput("its_estimator", "Estimator",
                                choices  = NULL, multiple = FALSE,
                                options  = list(`live-search` = TRUE)),
                    
                    pickerInput("its_metric", "Outcome Metric",
                                choices = c(
                                  "Booked Revenue ($)"  = "amt_booked",
                                  "Bid Volume ($)"      = "amt_bid",
                                  "Jobs Booked (Qty)"   = "qty_booked",
                                  "Win Rate (same-mo.)" = "win_rate"
                                ), selected = "amt_booked"),
                    
                    hr(),
                    
                    # Option A: manager-specified date
                    dateInput("its_manual_date", "Option A — Specify Intervention Date",
                              value = NULL, format = "M yyyy",
                              min   = "2010-01-01"),
                    p(class = "text-muted", style = "font-size:11px;",
                      "The date a strategy change, new hire, process shift, or other intervention occurred."),
                    
                    hr(),
                    
                    # Option B: auto-detect toggle
                    materialSwitch("its_autodetect",
                                   label  = "Option B — Auto-Detect Breakpoints",
                                   value  = TRUE,
                                   status = "primary", right = TRUE),
                    p(class = "text-muted", style = "font-size:11px;",
                      "Uses the strucchange algorithm to scan for the single most statistically significant structural break in the series."),
                    
                    br(),
                    actionButton("its_run", "Run Analysis",
                                 icon  = icon("play"),
                                 class = "btn-success btn-block",
                                 width = "100%")
                ),
                
                # Results summary box
                box(title = "Intervention Effect — Statistical Summary",
                    status = "primary", solidHeader = TRUE, width = 8,
                    uiOutput("its_summary_ui"),
                    p(class = "text-muted", style = "font-size:11px; margin-top:8px;",
                      "Segmented regression (OLS) with pre/post intervention terms. Level change = immediate shift at intervention date. Slope change = difference in monthly trajectory before vs. after.")
                )
              ),
              
              # ROW 2: Main ITS chart
              fluidRow(
                box(title = "Interrupted Time Series — Visual",
                    status = "primary", solidHeader = TRUE, width = 12,
                    plotlyOutput("its_chart", height = "440px"),
                    p(class = "text-muted", style = "font-size:11px;",
                      "Grey points = actual monthly values. Blue line = pre-intervention fitted trend. Green line = post-intervention fitted trend. Vertical dashed line = intervention point. Shaded band = 95% confidence interval.")
                )
              ),
              
              # ROW 3: Auto-detected breakpoints table
              fluidRow(
                box(title = "Auto-Detected Structural Breaks",
                    status = "warning", solidHeader = TRUE, width = 6,
                    dataTableOutput("its_breakpoints_table"),
                    p(class = "text-muted", style = "font-size:11px; margin-top:6px;",
                      "All significant breakpoints detected by the strucchange algorithm, ranked by F-statistic. Click a row to set it as the active intervention date.")),
                
                box(title = "Coefficient Detail",
                    status = "info", solidHeader = TRUE, width = 6,
                    dataTableOutput("its_coef_table"),
                    p(class = "text-muted", style = "font-size:11px; margin-top:6px;",
                      "Full regression output. p < 0.05 indicates a statistically significant effect."))
              )
      ),
      
      # ── DATA MANAGEMENT ─────────────────────────────────────────────────
      tabItem(tabName = "admin",
              fluidRow(
                box(title = "Upload Data", status = "danger", solidHeader = TRUE, width = 5,
                    fileInput("file1", "Upload CSV", accept = ".csv"),
                    actionButton("process_upload", "Process & Import",
                                 class = "btn-danger", width = "100%")),
                box(title = "System Status", status = "info", solidHeader = TRUE, width = 4,
                    verbatimTextOutput("db_status_text"),
                    dataTableOutput("preview_table")),
                box(title = "Export Report", status = "primary", solidHeader = TRUE, width = 3,
                    p(class = "text-muted",
                      "Download a formatted Excel workbook of the current filtered view — KPIs, leaderboard, monthly detail, and seasonality."),
                    downloadButton("download_report", "Download Excel Report",
                                   icon  = icon("file-excel"),
                                   style = "width:100%;font-weight:600;"))
              ),
              fluidRow(
                box(title = "Date Verification — Raw vs. Parsed",
                    status = "warning", solidHeader = TRUE, width = 12,
                    p(class = "text-muted", style = "font-size:12px;",
                      "Confirms MoYear values are parsed correctly. Format: YY-MM (e.g. '19-06' → June 2019)."),
                    dataTableOutput("date_verification_table"))
              )
      )
    )
  )
)

# ---------------------------------------------------------------------------
# SERVER
# ---------------------------------------------------------------------------
server <- function(input, output, session) {
  
  data_trigger <- reactiveVal(0)
  
  # ── DATA FETCHING ─────────────────────────────────────────────────────────
  get_data <- reactive({
    input$refresh
    data_trigger()
    con <- tryCatch(connect_db(), error = function(e) NULL)
    if (is.null(con)) return(data.frame())
    if (!dbExistsTable(con, "monthly_sales")) { dbDisconnect(con); return(data.frame()) }
    df <- dbGetQuery(con, "
      SELECT e.primary_name, s.*, EXTRACT(YEAR FROM s.report_date) AS year_num
      FROM monthly_sales s
      JOIN estimators e ON s.estimator_id = e.id
    ")
    dbDisconnect(con)
    df
  })
  
  # ── UPLOAD ────────────────────────────────────────────────────────────────
  observeEvent(input$process_upload, {
    req(input$file1)
    prog <- shiny::Progress$new(message = "Processing...", value = 0.2)
    on.exit(prog$close())
    tryCatch({
      raw  <- read_csv(input$file1$datapath, show_col_types = FALSE)
      clean <- raw %>%
        mutate(report_date = ym(MoYear),
               across(c(AmtBid, AmtBooked), ~ as.numeric(gsub("[\\$,]", "", .))),
               clean_name = case_when(
                 Estimator == "Scott W. Hutchings" ~ "Scott Hutchings",
                 Estimator %in% c("SW", "SH")      ~ "Scott Hutchings",
                 TRUE                               ~ Estimator))
      
      con <- connect_db()
      dbExecute(con, "CREATE TABLE IF NOT EXISTS estimators (
        id SERIAL PRIMARY KEY, primary_name VARCHAR(100) UNIQUE NOT NULL,
        active BOOLEAN DEFAULT TRUE);")
      dbExecute(con, "CREATE TABLE IF NOT EXISTS monthly_sales (
        id SERIAL PRIMARY KEY, estimator_id INTEGER REFERENCES estimators(id),
        report_date DATE NOT NULL, amt_bid NUMERIC(15,2), qty_bid INTEGER,
        amt_booked NUMERIC(15,2), qty_booked INTEGER,
        UNIQUE(estimator_id, report_date));")
      
      for (nm in unique(clean$clean_name))
        tryCatch(dbExecute(con,
                           "INSERT INTO estimators (primary_name) VALUES ($1) ON CONFLICT DO NOTHING",
                           params = list(nm)), error = function(e) NULL)
      
      est_map <- dbGetQuery(con, "SELECT id, primary_name FROM estimators")
      upload  <- clean %>%
        left_join(est_map, by = c("clean_name" = "primary_name")) %>%
        select(estimator_id = id, report_date,
               amt_bid = AmtBid, qty_bid = QtyBid,
               amt_booked = AmtBooked, qty_booked = QtyBooked)
      
      dbWriteTable(con, "staging_sales", upload, overwrite = TRUE, temporary = TRUE)
      n <- dbExecute(con, "
        INSERT INTO monthly_sales
          (estimator_id,report_date,amt_bid,qty_bid,amt_booked,qty_booked)
        SELECT estimator_id,report_date,amt_bid,qty_bid,amt_booked,qty_booked
        FROM staging_sales
        ON CONFLICT (estimator_id, report_date) DO NOTHING;")
      dbExecute(con, "DROP TABLE IF EXISTS staging_sales")
      dbDisconnect(con)
      
      showNotification(paste("Success! Added/Merged", n, "records."), type = "message")
      data_trigger(data_trigger() + 1)
    }, error = function(e) showNotification(paste("Error:", e$message), type = "error"))
  })
  
  # ── FILTERING ─────────────────────────────────────────────────────────────
  filtered_data <- reactive({
    req(input$dateRange)
    df <- get_data()
    if (nrow(df) == 0) return(df[0, ])
    df <- df %>% filter(report_date >= input$dateRange[1],
                        report_date <= input$dateRange[2])
    sel <- input$estimatorSelect
    if (!is.null(sel) && length(sel) > 0) df <- df %>% filter(primary_name %in% sel)
    df
  })
  
  # ── LAG-ADJUSTED WIN RATE HELPER ─────────────────────────────────────────
  # For each bid-month, sum bookings in the following `lag` months.
  #
  # TRAILING EDGE FIX: bid months where fewer than `lag` full months of
  # future data exist in the DB are excluded entirely. Without this cap,
  # a bid placed in e.g. October 2025 with lag=6 looks forward to April 2026
  # but the DB only goes to December 2025 — producing an artificially low
  # (or in sparse datasets, absurdly high) ratio. We only score bid months
  # where report_date + lag months <= max(report_date) in the full dataset.
  lag_win_rate <- reactive({
    df  <- filtered_data()
    lag <- input$win_lag
    if (nrow(df) == 0 || lag == 0) return(NULL)
    
    # Use the FULL dataset's max date as the ceiling, not just the view window,
    # so the trailing edge is defined by actual data availability in the DB.
    all_data <- get_data()
    max_data_date <- max(all_data$report_date, na.rm = TRUE)
    
    # The latest bid month we can fairly score is max_data_date minus lag months
    cutoff_date <- max_data_date %m-% months(lag)
    
    bookings_lookup <- df %>%
      select(primary_name, report_date, amt_booked, qty_booked) %>%
      rename(book_date = report_date)
    
    # Only bid months with a full lag window of future data
    bid_months <- df %>%
      select(primary_name, report_date, amt_bid, qty_bid) %>%
      distinct() %>%
      filter(report_date <= cutoff_date)   # ← trailing edge cap
    
    if (nrow(bid_months) == 0) return(NULL)
    
    result <- bid_months %>%
      rowwise() %>%
      mutate(
        lag_booked_amt = {
          window <- bookings_lookup %>%
            filter(primary_name == .data$primary_name,
                   book_date >  report_date,
                   book_date <= report_date %m+% months(lag))
          sum(window$amt_booked, na.rm = TRUE)
        },
        lag_booked_qty = {
          window <- bookings_lookup %>%
            filter(primary_name == .data$primary_name,
                   book_date >  report_date,
                   book_date <= report_date %m+% months(lag))
          sum(window$qty_booked, na.rm = TRUE)
        }
      ) %>%
      ungroup()
    
    result
  })
  
  # ── UPDATERS ──────────────────────────────────────────────────────────────
  observe({
    df <- get_data()
    if (nrow(df) == 0) return()
    
    est_list <- sort(unique(df$primary_name))
    updatePickerInput(session, "estimatorSelect",
                      choices = est_list, selected = isolate(input$estimatorSelect))
    
    yrs     <- sort(unique(df$year_num), decreasing = TRUE)
    cur_yr  <- isolate(input$quarter_year_select)
    updateSelectInput(session, "quarter_year_select",
                      choices = yrs,
                      selected = if (!is.null(cur_yr) && cur_yr %in% yrs) cur_yr else yrs[1])
    
    max_dt <- max(df$report_date, na.rm = TRUE)
    updateDateRangeInput(session, "dateRange",
                         start = max_dt %m-% months(24), end = max_dt)
    updateDateRangeInput(session, "baselineRange",
                         start = max_dt %m-% months(60),
                         end   = max_dt %m-% months(25))
  })
  
  observeEvent(input$reset_filters, {
    df <- get_data()
    if (nrow(df) == 0) return()
    max_dt <- max(df$report_date, na.rm = TRUE)
    updateDateRangeInput(session, "dateRange",
                         start = max_dt %m-% months(24), end = max_dt)
    updateDateRangeInput(session, "baselineRange",
                         start = max_dt %m-% months(60),
                         end   = max_dt %m-% months(25))
    updatePickerInput(session, "estimatorSelect", selected = character(0))
    updateRadioGroupButtons(session, "view_mode",           selected = "Aggregate")
    updateRadioGroupButtons(session, "time_granularity",    selected = "Monthly")
    updateRadioGroupButtons(session, "annual_granularity",  selected = "year")
    updateRadioGroupButtons(session, "yoy_position",        selected = "dodge")
    updateRadioGroupButtons(session, "trend_series",        selected = "booked")
    updateMaterialSwitch(session, "show_trendline",     value = FALSE)
    updateMaterialSwitch(session, "show_trendline_qty", value = FALSE)
    updateSliderInput(session, "win_lag", value = 3)
  })
  
  # STAKEHOLDER: Date Verification table — shows distinct MoYear → parsed date mappings
  output$date_verification_table <- renderDataTable({
    d <- get_data()
    if (nrow(d) == 0) return(NULL)
    
    # Reconstruct what ym() would have produced from the stored dates
    # by showing a sample of (estimator, report_date) pairs with formatted labels
    d %>%
      arrange(report_date) %>%
      mutate(
        `Raw Format (MoYear)`   = format(report_date, "%y-%m"),
        `Parsed Date`           = format(report_date, "%B %Y"),
        `Full Date Stored`      = as.character(report_date)
      ) %>%
      select(Estimator = primary_name,
             `Raw Format (MoYear)`,
             `Parsed Date`,
             `Full Date Stored`) %>%
      distinct() %>%
      head(50)
  }, options = list(pageLength = 10, scrollX = TRUE, dom = "ftp"))
  
  output$trend_box_title <- renderText(
    paste("Statistical Process Control —", input$time_granularity, "View")
  )
  
  # ── KPIs ──────────────────────────────────────────────────────────────────
  output$kpi_total_bid <- renderValueBox(
    valueBox(dollar(sum(filtered_data()$amt_bid, na.rm = TRUE), accuracy = 1),
             "Total Bid Volume", icon = icon("file-invoice-dollar"), color = "blue")
  )
  output$kpi_total_booked <- renderValueBox(
    valueBox(dollar(sum(filtered_data()$amt_booked, na.rm = TRUE), accuracy = 1),
             "Total Booked Revenue", icon = icon("money-bill-wave"), color = "green")
  )
  
  # Same-month win rate (legacy, kept for reference)
  output$kpi_win_rate_amt <- renderValueBox({
    d <- filtered_data()
    r <- sum(d$amt_booked, na.rm = TRUE) / sum(d$amt_bid, na.rm = TRUE)
    valueBox(percent(r, 0.1),
             HTML("Same-Month Win Rate<br><small style='font-size:10px;opacity:0.75;'>Bids & bookings matched to same month</small>"),
             icon = icon("chart-line"), color = "yellow")
  })
  
  # NEW: Lag-adjusted win rate KPI
  output$kpi_lag_win_rate <- renderValueBox({
    lag   <- input$win_lag
    if (lag == 0) {
      return(valueBox("N/A",
                      HTML("Lag-Adjusted Win Rate<br><small style='font-size:10px;opacity:0.75;'>Set lag > 0 in sidebar</small>"),
                      icon = icon("clock"), color = "purple"))
    }
    lwr <- lag_win_rate()
    if (is.null(lwr) || nrow(lwr) == 0) {
      return(valueBox("—",
                      HTML("Lag-Adjusted Win Rate<br><small style='font-size:10px;opacity:0.75;'>Insufficient complete windows in range</small>"),
                      icon = icon("clock"), color = "purple"))
    }
    r <- sum(lwr$lag_booked_amt, na.rm = TRUE) / sum(lwr$amt_bid, na.rm = TRUE)
    
    # Compute the cutoff so we can tell the user how many months are excluded
    all_data    <- get_data()
    max_dt      <- max(all_data$report_date, na.rm = TRUE)
    cutoff_dt   <- max_dt %m-% months(lag)
    excl_label  <- paste0("Bids after ", format(cutoff_dt, "%b %Y"), " excluded (incomplete window)")
    
    valueBox(percent(r, 0.1),
             HTML(paste0("Lag-Adjusted Win Rate<br><small style='font-size:10px;opacity:0.75;'>",
                         excl_label, "</small>")),
             icon = icon("clock"), color = "purple")
  })
  
  # ── CORRELATION INSIGHT ───────────────────────────────────────────────────
  output$correlation_insight <- renderUI({
    d <- filtered_data()
    if (nrow(d) < 10)
      return(tags$p("Waiting for sufficient data points...", class = "text-muted"))
    if (sd(d$qty_bid, na.rm = TRUE) == 0 || sd(d$amt_booked, na.rm = TRUE) == 0)
      return(tags$p("Insufficient variance for correlation.", class = "text-muted"))
    
    r <- cor(d$qty_bid, d$amt_booked, use = "complete.obs")
    txt <- if (is.na(r)) {
      "<span class='insight-text'>⚠️ Data insufficient for Pearson correlation.</span>"
    } else if (r < 0) {
      paste0("<span class='insight-text'>🔴 <b>Negative Correlation (r=", round(r,2), "):</b> More bids → lower revenue. Signals Estimator Fatigue — bid fewer, higher-probability jobs.</span>")
    } else if (r < 0.3) {
      paste0("<span class='insight-text'>⚠️ <b>Weak Correlation (r=", round(r,2), "):</b> Bid quantity has minimal impact on revenue. Quality > quantity.</span>")
    } else {
      paste0("<span class='insight-text'>🟢 <b>Positive Correlation (r=", round(r,2), "):</b> Higher bid volume is translating to revenue. Pipeline mechanics are healthy.</span>")
    }
    HTML(txt)
  })
  
  # ── SPC HELPER ────────────────────────────────────────────────────────────
  # FIX: Baseline mean/SD computed exclusively from the Baseline Period.
  # Those fixed limits are then applied to ALL points in the View Range,
  # so the chart is a true forward projection — not a shifting average.
  spc_limits <- function(df, metric_col, is_quarterly, by_person = FALSE) {
    req(input$baselineRange)
    bl_start <- input$baselineRange[1]
    bl_end   <- input$baselineRange[2]
    
    grp_date <- function(d) if (is_quarterly) floor_date(d, "quarter") else floor_date(d, "month")
    
    if (by_person) {
      # Baseline stats per person
      baseline_stats <- df %>%
        filter(report_date >= bl_start, report_date <= bl_end) %>%
        mutate(date_group = grp_date(report_date)) %>%
        group_by(primary_name, date_group) %>%
        summarise(Val = sum(.data[[metric_col]], na.rm = TRUE), .groups = "drop") %>%
        group_by(primary_name) %>%
        summarise(BL_Mean = mean(Val, na.rm = TRUE),
                  BL_SD   = replace_na(sd(Val, na.rm = TRUE), 0),
                  .groups = "drop")
      
      # Full view range trend
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
  
  # ── SPC: REVENUE ─────────────────────────────────────────────────────────
  output$trend_dollars <- renderPlotly({
    d <- filtered_data()
    req(input$time_granularity, input$view_mode, input$baselineRange)
    validate(need(nrow(d) > 0, "No data."))
    is_q       <- input$time_granularity == "Quarterly"
    by_person  <- input$view_mode == "Individual"
    show_both  <- isTRUE(input$trend_series == "both")
    
    if (by_person) {
      n <- length(unique(d$primary_name))
      validate(need(n <= 4, paste0("⚠️ Select 4 or fewer estimators. Currently: ", n)))
    }
    
    ctrl <- spc_limits(d, "amt_booked", is_q, by_person)
    
    # Also compute bid trend for overlay if requested
    if (show_both) {
      grp_date <- function(dt) if (is_q) floor_date(dt, "quarter") else floor_date(dt, "month")
      if (by_person) {
        bid_trend <- d %>%
          mutate(date_group = grp_date(report_date)) %>%
          group_by(date_group, primary_name) %>%
          summarise(Bid = sum(amt_bid, na.rm = TRUE), .groups = "drop")
        ctrl <- ctrl %>% left_join(bid_trend, by = c("date_group", "primary_name"))
      } else {
        bid_trend <- d %>%
          mutate(date_group = grp_date(report_date)) %>%
          group_by(date_group) %>%
          summarise(Bid = sum(amt_bid, na.rm = TRUE), .groups = "drop")
        ctrl <- ctrl %>% left_join(bid_trend, by = "date_group")
      }
    }
    
    if (by_person) {
      show_trend <- isTRUE(input$show_trendline)
      p <- ggplot(ctrl, aes(x = date_group, y = Val, group = primary_name,
                            text = paste0("<b>", format(date_group, "%b %Y"), "</b><br>",
                                          primary_name, "<br>Booked: ", dollar(round(Val), 1),
                                          "<br>Baseline Mean: ", dollar(round(BL_Mean), 1)))) +
        geom_line(aes(color = primary_name), linewidth = 0.9) +
        geom_point(aes(shape = Anomaly), size = 2) +
        { if (show_both) geom_line(aes(y = Bid, group = primary_name),
                                   linetype = "dotted", color = "#95a5a6", linewidth = 0.8) } +
        # Trend line: OLS fit per estimator — shows direction of travel independent of SPC limits
        { if (show_trend) geom_smooth(aes(group = primary_name, color = primary_name),
                                      method = "lm", se = TRUE, linewidth = 0.7,
                                      linetype = "solid", alpha = 0.12) } +
        geom_hline(aes(yintercept = BL_Mean), color = "#2980b9", linetype = "solid",  alpha = 0.5) +
        geom_hline(aes(yintercept = UCL),     color = "#c0392b", linetype = "dashed", alpha = 0.5) +
        geom_hline(aes(yintercept = LCL),     color = "#c0392b", linetype = "dashed", alpha = 0.5) +
        scale_shape_manual(values = c("Normal" = 16, "Anomaly" = 8)) +
        scale_y_continuous(labels = dollar_format(accuracy = 1)) +
        facet_wrap(~primary_name, ncol = 2, scales = "free_y") +
        theme_minimal() + labs(x = "", y = "") +
        theme(legend.position = "none",
              strip.background = element_rect(fill = "#f4f6f9"),
              strip.text = element_text(face = "bold"))
    } else {
      p <- ggplot(ctrl, aes(x = date_group, y = Val, group = 1,
                            text = paste0("<b>", format(date_group, "%b %Y"), "</b><br>",
                                          "Booked: ", dollar(round(Val), 1),
                                          "<br>Baseline Mean: ", dollar(round(BL_Mean), 1),
                                          if (show_both) paste0("<br>Bid: ", dollar(round(Bid), 1)) else ""))) +
        geom_line(color = "#2c3e50", linewidth = 0.9) +
        geom_point(aes(color = Anomaly), size = 2) +
        { if (show_both) geom_line(aes(y = Bid, group = 1),
                                   color = "#95a5a6", linetype = "dotted", linewidth = 1) } +
        geom_line(aes(y = BL_Mean), color = "#2980b9", linetype = "solid",  alpha = 0.5) +
        geom_line(aes(y = UCL),     color = "#c0392b", linetype = "dashed", alpha = 0.5) +
        geom_line(aes(y = LCL),     color = "#c0392b", linetype = "dashed", alpha = 0.5) +
        scale_color_manual(values = c("Normal" = "#2c3e50", "Anomaly" = "#e74c3c")) +
        scale_y_continuous(labels = dollar_format(accuracy = 1)) +
        theme_minimal() + labs(x = "", y = "") +
        theme(legend.position = "none")
    }
    ggplotly(p, tooltip = "text")
  })
  
  # ── SPC: QUANTITY ─────────────────────────────────────────────────────────
  output$trend_qty <- renderPlotly({
    d <- filtered_data()
    req(input$time_granularity, input$view_mode, input$baselineRange)
    validate(need(nrow(d) > 0, "No data."))
    is_q      <- input$time_granularity == "Quarterly"
    by_person <- input$view_mode == "Individual"
    
    if (by_person) {
      n <- length(unique(d$primary_name))
      validate(need(n <= 4, paste0("⚠️ Select 4 or fewer estimators. Currently: ", n)))
    }
    
    ctrl <- spc_limits(d, "qty_booked", is_q, by_person)
    
    if (by_person) {
      show_trend_qty <- isTRUE(input$show_trendline_qty)
      p <- ggplot(ctrl, aes(x = date_group, y = Val, group = primary_name,
                            text = paste0("<b>", format(date_group, "%b %Y"), "</b><br>",
                                          primary_name, "<br>Jobs: ", comma(Val, 1),
                                          "<br>Baseline Mean: ", comma(BL_Mean, 1)))) +
        geom_line(color = "#27ae60", linewidth = 0.9) +
        geom_point(aes(color = Anomaly), size = 2) +
        { if (show_trend_qty) geom_smooth(aes(group = primary_name),
                                          method = "lm", se = TRUE,
                                          color = "#27ae60", fill = "#27ae60",
                                          linewidth = 0.7, alpha = 0.12) } +
        geom_hline(aes(yintercept = BL_Mean), color = "#2980b9", linetype = "solid",  alpha = 0.5) +
        geom_hline(aes(yintercept = UCL),     color = "#c0392b", linetype = "dashed", alpha = 0.5) +
        geom_hline(aes(yintercept = LCL),     color = "#c0392b", linetype = "dashed", alpha = 0.5) +
        scale_color_manual(values = c("Normal" = "#27ae60", "Anomaly" = "#e74c3c")) +
        scale_y_continuous(labels = comma_format(accuracy = 1)) +
        facet_wrap(~primary_name, ncol = 2, scales = "free_y") +
        theme_minimal() + labs(x = "", y = "") +
        theme(legend.position = "none",
              strip.background = element_rect(fill = "#f4f6f9"),
              strip.text = element_text(face = "bold"))
    } else {
      p <- ggplot(ctrl, aes(x = date_group, y = Val, group = 1,
                            text = paste0("<b>", format(date_group, "%b %Y"), "</b><br>",
                                          "Jobs Booked: ", comma(Val, 1),
                                          "<br>Baseline Mean: ", comma(BL_Mean, 1)))) +
        geom_line(color = "#27ae60", linewidth = 0.9) +
        geom_point(aes(color = Anomaly), size = 2) +
        geom_line(aes(y = BL_Mean), color = "#2980b9", linetype = "solid",  alpha = 0.5) +
        geom_line(aes(y = UCL),     color = "#c0392b", linetype = "dashed", alpha = 0.5) +
        geom_line(aes(y = LCL),     color = "#c0392b", linetype = "dashed", alpha = 0.5) +
        scale_color_manual(values = c("Normal" = "#27ae60", "Anomaly" = "#e74c3c")) +
        scale_y_continuous(labels = comma_format(accuracy = 1)) +
        theme_minimal() + labs(x = "", y = "") +
        theme(legend.position = "none")
    }
    ggplotly(p, tooltip = "text")
  })
  
  # ── NEW: BID FUNNEL CHART ─────────────────────────────────────────────────
  output$bid_funnel_chart <- renderPlotly({
    d <- filtered_data()
    validate(need(nrow(d) > 0, "No data."))
    
    funnel <- d %>%
      group_by(primary_name) %>%
      summarise(
        Bid    = sum(amt_bid,    na.rm = TRUE),
        Booked = sum(amt_booked, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      filter(Bid > 0) %>%
      mutate(
        Win_Rate = Booked / Bid,
        # Sort by total bid descending so highest-effort estimator is on top
        primary_name = fct_reorder(primary_name, Bid)
      ) %>%
      pivot_longer(c(Bid, Booked), names_to = "Type", values_to = "Amount") %>%
      mutate(
        Win_Label = ifelse(Type == "Booked",
                           paste0(percent(Win_Rate, 0.1), " win rate"), NA_character_)
      )
    
    p <- ggplot(funnel,
                aes(x    = primary_name,
                    y    = Amount,
                    fill = Type,
                    text = paste0("<b>", primary_name, "</b><br>",
                                  Type, ": ", dollar(Amount, accuracy = 1),
                                  ifelse(!is.na(Win_Label), paste0("<br>", Win_Label), "")))) +
      geom_col(position = "dodge", width = 0.65) +
      geom_text(aes(label = Win_Label), position = position_dodge(width = 0.65),
                hjust = -0.1, size = 3, na.rm = TRUE, color = "#2c3e50") +
      scale_fill_manual(values = c("Bid" = "#95a5a6", "Booked" = "#27ae60")) +
      scale_y_continuous(labels = dollar_format(accuracy = 1),
                         expand  = expansion(mult = c(0, 0.25))) +
      coord_flip() +
      labs(x = "", y = "Amount ($)", fill = "") +
      theme_minimal() +
      theme(legend.position = "top",
            panel.grid.major.y = element_blank())
    
    ggplotly(p, tooltip = "text")
  })
  
  # ── NEW: SEASONALITY HEATMAP ──────────────────────────────────────────────
  output$seasonality_heatmap <- renderPlotly({
    # Use the full unfiltered dataset so we see all years of history,
    # but still respect the estimator filter if one is active.
    df  <- get_data()
    sel <- input$estimatorSelect
    if (!is.null(sel) && length(sel) > 0) df <- df %>% filter(primary_name %in% sel)
    
    validate(need(nrow(df) > 0, "No data."))
    
    heat <- df %>%
      mutate(Month = month(report_date, label = TRUE, abbr = TRUE),
             Year  = as.integer(year(report_date))) %>%
      group_by(Year, Month) %>%
      summarise(Revenue = sum(amt_booked, na.rm = TRUE), .groups = "drop") %>%
      # Ensure all month × year combos exist (fill missing with 0)
      complete(Year, Month, fill = list(Revenue = 0)) %>%
      mutate(Month = factor(Month, levels = rev(month.abb)))
    
    p <- ggplot(heat,
                aes(x    = factor(Year),
                    y    = Month,
                    fill = Revenue,
                    text = paste0("<b>", Month, " ", Year, "</b><br>",
                                  "Booked: ", dollar(Revenue, accuracy = 1)))) +
      geom_tile(color = "white", linewidth = 0.4) +
      scale_fill_gradient(low  = "#eaf4fb",
                          high = "#1a5276",
                          labels = dollar_format(scale = 1e-3, suffix = "K", accuracy = 1),
                          name   = "Booked ($)") +
      labs(x = "Year", y = "") +
      theme_minimal() +
      theme(axis.text.x  = element_text(angle = 45, hjust = 1),
            panel.grid   = element_blank(),
            legend.position = "right")
    
    ggplotly(p, tooltip = "text")
  })
  
  # ── K-MEANS (uses lag-adjusted win rate if lag > 0) ───────────────────────
  output$scatter_matrix <- renderPlotly({
    dat <- filtered_data()
    validate(need(nrow(dat) > 0, "Upload data to begin."))
    
    # Use lag-adjusted win rate in clustering if available
    lwr <- lag_win_rate()
    
    ds_df <- dat %>%
      group_by(primary_name) %>%
      summarise(
        Total_Bid    = sum(amt_bid,    na.rm = TRUE),
        Total_Booked = sum(amt_booked, na.rm = TRUE),
        Jobs_Bid     = sum(qty_bid,    na.rm = TRUE),
        .groups = "drop"
      ) %>%
      mutate(
        Avg_Job_Size = Total_Bid / Jobs_Bid,
        Win_Rate_SM  = Total_Booked / Total_Bid   # same-month fallback
      )
    
    if (!is.null(lwr) && nrow(lwr) > 0) {
      lag_summary <- lwr %>%
        group_by(primary_name) %>%
        summarise(Lag_Win_Rate = sum(lag_booked_amt, na.rm = TRUE) /
                    sum(amt_bid,        na.rm = TRUE),
                  .groups = "drop")
      ds_df <- ds_df %>% left_join(lag_summary, by = "primary_name") %>%
        mutate(Win_Rate = coalesce(Lag_Win_Rate, Win_Rate_SM))
    } else {
      ds_df$Win_Rate <- ds_df$Win_Rate_SM
    }
    
    ds_df <- ds_df %>%
      filter(Total_Bid > 0,
             !is.na(Win_Rate),     !is.infinite(Win_Rate),
             !is.na(Avg_Job_Size), !is.infinite(Avg_Job_Size))
    
    validate(need(nrow(ds_df) >= 3, "Need at least 3 active estimators to cluster."))
    
    set.seed(42)
    if (any(sapply(ds_df[, c("Total_Bid", "Win_Rate", "Avg_Job_Size")],
                   function(x) sd(x, na.rm = TRUE) == 0))) {
      ds_df$Archetype <- "Archetype 1"
    } else {
      feats  <- ds_df %>% select(Total_Bid, Win_Rate, Avg_Job_Size) %>% scale()
      k      <- min(3, nrow(ds_df) - 1)
      km     <- kmeans(feats, centers = k, nstart = 25)
      ds_df$Cluster <- as.factor(km$cluster)
      
      cl_means <- ds_df %>%
        group_by(Cluster) %>%
        summarise(mean_wr = mean(Win_Rate, na.rm = TRUE), .groups = "drop") %>%
        arrange(desc(mean_wr))
      
      arch_map <- setNames(
        c("High-Efficiency", "Selective", "High-Volume")[seq_len(nrow(cl_means))],
        cl_means$Cluster
      )
      ds_df$Archetype <- arch_map[as.character(ds_df$Cluster)]
    }
    
    p <- ggplot(ds_df,
                aes(x = Total_Bid, y = Win_Rate, color = Archetype,
                    size = Total_Booked,
                    text = paste0("<b>", primary_name, "</b><br>",
                                  "Archetype: ",     Archetype, "<br>",
                                  "Pipeline Effort: ", dollar(Total_Bid,    1), "<br>",
                                  "Win Rate: ",        percent(Win_Rate,    0.1), "<br>",
                                  "Avg Job Size: ",    dollar(Avg_Job_Size, 1), "<br>",
                                  "Total Booked: ",    dollar(Total_Booked, 1)))) +
      geom_point(alpha = 0.85) +
      scale_color_manual(values = c("High-Efficiency" = "#27ae60",
                                    "Selective"       = "#f39c12",
                                    "High-Volume"     = "#e74c3c")) +
      scale_x_continuous(labels = dollar_format(accuracy = 1)) +
      scale_y_continuous(labels = percent_format(accuracy = 1)) +
      labs(x = "Total Amount Bid", y = "Win Rate", color = "Archetype") +
      theme_minimal() + theme(legend.position = "right")
    
    ggplotly(p, tooltip = "text")
  })
  
  # ── ANNUAL CHART (supports Year / Quarter / Month) ───────────────────────
  output$annual_bar_chart <- renderPlotly({
    d         <- filtered_data()
    validate(need(nrow(d) > 0, "No data."))
    gran      <- input$annual_granularity
    by_person <- !is.null(input$estimatorSelect) && length(input$estimatorSelect) <= 5
    
    # Build x-axis grouping label based on granularity
    d <- d %>% mutate(
      x_group = case_when(
        gran == "year"    ~ as.character(year(report_date)),
        gran == "quarter" ~ paste0(year(report_date), " Q", quarter(report_date)),
        gran == "month"   ~ format(report_date, "%Y-%m"),
        TRUE              ~ as.character(year(report_date))
      )
    )
    
    # Keep correct chronological order for x axis
    x_levels <- d %>%
      arrange(report_date) %>%
      pull(x_group) %>%
      unique()
    
    d <- d %>% mutate(x_group = factor(x_group, levels = x_levels))
    
    if (by_person) {
      annual <- d %>% group_by(x_group, primary_name) %>%
        summarise(Booked = sum(amt_booked, na.rm = TRUE), .groups = "drop")
      p <- ggplot(annual, aes(x_group, Booked, fill = primary_name,
                              text = paste0("<b>", x_group, "</b><br>",
                                            primary_name, "<br>",
                                            dollar(round(Booked), accuracy = 1)))) +
        geom_col(position = "dodge") +
        scale_fill_brewer(palette = "Paired")
    } else {
      annual <- d %>% group_by(x_group) %>%
        summarise(Bid    = sum(amt_bid,    na.rm = TRUE),
                  Booked = sum(amt_booked, na.rm = TRUE), .groups = "drop") %>%
        pivot_longer(c(Bid, Booked), names_to = "Type", values_to = "Value")
      p <- ggplot(annual, aes(x_group, Value, fill = Type,
                              text = paste0("<b>", x_group, "</b><br>",
                                            Type, ": ", dollar(round(Value), accuracy = 1)))) +
        geom_col(position = "dodge") +
        scale_fill_manual(values = c("Bid" = "#95a5a6", "Booked" = "#27ae60"))
    }
    
    ggplotly(
      p + scale_y_continuous(labels = dollar_format(accuracy = 1)) +
        labs(x = "", y = "", fill = "") +
        theme_minimal() +
        theme(axis.text.x = element_text(angle = if (gran != "year") 45 else 0, hjust = 1)),
      tooltip = "text"
    )
  })
  
  # ── YOY CHART (grouped or stacked) ───────────────────────────────────────
  output$yoy_chart <- renderPlotly({
    d <- filtered_data()
    req(input$yoy_metric, input$view_mode)
    validate(need(nrow(d) > 0, "No data."))
    msym     <- sym(input$yoy_metric)
    bar_pos  <- input$yoy_position   # "dodge" or "stack"
    
    if (input$view_mode == "Individual") {
      validate(need(length(unique(d$primary_name)) <= 10,
                    "Select 10 or fewer estimators for Individual YoY."))
      ag <- d %>% group_by(year_num, primary_name) %>%
        summarise(V = sum(!!msym, na.rm = TRUE), .groups = "drop") %>%
        arrange(year_num) %>% group_by(primary_name) %>%
        mutate(G = (V - lag(V)) / lag(V)) %>% filter(!is.na(G))
      p <- ggplot(ag, aes(factor(year_num), G, fill = primary_name,
                          text = paste0(year_num, " — ", primary_name,
                                        "<br>Growth: ", percent(G, 0.1)))) +
        geom_col(position = bar_pos) +
        scale_fill_brewer(palette = "Paired")
    } else {
      ag <- d %>% group_by(year_num) %>%
        summarise(V = sum(!!msym, na.rm = TRUE), .groups = "drop") %>%
        arrange(year_num) %>%
        mutate(G = (V - lag(V)) / lag(V)) %>% filter(!is.na(G))
      p <- ggplot(ag, aes(factor(year_num), G, fill = G > 0,
                          text = paste0(year_num, "<br>Growth: ", percent(G, 0.1)))) +
        geom_col(show.legend = FALSE) +
        scale_fill_manual(values = c("TRUE" = "#27ae60", "FALSE" = "#c0392b"))
    }
    ggplotly(p + geom_hline(yintercept = 0) +
               scale_y_continuous(labels = percent_format()) +
               labs(x = "Year", y = "Growth %") + theme_minimal(),
             tooltip = "text")
  })
  
  # ── QUARTERLY DEEP DIVE ───────────────────────────────────────────────────
  output$quarterly_breakdown_chart <- renderPlotly({
    d <- filtered_data()
    req(input$quarter_year_select)
    validate(need(nrow(d) > 0, "No data."))
    
    q <- d %>%
      filter(year_num == as.numeric(input$quarter_year_select)) %>%
      mutate(Quarter = paste0("Q", quarter(report_date))) %>%
      group_by(Quarter) %>%
      summarise(Revenue = sum(amt_booked, na.rm = TRUE), .groups = "drop")
    
    p <- ggplot(q, aes(Quarter, Revenue,
                       text = paste0(Quarter, "<br>", dollar(Revenue, 1)))) +
      geom_col(fill = "#3c8dbc", width = 0.6) +
      scale_y_continuous(labels = dollar_format(accuracy = 1)) +
      labs(x = "", y = "Revenue ($)") + theme_minimal() +
      theme(panel.grid.major.x = element_blank())
    
    ggplotly(p, tooltip = "text")
  })
  
  # ── LEADERBOARD ───────────────────────────────────────────────────────────
  output$leaderboard_table <- renderDataTable({
    d <- filtered_data()
    if (nrow(d) == 0) return(NULL)
    
    lb <- d %>%
      group_by(primary_name) %>%
      summarise(
        Total_Bids   = sum(amt_bid,    na.rm = TRUE),
        Total_Booked = sum(amt_booked, na.rm = TRUE),
        Jobs_Won     = sum(qty_booked, na.rm = TRUE),
        Jobs_Bid     = sum(qty_bid,    na.rm = TRUE),
        .groups = "drop"
      ) %>%
      mutate(
        Win_Rate     = Total_Booked / Total_Bids,
        Avg_Job_Size = Total_Bids   / Jobs_Bid
      )
    
    # Add lag-adjusted win rate column if lag > 0
    if (input$win_lag > 0) {
      lwr <- lag_win_rate()
      if (!is.null(lwr)) {
        lag_col <- lwr %>% group_by(primary_name) %>%
          summarise(Lag_WR = sum(lag_booked_amt, na.rm = TRUE) /
                      sum(amt_bid,        na.rm = TRUE),
                    .groups = "drop")
        lb <- lb %>% left_join(lag_col, by = "primary_name")
      }
    }
    
    lb <- lb %>%
      mutate(
        Total_Bids   = dollar(Total_Bids,   accuracy = 1),
        Total_Booked = dollar(Total_Booked, accuracy = 1),
        Win_Rate     = percent(Win_Rate,    accuracy = 0.1),
        Avg_Job_Size = dollar(Avg_Job_Size, accuracy = 1)
      ) %>%
      rename(Estimator = primary_name, `Total Revenue` = Total_Booked,
             `Total Bids` = Total_Bids, `Win % (same-mo)` = Win_Rate,
             `Jobs Won` = Jobs_Won, `Jobs Bid` = Jobs_Bid,
             `Avg Job Size` = Avg_Job_Size)
    
    if ("Lag_WR" %in% names(lb))
      lb <- lb %>%
      mutate(Lag_WR = percent(Lag_WR, accuracy = 0.1)) %>%
      rename(`Win % (lag-adj)` = Lag_WR)
    
    datatable(lb,
              options  = list(pageLength = 15, scrollX = TRUE,
                              order = list(list(2, "desc"))),
              rownames = FALSE)
  })
  
  # ── ADMIN ─────────────────────────────────────────────────────────────────
  output$db_status_text <- renderText({
    paste("Database records loaded:", comma(nrow(get_data())))
  })
  
  # ── TIER 1: EXCEL REPORT DOWNLOAD ────────────────────────────────────────
  # Exports a formatted .xlsx with four sheets:
  #   1. Summary KPIs        2. Leaderboard
  #   3. Monthly Detail      4. Seasonality
  # No LaTeX, no Pandoc, no external dependencies beyond openxlsx.
  output$download_report <- downloadHandler(
    filename = function() {
      paste0("Estimator_Intelligence_", format(Sys.Date(), "%Y-%m-%d"), ".xlsx")
    },
    content = function(file) {
      d   <- filtered_data()
      req(nrow(d) > 0)
      
      withProgress(message = "Building Excel report…", value = 0.2, {
        
        wb <- createWorkbook()
        
        # ── Style helpers ──────────────────────────────────────────────────
        header_style <- createStyle(
          fontName      = "Calibri", fontSize = 11, fontColour = "#FFFFFF",
          fgFill        = "#2c3e50", halign = "LEFT", valign = "CENTER",
          textDecoration = "bold", border = "Bottom", borderColour = "#27ae60"
        )
        title_style <- createStyle(
          fontName = "Calibri", fontSize = 14, textDecoration = "bold",
          fontColour = "#2c3e50"
        )
        kpi_label_style <- createStyle(
          fontName = "Calibri", fontSize = 11, fontColour = "#5a5a5a",
          textDecoration = "bold"
        )
        kpi_value_style <- createStyle(
          fontName = "Calibri", fontSize = 13, fontColour = "#2c3e50",
          textDecoration = "bold", halign = "RIGHT"
        )
        alt_row_style <- createStyle(fgFill = "#f4f6f9")
        border_style  <- createStyle(
          border = "TopBottomLeftRight", borderColour = "#dddddd"
        )
        dollar_style <- createStyle(numFmt = "$#,##0")
        pct_style    <- createStyle(numFmt = "0.0%")
        date_style   <- createStyle(numFmt = "mmm yyyy")
        
        # ── Computed values ────────────────────────────────────────────────
        sel_label  <- {
          sel <- isolate(input$estimatorSelect)
          if (is.null(sel) || length(sel) == 0) "All Estimators"
          else paste(sel, collapse = ", ")
        }
        total_bid    <- sum(d$amt_bid,    na.rm = TRUE)
        total_booked <- sum(d$amt_booked, na.rm = TRUE)
        win_rate     <- total_booked / total_bid
        job_win_rate <- sum(d$qty_booked, na.rm = TRUE) / sum(d$qty_bid, na.rm = TRUE)
        
        # ════════════════════════════════════════════════════════════════════
        # SHEET 1 — Summary KPIs
        # ════════════════════════════════════════════════════════════════════
        addWorksheet(wb, "Summary")
        setColWidths(wb, "Summary", cols = 1:3, widths = c(30, 22, 22))
        
        writeData(wb, "Summary", "Estimator Intelligence Report", startRow = 1, startCol = 1)
        addStyle(wb, "Summary", title_style, rows = 1, cols = 1)
        
        meta <- data.frame(
          Field = c("Reporting Period", "Estimator Scope", "Win Rate Lag", "Generated"),
          Value = c(
            paste(format(isolate(input$dateRange[1]), "%b %d %Y"), "—",
                  format(isolate(input$dateRange[2]), "%b %d %Y")),
            sel_label,
            paste(isolate(input$win_lag), "month(s)"),
            format(Sys.Date(), "%B %d, %Y")
          ), stringsAsFactors = FALSE
        )
        writeData(wb, "Summary", meta, startRow = 3, startCol = 1, colNames = FALSE)
        
        kpi_data <- data.frame(
          Metric = c("Total Bid Volume", "Total Booked Revenue",
                     "Dollar Win Rate (same-month)", "Job Win Rate (same-month)",
                     "Active Estimators"),
          Value = c(
            dollar(total_bid,    accuracy = 1),
            dollar(total_booked, accuracy = 1),
            percent(win_rate,     accuracy = 0.1),
            percent(job_win_rate, accuracy = 0.1),
            as.character(length(unique(d$primary_name)))
          ), stringsAsFactors = FALSE
        )
        writeData(wb, "Summary", kpi_data, startRow = 9, startCol = 1, colNames = TRUE)
        addStyle(wb, "Summary", header_style, rows = 9, cols = 1:2)
        for (r in seq(10, 10 + nrow(kpi_data) - 1, 2))
          addStyle(wb, "Summary", alt_row_style, rows = r, cols = 1:2, gridExpand = TRUE)
        
        setProgress(0.4)
        
        # ════════════════════════════════════════════════════════════════════
        # SHEET 2 — Leaderboard
        # ════════════════════════════════════════════════════════════════════
        addWorksheet(wb, "Leaderboard")
        setColWidths(wb, "Leaderboard", cols = 1:7,
                     widths = c(22, 18, 18, 10, 10, 14, 14))
        
        lb_raw <- d %>%
          group_by(Estimator = primary_name) %>%
          summarise(
            `Total Bids`    = sum(amt_bid,    na.rm = TRUE),
            `Total Revenue` = sum(amt_booked, na.rm = TRUE),
            `Jobs Bid`      = sum(qty_bid,    na.rm = TRUE),
            `Jobs Won`      = sum(qty_booked, na.rm = TRUE),
            `Win Rate`      = `Total Revenue` / `Total Bids`,
            `Avg Job Size`  = `Total Bids`    / `Jobs Bid`,
            .groups = "drop"
          ) %>%
          arrange(desc(`Total Revenue`))
        
        writeData(wb, "Leaderboard", lb_raw, startRow = 1, colNames = TRUE)
        addStyle(wb, "Leaderboard", header_style, rows = 1, cols = 1:7)
        # Dollar formatting on raw numeric columns
        addStyle(wb, "Leaderboard", dollar_style,
                 rows = 2:(nrow(lb_raw) + 1), cols = c(2, 3, 7), gridExpand = TRUE)
        addStyle(wb, "Leaderboard", pct_style,
                 rows = 2:(nrow(lb_raw) + 1), cols = 6, gridExpand = TRUE)
        for (r in seq(3, nrow(lb_raw) + 1, 2))
          addStyle(wb, "Leaderboard", alt_row_style, rows = r, cols = 1:7, gridExpand = TRUE)
        
        setProgress(0.6)
        
        # ════════════════════════════════════════════════════════════════════
        # SHEET 3 — Monthly Detail
        # ════════════════════════════════════════════════════════════════════
        addWorksheet(wb, "Monthly Detail")
        setColWidths(wb, "Monthly Detail", cols = 1:7,
                     widths = c(22, 14, 14, 10, 14, 10, 14))
        
        monthly_raw <- d %>%
          arrange(primary_name, report_date) %>%
          mutate(
            # Pre-format as plain strings — eliminates the Excel serial-integer
            # bug entirely. No numFmt style needed; style conflicts are impossible.
            Date     = format(report_date, "%b %Y"),
            Win_Rate = ifelse(amt_bid > 0,
                              percent(amt_booked / amt_bid, accuracy = 0.1),
                              "0.0%")
          ) %>%
          select(
            Estimator    = primary_name,
            Date,
            `Bid ($)`    = amt_bid,
            `Jobs Bid`   = qty_bid,
            `Booked ($)` = amt_booked,
            `Jobs Won`   = qty_booked,
            `Win Rate`   = Win_Rate
          )
        
        writeData(wb, "Monthly Detail", monthly_raw, startRow = 1, colNames = TRUE)
        addStyle(wb, "Monthly Detail", header_style, rows = 1, cols = 1:7)
        # Alt shading FIRST — dollar formats applied after so they are never overwritten
        for (r in seq(3, nrow(monthly_raw) + 1, 2))
          addStyle(wb, "Monthly Detail", alt_row_style,
                   rows = r, cols = 1:7, gridExpand = TRUE)
        addStyle(wb, "Monthly Detail", dollar_style,
                 rows = 2:(nrow(monthly_raw) + 1), cols = c(3, 5), gridExpand = TRUE)
        
        setProgress(0.8)
        
        # ════════════════════════════════════════════════════════════════════
        # SHEET 4 — Seasonality
        # ════════════════════════════════════════════════════════════════════
        addWorksheet(wb, "Seasonality")
        setColWidths(wb, "Seasonality", cols = 1:3, widths = c(14, 10, 18))
        
        seasonal <- d %>%
          mutate(Month = month(report_date, label = TRUE, abbr = FALSE),
                 Year  = year(report_date)) %>%
          group_by(Year, Month) %>%
          summarise(`Booked ($)` = sum(amt_booked, na.rm = TRUE), .groups = "drop") %>%
          arrange(Year, Month)
        
        writeData(wb, "Seasonality", seasonal, startRow = 1, colNames = TRUE)
        addStyle(wb, "Seasonality", header_style, rows = 1, cols = 1:3)
        addStyle(wb, "Seasonality", dollar_style,
                 rows = 2:(nrow(seasonal) + 1), cols = 3, gridExpand = TRUE)
        for (r in seq(3, nrow(seasonal) + 1, 2))
          addStyle(wb, "Seasonality", alt_row_style,
                   rows = r, cols = 1:3, gridExpand = TRUE)
        
        setProgress(0.95)
        saveWorkbook(wb, file, overwrite = TRUE)
      })
    }
  )
  
  output$preview_table <- renderDataTable({
    d <- get_data()
    if (nrow(d) == 0) return(NULL)
    d %>% arrange(desc(report_date)) %>% head(5) %>%
      select(Estimator = primary_name, Date = report_date,
             Bid = amt_bid, Booked = amt_booked)
  }, options = list(dom = "t", searching = FALSE))
  
  # ── INTERVENTION ANALYSIS (ITS) ──────────────────────────────────────────
  
  # Populate ITS estimator picker from DB
  observe({
    df <- get_data()
    if (nrow(df) == 0) return()
    # Only estimators with enough history to fit a model (≥ 18 months)
    eligible <- df %>%
      group_by(primary_name) %>%
      summarise(n = n(), .groups = "drop") %>%
      filter(n >= 18) %>%
      pull(primary_name) %>%
      sort()
    updatePickerInput(session, "its_estimator",
                      choices  = eligible,
                      selected = eligible[1])
  })
  
  # Reactive: build the time series for the selected estimator and metric
  its_series <- reactive({
    df  <- get_data()
    req(nrow(df) > 0, input$its_estimator, input$its_metric)
    
    metric <- input$its_metric
    
    series <- df %>%
      filter(primary_name == input$its_estimator) %>%
      arrange(report_date) %>%
      mutate(date_group = floor_date(report_date, "month")) %>%
      group_by(date_group) %>%
      summarise(
        amt_bid    = sum(amt_bid,    na.rm = TRUE),
        amt_booked = sum(amt_booked, na.rm = TRUE),
        qty_booked = sum(qty_booked, na.rm = TRUE),
        qty_bid    = sum(qty_bid,    na.rm = TRUE),
        .groups = "drop"
      ) %>%
      mutate(
        win_rate = ifelse(amt_bid > 0, amt_booked / amt_bid, NA_real_),
        value    = .data[[metric]],
        t        = row_number()   # time index for regression
      ) %>%
      filter(!is.na(value))
    
    series
  })
  
  # ITS state: holds results after "Run Analysis" is clicked
  its_result <- reactiveVal(NULL)
  
  # Allow clicking a breakpoint row to set it as the intervention date
  its_selected_bp <- reactiveVal(NULL)
  
  observeEvent(input$its_breakpoints_table_rows_selected, {
    bps <- its_result()$breakpoints
    idx <- input$its_breakpoints_table_rows_selected
    if (!is.null(bps) && !is.null(idx) && idx <= nrow(bps)) {
      sel_date <- bps$Date[idx]
      updateDateInput(session, "its_manual_date", value = sel_date)
      its_selected_bp(sel_date)
    }
  })
  
  observeEvent(input$its_run, {
    series <- its_series()
    validate_msg <- NULL
    if (nrow(series) < 18) validate_msg <- "Need at least 18 monthly observations."
    if (!is.null(validate_msg)) { its_result(list(error = validate_msg)); return() }
    
    lag        <- input$win_lag
    metric     <- input$its_metric
    manual_dt  <- input$its_manual_date
    autodetect <- isTRUE(input$its_autodetect)
    
    # ── AUTO-DETECT BREAKPOINTS via strucchange::breakpoints ──────────────
    bp_table <- NULL
    bp_dates <- c()
    
    if (autodetect) {
      tryCatch({
        bp_fit <- breakpoints(value ~ t, data = series, h = 6)
        bp_idx <- bp_fit$breakpoints
        if (!any(is.na(bp_idx)) && length(bp_idx) > 0) {
          bp_dates <- series$date_group[bp_idx]
          # Compute F-statistic for each breakpoint via Fstats
          fs        <- Fstats(value ~ t, data = series)
          bp_table  <- tibble(
            Date         = bp_dates,
            `Time Index` = bp_idx,
            `F-Statistic`= round(fs$Fstats[bp_idx], 2),
            `Months of Data Before` = bp_idx,
            `Months of Data After`  = nrow(series) - bp_idx
          )
        }
      }, error = function(e) NULL)
    }
    
    # ── DETERMINE ACTIVE INTERVENTION DATE ────────────────────────────────
    # Priority: (1) manual date if set, (2) top auto-detected break, (3) midpoint
    active_date <- NULL
    
    if (!is.null(manual_dt) && !is.na(manual_dt) &&
        manual_dt >= min(series$date_group) &&
        manual_dt <= max(series$date_group)) {
      active_date <- floor_date(as.Date(manual_dt), "month")
    } else if (length(bp_dates) > 0) {
      active_date <- bp_dates[1]
    } else {
      active_date <- series$date_group[round(nrow(series) / 2)]
    }
    
    # ── FIT SEGMENTED REGRESSION ──────────────────────────────────────────
    # Model: Y = b0 + b1*t + b2*D + b3*(t - T0)*D + e
    #   D     = 1 post-intervention, 0 pre
    #   b2    = immediate level change at intervention
    #   b3    = slope change (post slope - pre slope)
    #   b1    = pre-intervention monthly trend
    #   b1+b3 = post-intervention monthly trend
    t0 <- which(series$date_group == active_date)
    if (length(t0) == 0) t0 <- which.min(abs(series$date_group - active_date))
    
    model_df <- series %>%
      mutate(
        D         = as.integer(t >= t0),
        t_post    = (t - t0) * D
      )
    
    fit <- tryCatch(
      lm(value ~ t + D + t_post, data = model_df),
      error = function(e) NULL
    )
    
    if (is.null(fit)) {
      its_result(list(error = "Model could not be fitted. Try a different intervention date or metric."))
      return()
    }
    
    cf      <- coeftest(fit)
    cf_df   <- as.data.frame.matrix(cf) %>%
      tibble::rownames_to_column("Term") %>%
      mutate(
        Term        = recode(Term,
                             `(Intercept)` = "Intercept (baseline level)",
                             `t`           = "Pre-intervention monthly trend",
                             `D`           = "Level change at intervention",
                             `t_post`      = "Slope change after intervention"),
        Estimate    = round(Estimate, 2),
        `Std Error` = round(`Std. Error`, 2),
        `t value`   = round(`t value`, 3),
        `p value`   = ifelse(`Pr(>|t|)` < 0.001, "< 0.001",
                             as.character(round(`Pr(>|t|)`, 3))),
        Significant = ifelse(`Pr(>|t|)` < 0.05, "Yes *", "No")
      ) %>%
      select(Term, Estimate, `Std Error`, `t value`, `p value`, Significant)
    
    # ── FITTED VALUES FOR CHART ───────────────────────────────────────────
    model_df <- model_df %>%
      mutate(
        fitted     = fitted(fit),
        resid      = residuals(fit),
        # Counterfactual: what would have happened without the intervention
        counterfact = coef(fit)[1] + coef(fit)[2] * t
      )
    
    # Pre/post split for color-coded trend lines
    pre  <- model_df %>% filter(t <= t0)
    post <- model_df %>% filter(t >= t0)
    
    # Effect size interpretation
    b2 <- coef(fit)["D"]
    b3 <- coef(fit)["t_post"]
    p2 <- cf[3, 4]
    p3 <- cf[4, 4]
    
    level_txt <- if (p2 < 0.05) {
      dir <- if (b2 > 0) "increased" else "decreased"
      paste0("The outcome ", dir, " immediately by ",
             if (metric %in% c("amt_booked","amt_bid")) dollar(abs(round(b2))) else round(abs(b2), 1),
             " at the intervention point (p = ", round(p2, 3), ").")
    } else {
      paste0("No significant immediate level change detected (p = ", round(p2, 3), ").")
    }
    
    slope_txt <- if (p3 < 0.05) {
      dir <- if (b3 > 0) "accelerated" else "decelerated"
      paste0("The monthly trajectory ", dir, " by ",
             if (metric %in% c("amt_booked","amt_bid")) dollar(abs(round(b3))) else round(abs(b3), 3),
             " per month after the intervention (p = ", round(p3, 3), ").")
    } else {
      paste0("No significant change in monthly trajectory detected (p = ", round(p3, 3), ").")
    }
    
    overall <- if (p2 < 0.05 || p3 < 0.05) {
      "At least one significant intervention effect was detected."
    } else {
      "No statistically significant intervention effect was detected at the 5% level."
    }
    
    its_result(list(
      series       = model_df,
      pre          = pre,
      post         = post,
      fit          = fit,
      coef_df      = cf_df,
      breakpoints  = bp_table,
      active_date  = active_date,
      t0           = t0,
      metric       = metric,
      estimator    = input$its_estimator,
      level_txt    = level_txt,
      slope_txt    = slope_txt,
      overall      = overall,
      r2           = round(summary(fit)$r.squared, 3),
      autodetect   = autodetect
    ))
  })
  
  # ── ITS SUMMARY UI ────────────────────────────────────────────────────────
  output$its_summary_ui <- renderUI({
    res <- its_result()
    if (is.null(res)) return(tags$p("Configure settings and click 'Run Analysis'.",
                                    class = "text-muted"))
    if (!is.null(res$error)) return(tags$p(res$error, style = "color:#c0392b;"))
    
    metric_label <- c(
      amt_booked = "Booked Revenue ($)",
      amt_bid    = "Bid Volume ($)",
      qty_booked = "Jobs Booked",
      win_rate   = "Win Rate"
    )
    
    tagList(
      tags$table(style = "width:100%; border-collapse:collapse; font-size:13px;",
                 tags$tr(
                   tags$td(style = "padding:6px 10px; font-weight:600; color:#2c3e50; width:40%;",
                           "Estimator"),
                   tags$td(style = "padding:6px 10px; color:#5A6A72;", res$estimator)
                 ),
                 tags$tr(style = "background:#f4f6f9;",
                         tags$td(style = "padding:6px 10px; font-weight:600; color:#2c3e50;",
                                 "Outcome Metric"),
                         tags$td(style = "padding:6px 10px; color:#5A6A72;",
                                 metric_label[res$metric])
                 ),
                 tags$tr(
                   tags$td(style = "padding:6px 10px; font-weight:600; color:#2c3e50;",
                           "Intervention Date"),
                   tags$td(style = "padding:6px 10px; color:#5A6A72;",
                           format(res$active_date, "%B %Y"),
                           if (!is.null(input$its_manual_date) &&
                               !is.na(input$its_manual_date) &&
                               floor_date(as.Date(input$its_manual_date), "month") == res$active_date)
                             tags$span(" (manager-specified)", style = "color:#e67e22; font-size:11px;")
                           else
                             tags$span(" (auto-detected)", style = "color:#27ae60; font-size:11px;")
                   )
                 ),
                 tags$tr(style = "background:#f4f6f9;",
                         tags$td(style = "padding:6px 10px; font-weight:600; color:#2c3e50;",
                                 "Model R\u00b2"),
                         tags$td(style = "padding:6px 10px; color:#5A6A72;", res$r2)
                 )
      ),
      tags$hr(style = "margin:12px 0;"),
      tags$p(style = "font-size:13px; color:#2c3e50; font-weight:600;",
             res$overall),
      tags$p(style = "font-size:12px; color:#5A6A72; margin:4px 0;",
             tags$b("Level change: "), res$level_txt),
      tags$p(style = "font-size:12px; color:#5A6A72; margin:4px 0;",
             tags$b("Slope change: "), res$slope_txt)
    )
  })
  
  # ── ITS CHART ─────────────────────────────────────────────────────────────
  output$its_chart <- renderPlotly({
    res <- its_result()
    if (is.null(res) || !is.null(res$error)) return(plotly_empty())
    
    series  <- res$series
    pre     <- res$pre
    post    <- res$post
    metric  <- res$metric
    fmt     <- if (metric %in% c("amt_booked","amt_bid"))
      function(x) dollar(round(x))
    else if (metric == "win_rate")
      function(x) percent(x, 0.1)
    else
      function(x) comma(round(x))
    
    y_label <- c(amt_booked="Booked Revenue ($)", amt_bid="Bid Volume ($)",
                 qty_booked="Jobs Booked", win_rate="Win Rate")[metric]
    
    p <- ggplot(series, aes(x = date_group)) +
      # Actual values
      geom_point(aes(y = value,
                     text = paste0("<b>", format(date_group, "%b %Y"), "</b><br>",
                                   "Actual: ", fmt(value))),
                 color = "#95a5a6", size = 2.2, alpha = 0.8) +
      # Counterfactual (what would have happened without intervention)
      geom_line(aes(y = counterfact, group = 1),
                color = "#c0392b", linetype = "dotted", linewidth = 0.7, alpha = 0.6) +
      # Pre-intervention fitted trend
      geom_line(data = pre,  aes(y = fitted, group = 1), color = "#2980b9", linewidth = 1.1) +
      # Post-intervention fitted trend
      geom_line(data = post, aes(y = fitted, group = 1), color = "#27ae60", linewidth = 1.1) +
      # Intervention vertical line
      geom_vline(xintercept = as.numeric(res$active_date),
                 color = "#e67e22", linetype = "dashed", linewidth = 0.9) +
      annotate("text",
               x     = res$active_date,
               y     = max(series$value, na.rm = TRUE) * 0.95,
               label = paste("Intervention\n", format(res$active_date, "%b %Y")),
               hjust = -0.08, size = 3, color = "#e67e22") +
      scale_y_continuous(labels = if (metric %in% c("amt_booked","amt_bid"))
        dollar_format(accuracy = 1)
        else if (metric == "win_rate")
          percent_format(accuracy = 1)
        else
          comma_format(accuracy = 1)) +
      labs(x = NULL, y = y_label,
           title = paste0(res$estimator, " — ITS: ", y_label),
           caption = "Blue = pre-intervention trend  |  Green = post-intervention trend  |  Red dotted = counterfactual  |  Orange dashed = intervention point") +
      theme_minimal() +
      theme(plot.caption = element_text(size = 8, color = "#95a5a6"),
            plot.title   = element_text(size = 12, face = "bold", color = "#2c3e50"))
    
    ggplotly(p, tooltip = "text") %>%
      layout(legend = list(orientation = "h"))
  })
  
  # ── ITS BREAKPOINTS TABLE ─────────────────────────────────────────────────
  output$its_breakpoints_table <- renderDataTable({
    res <- its_result()
    if (is.null(res) || !is.null(res$error)) return(NULL)
    if (is.null(res$breakpoints) || nrow(res$breakpoints) == 0) {
      return(data.frame(Message = "No significant breakpoints detected automatically."))
    }
    res$breakpoints %>%
      mutate(Date = format(Date, "%B %Y")) %>%
      arrange(desc(`F-Statistic`))
  }, selection = "single",
  options  = list(dom = "t", pageLength = 10, scrollX = TRUE))
  
  # ── ITS COEFFICIENT TABLE ─────────────────────────────────────────────────
  output$its_coef_table <- renderDataTable({
    res <- its_result()
    if (is.null(res) || !is.null(res$error)) return(NULL)
    res$coef_df
  }, options = list(dom = "t", pageLength = 10, scrollX = TRUE))
  
}

shinyApp(ui, server)
