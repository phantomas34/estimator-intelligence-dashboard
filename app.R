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

# --- DATABASE CONNECTION ---
connect_db <- function() {
  dbConnect(RPostgres::Postgres(), 
            dbname = "sales_db", 
            host = "localhost", 
            port = 5433, 
            user = Sys.info()[["user"]], 
            password = "")
}

# ... (Libraries and DB connection remain the same) ...

# --- UI ---
ui <- dashboardPage(
  skin = "black",
  dashboardHeader(title = span("Estimator Intelligence", style = "font-weight: bold; font-size: 18px;")),
  
  dashboardSidebar(
    width = 250,
    sidebarMenu(
      menuItem("Executive Dashboard", tabName = "dashboard", icon = icon("chart-area")),
      menuItem("Data Management", tabName = "admin", icon = icon("database")),
      hr(),
      
      div(style = "padding: 10px;",
          h5("Filter Controls", style = "color: #b8c7ce; text-transform: uppercase; font-size: 12px; margin-bottom: 10px;"),
          
          dateRangeInput("dateRange", NULL, start = "2021-01-01", end = Sys.Date(), separator = " to "),
          
          # --- NEW: VIEW MODE TOGGLE ---
          radioGroupButtons(
            inputId = "view_mode",
            label = "Breakdown Mode",
            choices = c("Aggregate", "Individual"),
            selected = "Aggregate",
            status = "success", # Green to stand out
            size = "sm",
            justified = TRUE
          ),
          br(),
          
          radioGroupButtons(
            inputId = "time_granularity",
            label = "Time Scale",
            choices = c("Monthly", "Quarterly"),
            selected = "Monthly",
            status = "primary",
            size = "sm",
            justified = TRUE
          ),
          br(),
          
          pickerInput("estimatorSelect", "Estimators:", 
                      choices = NULL, multiple = TRUE,
                      options = list(`actions-box` = TRUE, `live-search` = TRUE, `selected-text-format` = "count > 2", `count-selected-text` = "{0} Selected", `none-selected-text` = "All Company Data")),
          
          div(style = "text-align: center; margin-top: 15px;",
              actionButton("refresh", "Refresh Data", icon = icon("sync"), width = "90%", class = "btn-primary btn-sm btn-flat"))
      )
    )
  ),
  
  dashboardBody(
    tags$head(tags$style(HTML("
      .box.box-solid.box-primary>.box-header { background: #2c3e50; }
      .box.box-solid.box-primary { border: 1px solid #2c3e50; }
      .content-wrapper, .right-side { background-color: #f4f6f9; }
      .plotly { margin-top: 10px; }
    "))),
    
    tabItems(
      tabItem(tabName = "dashboard",
              fluidRow(
                valueBoxOutput("kpi_total_bid", width = 3),
                valueBoxOutput("kpi_total_booked", width = 3),
                valueBoxOutput("kpi_win_rate_amt", width = 3),
                valueBoxOutput("kpi_win_rate_qty", width = 3)
              ),
              fluidRow(
                tabBox(title = textOutput("trend_box_title"), width = 12, side = "right", selected = "Dollar Amounts ($)",
                       tabPanel("Dollar Amounts ($)", plotlyOutput("trend_dollars", height = "400px"), p(class = "text-muted", style = "font-size: 11px;", "In 'Individual' mode, dashed trends are removed for clarity.")),
                       tabPanel("Quantity (Count)", plotlyOutput("trend_qty", height = "400px")))
              ),
              fluidRow(
                box(title = "Annual Performance", status = "primary", solidHeader = TRUE, width = 6,
                    plotlyOutput("annual_bar_chart", height = "400px")),
                
                box(title = "Year-Over-Year Growth (%)", status = "primary", solidHeader = TRUE, width = 6,
                    fluidRow(
                      column(width = 7, p(class = "text-muted", style = "font-size: 11px; margin-top: 10px;", "Aggregate: Red/Green | Individual: Colored by Person")),
                      column(width = 5, pickerInput("yoy_metric", NULL, choices = c("Booked Revenue ($)"="amt_booked", "Bid Volume ($)"="amt_bid", "Jobs Booked (Qty)"="qty_booked", "Jobs Bid (Qty)"="qty_bid"), selected = "amt_booked", width = "100%", options = list(style = "btn-default btn-sm"))) 
                    ),
                    plotlyOutput("yoy_chart", height = "350px")
                )
              ),
              fluidRow(box(title = "Top Estimator Leaderboard", status = "warning", solidHeader = TRUE, width = 12, dataTableOutput("leaderboard_table")))
      ),
      tabItem(tabName = "admin",
              fluidRow(
                box(title = "Upload Data", status = "danger", solidHeader = TRUE, width = 5,
                    fileInput("file1", "Upload CSV", accept = c(".csv")),
                    actionButton("process_upload", "Process & Import", class = "btn-danger", width = "100%")),
                box(title = "System Status", status = "info", solidHeader = TRUE, width = 7,
                    verbatimTextOutput("db_status_text"), dataTableOutput("preview_table"))
              )
      )
    )
  )
)

# --- SERVER ---
server <- function(input, output, session) {
  
  data_trigger <- reactiveVal(0)
  
  get_data <- reactive({
    input$refresh; data_trigger(); con <- tryCatch(connect_db(), error = function(e) return(NULL))
    if(is.null(con)) return(data.frame()); if(!dbExistsTable(con, "monthly_sales")) { dbDisconnect(con); return(data.frame()) }
    query <- "SELECT e.primary_name, s.*, EXTRACT(YEAR FROM s.report_date) as year_num FROM monthly_sales s JOIN estimators e ON s.estimator_id = e.id"
    df <- dbGetQuery(con, query); dbDisconnect(con); return(df)
  })
  
  observeEvent(input$process_upload, {
    req(input$file1); progress <- shiny::Progress$new(message = "Processing...", value = 0.2); on.exit(progress$close())
    tryCatch({
      raw_data <- read_csv(input$file1$datapath, show_col_types = FALSE)
      clean_data <- raw_data %>% mutate(report_date = ym(MoYear)) %>% mutate(across(c(AmtBid, AmtBooked), ~ as.numeric(gsub("[\\$,]", "", .)))) %>% mutate(clean_name = case_when(Estimator == "Scott W. Hutchings" ~ "Scott Hutchings", Estimator == "SW" ~ "Scott Hutchings", Estimator == "SH" ~ "Scott Hutchings", TRUE ~ Estimator))
      con <- connect_db()
      unique_estimators <- unique(clean_data$clean_name)
      for(name in unique_estimators){ tryCatch({ dbExecute(con, "INSERT INTO estimators (primary_name) VALUES ($1) ON CONFLICT DO NOTHING", params = list(name)) }, error = function(e) NULL) }
      est_map <- dbGetQuery(con, "SELECT id, primary_name FROM estimators")
      upload_ready <- clean_data %>% left_join(est_map, by = c("clean_name" = "primary_name")) %>% select(estimator_id = id, report_date, amt_bid = AmtBid, qty_bid = QtyBid, amt_booked = AmtBooked, qty_booked = QtyBooked)
      dbWriteTable(con, "staging_sales", upload_ready, overwrite = TRUE, temporary = TRUE)
      dbExecute(con, "INSERT INTO monthly_sales (estimator_id, report_date, amt_bid, qty_bid, amt_booked, qty_booked) SELECT estimator_id, report_date, amt_bid, qty_bid, amt_booked, qty_booked FROM staging_sales ON CONFLICT (estimator_id, report_date) DO NOTHING;")
      dbExecute(con, "DROP TABLE IF EXISTS staging_sales"); dbDisconnect(con)
      showNotification("Import Complete.", type = "message"); data_trigger(data_trigger() + 1)
    }, error = function(e) { showNotification(paste("Error:", e$message), type = "error") })
  })
  
  observe({ df <- get_data(); if(nrow(df) > 0) { est_list <- sort(unique(df$primary_name)); updatePickerInput(session, "estimatorSelect", choices = est_list, selected = input$estimatorSelect) } })
  
  filtered_data <- reactive({
    req(input$dateRange); df <- get_data(); if(nrow(df) == 0) return(df[0, ])
    df <- df %>% filter(report_date >= input$dateRange[1], report_date <= input$dateRange[2])
    if (!is.null(input$estimatorSelect)) { df <- df %>% filter(primary_name %in% input$estimatorSelect) }
    return(df)
  })
  
  output$trend_box_title <- renderText({ req(input$time_granularity); paste("Trends (Bid vs Booked) -", input$time_granularity, "View") })
  
  output$kpi_total_bid <- renderValueBox({ valueBox(dollar(sum(filtered_data()$amt_bid, na.rm=T)), "Total Bid Volume", icon = icon("file-invoice-dollar"), color = "blue") })
  output$kpi_total_booked <- renderValueBox({ valueBox(dollar(sum(filtered_data()$amt_booked, na.rm=T)), "Total Booked Revenue", icon = icon("money-bill-wave"), color = "green") })
  output$kpi_win_rate_amt <- renderValueBox({ d <- filtered_data(); r <- sum(d$amt_booked, na.rm=T)/sum(d$amt_bid, na.rm=T); valueBox(percent(r, 0.1), "Dollar Win Rate", icon = icon("chart-line"), color = "yellow") })
  output$kpi_win_rate_qty <- renderValueBox({ d <- filtered_data(); r <- sum(d$qty_booked, na.rm=T)/sum(d$qty_bid, na.rm=T); valueBox(percent(r, 0.1), "Job Win Rate", icon = icon("check-circle"), color = "purple") })
  
  # --- LOGIC FOR TRENDS (AGGREGATE vs INDIVIDUAL) ---
  render_trend_chart <- function(val_bid, val_booked, prefix) {
    d <- filtered_data(); req(input$time_granularity, input$view_mode); validate(need(nrow(d) > 0, "No data."))
    
    # 1. Prepare Data Grouping based on Mode
    if(input$view_mode == "Individual") {
      # Keep 'primary_name' in grouping
      d_trend <- d %>%
        mutate(date_group = if(input$time_granularity == "Quarterly") floor_date(report_date, "quarter") else report_date) %>%
        group_by(date_group, primary_name) %>% 
        summarise(Bid = sum(!!sym(val_bid), na.rm=T), Booked = sum(!!sym(val_booked), na.rm=T)) %>%
        pivot_longer(c(Bid, Booked), names_to = "Type", values_to = "Value")
    } else {
      # Aggregate everything
      d_trend <- d %>%
        mutate(date_group = if(input$time_granularity == "Quarterly") floor_date(report_date, "quarter") else report_date) %>%
        group_by(date_group) %>%
        summarise(Bid = sum(!!sym(val_bid), na.rm=T), Booked = sum(!!sym(val_booked), na.rm=T)) %>%
        pivot_longer(c(Bid, Booked), names_to = "Type", values_to = "Value")
    }
    
    # 2. Base Plot
    p <- ggplot(d_trend, aes(x = date_group, y = Value, color = Type, group = Type,
                             text = paste0("<b>Date:</b> ", format(date_group, "%m-%d-%Y"), "<br>",
                                           "<b>Type:</b> ", Type, "<br>",
                                           "<b>Value:</b> ", if(prefix=="$") dollar(Value, accuracy=1) else comma(Value, accuracy=1)))) + 
      geom_line(size = 1) + 
      scale_color_manual(values = c("Bid" = "#2c3e50", "Booked" = "#27ae60")) + 
      scale_y_continuous(labels = if(prefix=="$") dollar_format(accuracy=1) else comma_format(accuracy=1)) + 
      theme_minimal() + labs(x="", y="", title=NULL)
    
    # 3. Apply Splitting Logic
    if(input$view_mode == "Individual") {
      # Add Faceting (Small Multiples)
      p <- p + facet_wrap(~primary_name, scales = "free_y") + 
        theme(strip.background = element_rect(fill="#f4f6f9"), strip.text = element_text(face="bold"))
    } else {
      # Add Regression only in Aggregate mode
      p <- p + geom_smooth(method = "lm", se = FALSE, linetype = "dashed", alpha = 0.5, size = 0.5)
    }
    
    ggplotly(p, tooltip = "text") %>% layout(legend = list(orientation = "h", x = 0.4, y = -0.2))
  }
  
  output$trend_dollars <- renderPlotly({ render_trend_chart("amt_bid", "amt_booked", "$") })
  output$trend_qty <- renderPlotly({ render_trend_chart("qty_bid", "qty_booked", "") })
  
  output$annual_bar_chart <- renderPlotly({
    d <- filtered_data(); validate(need(nrow(d) > 0, "No data.")); 
    # Logic: If Individual Mode OR (Aggregate Mode but < 6 people selected), show dodge.
    # Note: Annual chart always looks better grouped by person if few are selected.
    by_person <- !is.null(input$estimatorSelect) && length(input$estimatorSelect) <= 5
    
    if (by_person) { annual <- d %>% group_by(year_num, primary_name) %>% summarise(Booked = sum(amt_booked, na.rm=T)); p <- ggplot(annual, aes(x = factor(year_num), y = Booked, fill = primary_name)) + geom_col(position = "dodge") + scale_fill_brewer(palette = "Paired")
    } else { annual <- d %>% group_by(year_num) %>% summarise(Bid = sum(amt_bid, na.rm=T), Booked = sum(amt_booked, na.rm=T)) %>% pivot_longer(c(Bid, Booked), names_to = "Type", values_to = "Value"); p <- ggplot(annual, aes(x = factor(year_num), y = Value, fill = Type)) + geom_col(position = "dodge") + scale_fill_manual(values = c("Bid" = "#95a5a6", "Booked" = "#27ae60")) }
    ggplotly(p + scale_y_continuous(labels = dollar_format()) + labs(x="",y="",fill="") + theme_minimal())
  })
  
  # --- LOGIC FOR YOY (AGGREGATE vs INDIVIDUAL) ---
  output$yoy_chart <- renderPlotly({
    d <- filtered_data(); req(input$yoy_metric, input$view_mode); validate(need(nrow(d) > 0, "No data.")); metric_sym <- sym(input$yoy_metric)
    
    if(input$view_mode == "Individual") {
      # Individual Mode: Colored by Person, Side-by-Side bars
      validate(need(length(unique(d$primary_name)) <= 10, "For Individual YoY View, please select 10 or fewer estimators."))
      
      annual_growth <- d %>% group_by(year_num, primary_name) %>% summarise(Total_Value = sum(!!metric_sym, na.rm=T)) %>% arrange(year_num) %>% group_by(primary_name) %>% mutate(Growth_Pct = (Total_Value - lag(Total_Value)) / lag(Total_Value)) %>% filter(!is.na(Growth_Pct))
      
      p <- ggplot(annual_growth, aes(x = factor(year_num), y = Growth_Pct, fill = primary_name, 
                                     text = paste("Year:", year_num, "<br>Estimator:", primary_name, "<br>Growth:", percent(Growth_Pct, 0.1)))) +
        geom_col(position = "dodge") +
        scale_fill_brewer(palette = "Paired")
      
    } else {
      # Aggregate Mode: Red/Green Bars
      annual_growth <- d %>% group_by(year_num) %>% summarise(Total_Value = sum(!!metric_sym, na.rm=T)) %>% arrange(year_num) %>% mutate(Growth_Pct = (Total_Value - lag(Total_Value)) / lag(Total_Value)) %>% filter(!is.na(Growth_Pct))
      
      p <- ggplot(annual_growth, aes(x = factor(year_num), y = Growth_Pct, fill = Growth_Pct > 0, 
                                     text = paste("Year:", year_num, "<br>Growth:", percent(Growth_Pct, 0.1)))) +
        geom_col(show.legend = FALSE) +
        scale_fill_manual(values = c("TRUE" = "#27ae60", "FALSE" = "#c0392b"))
    }
    
    p <- p + geom_hline(yintercept = 0, color = "black") + scale_y_continuous(labels = percent_format()) + labs(x = "Year", y = "Growth %") + theme_minimal()
    ggplotly(p, tooltip = "text")
  })
  
  output$leaderboard_table <- renderDataTable({
    d <- filtered_data(); if(nrow(d) == 0) return(NULL)
    leaderboard <- d %>% group_by(primary_name) %>% summarise(Bids = sum(amt_bid, na.rm = TRUE), Booked = sum(amt_booked, na.rm = TRUE), Win_Rate = sum(amt_booked, na.rm = TRUE) / sum(amt_bid, na.rm = TRUE), Jobs_Won = sum(qty_booked, na.rm = TRUE), Avg_Bid_Size = mean(amt_bid, na.rm = TRUE)) %>% arrange(desc(Booked)) %>% mutate(Bids = dollar(Bids, accuracy = 1), Booked = dollar(Booked, accuracy = 1), Win_Rate = percent(Win_Rate, accuracy = 0.1), Avg_Bid_Size = dollar(Avg_Bid_Size, accuracy = 1)) %>% rename(Estimator = primary_name, `Total Revenue` = Booked, `Total Bids` = Bids, `Win %` = Win_Rate, `Jobs` = Jobs_Won, `Avg Job Size` = Avg_Bid_Size)
    datatable(leaderboard, options = list(dom = 't', pageLength = 10, scrollX = TRUE))
  })
  
  output$db_status_text <- renderText({ d <- get_data(); paste("Database records loaded:", comma(nrow(d))) })
  output$preview_table <- renderDataTable({ d <- get_data(); if(nrow(d)==0) return(NULL); d %>% arrange(desc(report_date)) %>% head(5) %>% select(Estimator=primary_name, Date=report_date, Bid=amt_bid, Booked=amt_booked) }, options = list(dom='t', searching=FALSE))
}

shinyApp(ui, server)