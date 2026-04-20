# --- ui.R ---
dashboardPage(
  title = "Estimator Intelligence",
  skin  = "black",
  
  dashboardHeader(title = span("Estimator Intelligence", style = "font-weight:bold;font-size:18px;")),
  
  dashboardSidebar(
    width = 260,
    sidebarMenu(
      menuItem("Executive Dashboard", tabName = "dashboard", icon = icon("chart-area")),
      menuItem("Data Management",     tabName = "admin",     icon = icon("database")),
      hr(),
      
      div(style = "padding:10px;",
          h5("Filter Controls", style = "color:#b8c7ce;text-transform:uppercase;font-size:12px;margin-bottom:10px;"),
          
          dateRangeInput("dateRange", "View Range", start = Sys.Date() %m-% months(24), end = Sys.Date(), separator = " to "),
          dateRangeInput("baselineRange", "SPC Baseline Period", start = Sys.Date() %m-% months(48), end = Sys.Date() %m-% months(25), separator = " to "),
          
          radioGroupButtons("view_mode", "Breakdown Mode", choices = c("Aggregate", "Individual"), selected = "Aggregate", status = "success", size = "sm", justified = TRUE),
          materialSwitch(inputId = "show_trend", label = "Show Individual Trend Lines", status = "primary", right = TRUE),
          br(),
          
          radioGroupButtons("time_granularity", "Time Scale", choices = c("Monthly", "Quarterly"), selected = "Monthly", status = "primary", size = "sm", justified = TRUE),
          br(),
          
          sliderInput("win_lag", "Win Rate Lag (months)", min = 0, max = 6, value = 3, step = 1, ticks = FALSE),
          p(style = "font-size:10px;color:#b8c7ce;margin-top:-8px;", "Match bids to bookings N months later."),
          
          pickerInput("estimatorSelect", "Estimators:", choices = NULL, multiple = TRUE,
                      options = list(`actions-box` = TRUE, `live-search` = TRUE, `selected-text-format` = "count > 2", `count-selected-text` = "{0} Selected", `none-selected-text` = "All Estimators (Default)")),
          
          div(style = "margin-top:12px;display:flex;gap:6px;",
              actionButton("refresh", "Refresh", icon = icon("sync"), style = "flex:1;font-size:12px;", class = "btn-primary btn-sm btn-flat"),
              actionButton("reset_filters", "Reset", icon = icon("undo"), style = "flex:1;font-size:12px;", class = "btn-warning btn-sm btn-flat")
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
      tabItem(tabName = "dashboard",
              fluidRow(
                valueBoxOutput("kpi_total_bid", width = 3), valueBoxOutput("kpi_total_booked", width = 3),
                valueBoxOutput("kpi_win_rate_amt", width = 3), valueBoxOutput("kpi_lag_win_rate", width = 3)
              ),
              fluidRow(box(title = "Data Science Insight: Pipeline Correlation", status = "info", solidHeader = TRUE, width = 12, uiOutput("correlation_insight"))),
              fluidRow(
                tabBox(title = textOutput("trend_box_title"), width = 12, side = "right", selected = "Control Chart: Revenue ($)",
                       tabPanel("Control Chart: Revenue ($)",
                                fluidRow(column(12, radioGroupButtons("trend_series", NULL, choices = c("Booked Only" = "booked", "Bid vs. Booked" = "both"), selected = "booked", status = "default", size = "xs"))),
                                plotlyOutput("trend_dollars", height = "380px"),
                                p(class = "text-muted", style = "font-size:11px;", "SPC: Blue line = Baseline Mean. Red dashed = UCL/LCL. Red dots = anomalies.")),
                       tabPanel("Control Chart: Quantity", plotlyOutput("trend_qty", height = "400px"))
                )
              ),
              fluidRow(
                box(title = "Annual Performance", status = "primary", solidHeader = TRUE, width = 6,
                    fluidRow(column(12, radioGroupButtons("annual_granularity", NULL, choices = c("By Year" = "year", "By Quarter" = "quarter", "By Month" = "month"), selected = "year", status = "default", size = "xs", justified = TRUE))),
                    plotlyOutput("annual_bar_chart", height = "370px")),
                tabBox(title = "Growth & Breakdown", width = 6, side = "right", selected = "YoY Growth (%)",
                       tabPanel("YoY Growth (%)",
                                fluidRow(
                                  column(4, p(class="text-muted", style="font-size:11px;margin-top:10px;", "Aggregate: Red/Green | Individual: Colored by Person")),
                                  column(4, pickerInput("yoy_metric", NULL, choices = c("Booked Revenue ($)"="amt_booked", "Bid Volume ($)"="amt_bid", "Jobs Booked (Qty)"="qty_booked", "Jobs Bid (Qty)"="qty_bid"), selected = "amt_booked", width = "100%", options = list(style = "btn-default btn-sm"))),
                                  column(4, radioGroupButtons("yoy_position", NULL, choices = c("Grouped"="dodge", "Stacked"="stack"), selected = "dodge", status = "default", size = "xs", justified = TRUE))
                                ),
                                plotlyOutput("yoy_chart", height = "330px")),
                       tabPanel("Quarterly Deep Dive",
                                fluidRow(column(7, p(class="text-muted", style="font-size:11px;margin-top:10px;", "Revenue Breakdown by Quarter.")), column(5, selectInput("quarter_year_select", NULL, choices = NULL, width = "100%"))),
                                plotlyOutput("quarterly_breakdown_chart", height = "350px"))
                )
              ),
              fluidRow(
                tabBox(title = "Advanced Analytics", width = 12, side = "right", selected = "Bid Funnel",
                       tabPanel("Bid Funnel", plotlyOutput("bid_funnel_chart", height = "420px")),
                       tabPanel("Seasonality Heatmap",
                                # CEO TWEAK: Added the Bid vs Booked toggle for the Heatmap
                                fluidRow(column(12, radioGroupButtons("heatmap_metric", NULL, choices = c("Booked Revenue" = "amt_booked", "Bid Volume" = "amt_bid"), selected = "amt_booked", status = "default", size = "xs"))),
                                plotlyOutput("seasonality_heatmap", height = "390px"),
                                p(class = "text-muted", style = "font-size:11px;", "Reveals consistent seasonal peaks and troughs across the full history."))
                )
              ),
              fluidRow(box(title = "K-Means Behavioral Segmentation", status = "primary", solidHeader = TRUE, width = 12, plotlyOutput("scatter_matrix", height = "400px"))),
              fluidRow(box(title = "Estimator Leaderboard", status = "warning", solidHeader = TRUE, width = 12, dataTableOutput("leaderboard_table")))
      ),
      
      tabItem(tabName = "admin",
              fluidRow(
                box(title = "Upload Data", status = "danger", solidHeader = TRUE, width = 5,
                    fileInput("file1", "Upload CSV", accept = ".csv"),
                    actionButton("process_upload", "Process & Import", class = "btn-danger", width = "100%")),
                box(title = "System Status", status = "info", solidHeader = TRUE, width = 4,
                    verbatimTextOutput("db_status_text"), dataTableOutput("preview_table")),
                box(title = "Export Report", status = "primary", solidHeader = TRUE, width = 3,
                    p(class = "text-muted", "Download a dynamic HTML Executive Summary. (Open in browser and Save as PDF)."),
                    downloadButton("download_report", "Download HTML Report", style = "width:100%;font-weight:600;"))
              ),
              fluidRow(
                box(title = "Date Verification — Raw vs. Parsed", status = "warning", solidHeader = TRUE, width = 12,
                    p(class = "text-muted", style = "font-size:12px;", "Confirms MoYear values are parsed correctly. Format: YY-MM (e.g. '19-06' → June 2019)."),
                    dataTableOutput("date_verification_table"))
              )
      )
    )
  )
)