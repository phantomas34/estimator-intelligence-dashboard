# --- ui.R ---

dashboardPage(
  title = "Estimator Intelligence",
  skin  = "black",
  
  dashboardHeader(title = span("Estimator Intelligence",
                               style = "font-weight:bold;font-size:18px;")),
  
  dashboardSidebar(
    width = 260,
    sidebarMenu(
      menuItem("Top Brief",           tabName = "ceo_brief", icon = icon("briefcase")),
      menuItem("Executive Dashboard",   tabName = "dashboard", icon = icon("chart-area")),
      menuItem("Intervention Analysis", tabName = "its",       icon = icon("flask")),
      menuItem("Data Management",       tabName = "admin",     icon = icon("database")),
      menuItem("Client Intelligence", tabName = "client_intel", icon = icon("handshake")),
      hr(),
      div(style = "padding:10px;",
          h5("Filter Controls",
             style = "color:#b8c7ce;text-transform:uppercase;font-size:12px;margin-bottom:10px;"),
          
          dateRangeInput("dateRange", "View Range",
                         start    = Sys.Date() %m-% months(24),
                         end      = Sys.Date(),
                         separator = " to "),
          
          dateRangeInput("baselineRange", "SPC Baseline Period",
                         start    = Sys.Date() %m-% months(48),
                         end      = Sys.Date() %m-% months(25),
                         separator = " to "),
          
          radioGroupButtons("view_mode", "Breakdown Mode",
                            choices  = c("Aggregate", "Individual"),
                            selected = "Aggregate",
                            status   = "success", size = "sm", justified = TRUE),
          br(),
          
          radioGroupButtons("time_granularity", "Time Scale",
                            choices  = c("Monthly", "Quarterly"),
                            selected = "Monthly",
                            status   = "primary", size = "sm", justified = TRUE),
          br(),
          
          sliderInput("win_lag", "Win Rate Lag (months)",
                      min = 0, max = 6, value = 3, step = 1, ticks = FALSE),
          p(style = "font-size:10px;color:#b8c7ce;margin-top:-8px;",
            "Match bids to bookings N months later. 0 = same-month (old behavior)."),
          
          pickerInput("estimatorSelect", "Estimators:",
                      choices  = NULL, multiple = TRUE,
                      options  = list(
                        `actions-box`            = TRUE,
                        `live-search`            = TRUE,
                        `selected-text-format`   = "count > 2",
                        `count-selected-text`    = "{0} Selected",
                        `none-selected-text`     = "All Estimators (Default)"
                      )),
          
          div(style = "margin-top:12px;display:flex;gap:6px;",
              actionButton("refresh", "Refresh",
                           icon  = icon("sync"),
                           style = "flex:1;font-size:12px;",
                           class = "btn-primary btn-sm btn-flat"),
              actionButton("reset_filters", "Reset",
                           icon  = icon("undo"),
                           style = "flex:1;font-size:12px;",
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
      
      # ── CEO BRIEF ────────────────────────────────────────────────────────
      tabItem(tabName = "ceo_brief",
              fluidRow(
                box(title = "Top Brief Calibration & Export",
                    status = "primary", solidHeader = TRUE, width = 12,
                    fluidRow(
                      column(2, sliderInput("ceo_w_win", "Weight: Win Quality", min = 0, max = 100, value = 35, step = 1)),
                      column(2, sliderInput("ceo_w_eff", "Weight: Efficiency", min = 0, max = 100, value = 20, step = 1)),
                      column(2, sliderInput("ceo_w_stab", "Weight: Stability", min = 0, max = 100, value = 20, step = 1)),
                      column(2, sliderInput("ceo_w_out", "Weight: Output", min = 0, max = 100, value = 25, step = 1)),
                      column(2, numericInput("ceo_risk_win", "At-Risk Win % <", value = 12, min = 1, max = 100, step = 1)),
                      column(2, numericInput("ceo_risk_cv", "At-Risk Volatility >", value = 1.25, min = 0.1, max = 3, step = 0.05))
                    ),
                    fluidRow(
                      column(10, p(class = "text-muted", style = "font-size:11px;margin-top:6px;",
                                   "Weights auto-normalize to 100%. Win threshold is percentage points. Volatility is coefficient of variation for booked revenue.")),
                      column(2, div(style = "margin-top:8px;",
                                    downloadButton("download_top_brief", "Download Top Brief",
                                                   icon = icon("file-excel"),
                                                   style = "width:100%;font-weight:600;")))
                    )
                )
              ),
              
              fluidRow(
                valueBoxOutput("ceo_total_pred_90d", width = 3),
                valueBoxOutput("ceo_top_estimator",  width = 3),
                valueBoxOutput("ceo_at_risk_count",  width = 3),
                valueBoxOutput("ceo_model_quality",  width = 3)
              ),
              
              fluidRow(
                box(title = "90-Day Booked Revenue Forecast by Estimator",
                    status = "primary", solidHeader = TRUE, width = 7,
                    plotlyOutput("ceo_forecast_plot", height = "380px"),
                    p(class = "text-muted", style = "font-size:11px;",
                      "Forecast uses estimator-level monthly history with lag features and seasonality. Error bars show an approximate 80% interval from in-sample model error.")
                ),
                box(title = "Executive Action Summary",
                    status = "info", solidHeader = TRUE, width = 5,
                    uiOutput("ceo_action_summary"),
                    p(class = "text-muted", style = "font-size:11px;",
                      "Composite score combines win quality, efficiency, stability, and expected near-term revenue.")
                )
              ),
              
              fluidRow(
                box(title = "Estimator Performance Scorecard",
                    status = "warning", solidHeader = TRUE, width = 12,
                    dataTableOutput("ceo_scorecard_table"))
              )
      ),
      
      # ── EXECUTIVE DASHBOARD ──────────────────────────────────────────────
      tabItem(tabName = "dashboard",
              
              # ROW 1: KPIs
              fluidRow(
                valueBoxOutput("kpi_total_bid",      width = 3),
                valueBoxOutput("kpi_total_booked",   width = 3),
                valueBoxOutput("kpi_win_rate_amt",   width = 3),
                valueBoxOutput("kpi_lag_win_rate",   width = 3)
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
                                                           status = "default", size = "xs")
                                  ),
                                  column(6,
                                         materialSwitch("show_trendline",
                                                        label  = "Trend Lines (Individual)",
                                                        value  = FALSE,
                                                        status = "primary",
                                                        right  = TRUE)
                                  )
                                ),
                                plotlyOutput("trend_dollars", height = "370px"),
                                p(class = "text-muted", style = "font-size:11px;",
                                  "SPC: Blue = Baseline Mean. Red dashed = UCL/LCL (\u00b13 SD from Baseline Period). Red dots = anomalies. Trend line shows linear trajectory per estimator (Individual mode only).")),
                       
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
                    fluidRow(
                      column(12,
                             radioGroupButtons("annual_granularity", NULL,
                                               choices  = c("By Year" = "year", "By Quarter" = "quarter", "By Month" = "month"),
                                               selected = "year",
                                               status = "default", size = "xs", justified = TRUE)
                      )
                    ),
                    plotlyOutput("annual_bar_chart", height = "370px")),
                
                tabBox(title = "Growth & Breakdown", width = 6, side = "right",
                       selected = "YoY Growth (%)",
                       
                       tabPanel("YoY Growth (%)",
                                fluidRow(
                                  column(4, p(class = "text-muted", style = "font-size:11px;margin-top:10px;",
                                              "Aggregate: Red/Green | Individual: Colored by Person")),
                                  column(4, pickerInput("yoy_metric", NULL,
                                                        choices  = c("Booked Revenue ($)"  = "amt_booked",
                                                                     "Bid Volume ($)"      = "amt_bid",
                                                                     "Jobs Booked (Qty)"   = "qty_booked",
                                                                     "Jobs Bid (Qty)"      = "qty_bid"),
                                                        selected = "amt_booked", width = "100%",
                                                        options  = list(style = "btn-default btn-sm"))),
                                  column(4, radioGroupButtons("yoy_position", NULL,
                                                              choices  = c("Grouped" = "dodge", "Stacked" = "stack"),
                                                              selected = "dodge",
                                                              status = "default", size = "xs", justified = TRUE))
                                ),
                                plotlyOutput("yoy_chart", height = "330px")),
                       
                       tabPanel("Quarterly Deep Dive",
                                fluidRow(
                                  column(7, p(class = "text-muted", style = "font-size:11px;margin-top:10px;",
                                              "Revenue Breakdown by Quarter for Selected Year.")),
                                  column(5, selectInput("quarter_year_select", NULL, choices = NULL, width = "100%"))
                                ),
                                plotlyOutput("quarterly_breakdown_chart", height = "350px"))
                )
              ),
              
              # ROW 5: ADVANCED ANALYTICS
              fluidRow(
                tabBox(title = "Advanced Analytics", width = 12, side = "right",
                       selected = "Bid Funnel",
                       
                       tabPanel("Bid Funnel",
                                plotlyOutput("bid_funnel_chart", height = "420px"),
                                p(class = "text-muted", style = "font-size:11px;",
                                  "Grouped bars show Total Bid $ vs. Total Booked $ per estimator. Win % annotated on each booked bar. Sorted by Total Bid descending.")),
                       
                       tabPanel("Seasonality Heatmap",
                                fluidRow(
                                  column(12,
                                         radioGroupButtons(
                                           "heatmap_metric",
                                           label    = NULL,
                                           choices  = c("Booked Revenue" = "amt_booked", "Bid Volume" = "amt_bid"),
                                           selected = "amt_booked",
                                           status   = "default",
                                           size     = "sm"
                                         )
                                  )
                                ),
                                plotlyOutput("seasonality_heatmap", height = "400px"),
                                p(class = "text-muted", style = "font-size:11px;",
                                  "Revenue or bid volume by calendar month and year. Darker = higher value. Reveals consistent seasonal peaks and troughs across the full history."))
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
                box(title = "Estimator Leaderboard \u2014 Click any column to sort",
                    status = "warning", solidHeader = TRUE, width = 12,
                    dataTableOutput("leaderboard_table"))
              )
      ),
      
      # ── INTERVENTION ANALYSIS (ITS) ──────────────────────────────────────
      tabItem(tabName = "its",
              
              fluidRow(
                box(title = "Intervention Settings", status = "primary",
                    solidHeader = TRUE, width = 4,
                    
                    pickerInput("its_estimator", "Estimator",
                                choices = NULL, multiple = FALSE,
                                options = list(`live-search` = TRUE)),
                    
                    pickerInput("its_metric", "Outcome Metric",
                                choices  = c("Booked Revenue ($)"    = "amt_booked",
                                             "Bid Volume ($)"        = "amt_bid",
                                             "Jobs Booked (Qty)"     = "qty_booked",
                                             "Win Rate (same-mo.)"   = "win_rate"),
                                selected = "amt_booked"),
                    hr(),
                    
                    dateInput("its_manual_date", "Option A \u2014 Specify Intervention Date",
                              value  = NULL, format = "M yyyy",
                              min    = "2010-01-01"),
                    p(class = "text-muted", style = "font-size:11px;",
                      "The date a strategy change, new hire, process shift, or other intervention occurred."),
                    hr(),
                    
                    materialSwitch("its_autodetect",
                                   label  = "Option B \u2014 Auto-Detect Breakpoints",
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
                
                box(title = "Intervention Effect \u2014 Statistical Summary",
                    status = "primary", solidHeader = TRUE, width = 8,
                    uiOutput("its_summary_ui"),
                    p(class = "text-muted", style = "font-size:11px; margin-top:8px;",
                      "Segmented regression (OLS) with pre/post intervention terms. Level change = immediate shift at intervention date. Slope change = difference in monthly trajectory before vs. after.")
                )
              ),
              
              fluidRow(
                box(title = "Interrupted Time Series \u2014 Visual",
                    status = "primary", solidHeader = TRUE, width = 12,
                    plotlyOutput("its_chart", height = "440px"),
                    p(class = "text-muted", style = "font-size:11px;",
                      "Grey points = actual monthly values. Blue line = pre-intervention fitted trend. Green line = post-intervention fitted trend. Vertical dashed line = intervention point. Red dotted = counterfactual.")
                )
              ),
              
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
                      "Download a formatted Excel workbook of the current filtered view."),
                    downloadButton("download_report", "Download Excel Report",
                                   icon  = icon("file-excel"),
                                   style = "width:100%;font-weight:600;"))
              ),
              
              fluidRow(
                box(title = "Date Verification \u2014 Raw vs. Parsed",
                    status = "warning", solidHeader = TRUE, width = 12,
                    p(class = "text-muted", style = "font-size:12px;",
                      "Confirms MoYear values are parsed correctly. Format: YY-MM (e.g. '19-06' \u2192 June 2019)."),
                    dataTableOutput("date_verification_table"))
              )
      ),
      tabItem(tabName = "client_intel",
              
              # ── HEADER NOTE ────────────────────────────────────────────────────────────
              fluidRow(
                box(
                  width = 12, status = "info", solidHeader = FALSE,
                  div(style = "padding:4px 0;",
                      p(style = "margin:0; font-size:13px; color:#2c3e50;",
                        icon("flask"), " ",
                        tags$b("Experimental Tab — "),
                        "Powered by IMS bid follow-up records. Shows contractor relationships,
           pipeline activity, and award signals extracted from estimator field notes.
           Data source: ", tags$code("bid___follow_up"), " joined to ",
                        tags$code("estimators"), "."
                      )
                  )
                )
              ),
              
              # ── ROW 1: KPIs ────────────────────────────────────────────────────────────
              fluidRow(
                valueBoxOutput("ci_kpi_followups",   width = 3),
                valueBoxOutput("ci_kpi_contractors", width = 3),
                valueBoxOutput("ci_kpi_top_est",     width = 3),
                valueBoxOutput("ci_kpi_date_range",  width = 3)
              ),
              
              # ── ROW 2: FILTERS ─────────────────────────────────────────────────────────
              fluidRow(
                box(
                  width = 12, status = "primary", solidHeader = TRUE,
                  title = "Filters",
                  fluidRow(
                    column(4,
                           pickerInput(
                             "ci_estimator", "Estimator",
                             choices  = NULL, multiple = TRUE,
                             options  = list(
                               `actions-box`          = TRUE,
                               `live-search`          = TRUE,
                               `selected-text-format` = "count > 2",
                               `count-selected-text`  = "{0} Selected",
                               `none-selected-text`   = "All Estimators"
                             )
                           )
                    ),
                    column(4,
                           dateRangeInput(
                             "ci_date_range", "Date Range",
                             start     = Sys.Date() %m-% months(36),
                             end       = Sys.Date(),
                             separator = " to "
                           )
                    ),
                    column(4,
                           pickerInput(
                             "ci_contractor", "Contractor (GC)",
                             choices  = NULL, multiple = TRUE,
                             options  = list(
                               `actions-box`          = TRUE,
                               `live-search`          = TRUE,
                               `selected-text-format` = "count > 2",
                               `none-selected-text`   = "All Contractors"
                             )
                           )
                    )
                  )
                )
              ),
              
              # ── ROW 3: TOP CONTRACTORS + ACTIVITY TIMELINE ─────────────────────────────
              fluidRow(
                box(
                  title = "Top Contractors by Follow-Up Activity",
                  status = "primary", solidHeader = TRUE, width = 6,
                  fluidRow(
                    column(6,
                           radioGroupButtons(
                             "ci_contractor_metric", NULL,
                             choices  = c("Follow-Ups" = "followups", "Estimators" = "estimators"),
                             selected = "followups",
                             status = "default", size = "xs", justified = TRUE
                           )
                    ),
                    column(6,
                           sliderInput(
                             "ci_top_n", "Show Top N",
                             min = 5, max = 25, value = 15, step = 5, ticks = FALSE
                           )
                    )
                  ),
                  plotlyOutput("ci_contractor_chart", height = "380px"),
                  p(class = "text-muted", style = "font-size:11px;",
                    "Bars show total follow-up contacts per GC across all selected estimators.
         'Estimators' mode shows how many different estimators have contacted each GC.")
                ),
                
                box(
                  title = "Follow-Up Activity Timeline",
                  status = "primary", solidHeader = TRUE, width = 6,
                  fluidRow(
                    column(6,
                           radioGroupButtons(
                             "ci_timeline_grain", NULL,
                             choices  = c("Monthly" = "month", "Quarterly" = "quarter"),
                             selected = "month",
                             status = "default", size = "xs", justified = TRUE
                           )
                    ),
                    column(6,
                           radioGroupButtons(
                             "ci_timeline_mode", NULL,
                             choices  = c("Aggregate" = "agg", "By Estimator" = "ind"),
                             selected = "agg",
                             status = "default", size = "xs", justified = TRUE
                           )
                    )
                  ),
                  plotlyOutput("ci_timeline_chart", height = "380px"),
                  p(class = "text-muted", style = "font-size:11px;",
                    "Monthly or quarterly count of follow-up contacts logged.
         Reveals pipeline activity intensity over time.")
                )
              ),
              
              # ── ROW 4: ESTIMATOR × CONTRACTOR MATRIX + AWARD FEED ─────────────────────
              fluidRow(
                box(
                  title = "Estimator \u00d7 Contractor Relationship Matrix",
                  status = "primary", solidHeader = TRUE, width = 5,
                  plotlyOutput("ci_matrix_chart", height = "400px"),
                  p(class = "text-muted", style = "font-size:11px;",
                    "Heatmap of follow-up contacts. Darker = stronger relationship.
         Reveals which estimators own which GC relationships.")
                ),
                
                box(
                  title = "Award Signal Feed \u2014 Notes containing win/award language",
                  status = "warning", solidHeader = TRUE, width = 7,
                  fluidRow(
                    column(12,
                           p(style = "font-size:12px; color:#5A6A72; margin-bottom:6px;",
                             "Filtered for notes containing: ",
                             tags$b("awarded, booked, proceed, LOI, notice to proceed, doing this project, re-awarded")
                           )
                    )
                  ),
                  dataTableOutput("ci_award_feed"),
                  p(class = "text-muted", style = "font-size:11px; margin-top:6px;",
                    "Award signals extracted from IMS follow-up notes. Use as a win log proxy
         until full bid outcome data is available.")
                )
              )
      )
      
      
    )
  )
)