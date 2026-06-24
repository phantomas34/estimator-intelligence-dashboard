# =============================================================================
# MODULE: Cohort Win Rate Analysis
# File:   mod_cohort_analysis.R
#
# Drop-in Shiny module for the Estimator Intelligence Dashboard.
# Measures how much work bid in a given period is eventually rewarded,
# regardless of when the booking is recorded.
#
# Usage in ui.R    → tabItem(tabName = "cohort", cohortAnalysisUI("cohort"))
# Usage in server.R → cohortAnalysisServer("cohort", get_data = get_data)
#
# Pass `get_data` — the UNFILTERED pool reactive — so the module has full
# history for forward attribution windows beyond the sidebar date range.
# Expected columns: primary_name, report_date (Date), amt_bid, qty_bid,
#                   amt_booked, qty_booked
# =============================================================================


# -----------------------------------------------------------------------------
# UI
# -----------------------------------------------------------------------------

cohortAnalysisUI <- function(id) {
  ns <- NS(id)
  
  tagList(
    
    # --- Control Row ---
    fluidRow(
      box(
        title       = "Cohort Parameters",
        status      = "primary",
        solidHeader = TRUE,
        width       = 12,
        collapsible = TRUE,
        
        fluidRow(
          column(3,
                 dateRangeInput(
                   ns("cohort_range"),
                   label     = "Bid Cohort Window",
                   start     = floor_date(Sys.Date() %m-% months(12), "month"),
                   end       = floor_date(Sys.Date() %m-% months(1),  "month"),
                   format    = "M yyyy",
                   startview = "year"
                 ),
                 p(class = "text-muted", style = "font-size:11px; margin-top:-6px;",
                   "Define the period during which bids were submitted.")
          ),
          column(3,
                 sliderInput(
                   ns("forward_window"),
                   label = "Forward Attribution Window (months)",
                   min   = 1,
                   max   = 18,
                   value = 9,
                   step  = 1,
                   ticks = FALSE,
                   post  = " mo"
                 ),
                 p(class = "text-muted", style = "font-size:11px; margin-top:-6px;",
                   "How many months past the cohort end to capture delayed bookings.")
          ),
          column(3,
                 pickerInput(
                   ns("estimator_filter"),
                   label    = "Estimators:",
                   choices  = NULL,
                   multiple = TRUE,
                   options  = list(
                     `actions-box`          = TRUE,
                     `live-search`          = TRUE,
                     `selected-text-format` = "count > 2",
                     `count-selected-text`  = "{0} Selected",
                     `none-selected-text`   = "All Estimators"
                   )
                 )
          ),
          column(3,
                 br(),
                 actionButton(ns("run_cohort"), "Calculate Cohort",
                              icon  = icon("calculator"),
                              class = "btn-success btn-block",
                              width = "100%")
          )
        )
      )
    ),
    
    # --- KPI Row ---
    fluidRow(
      valueBoxOutput(ns("box_total_bid"),    width = 3),
      valueBoxOutput(ns("box_total_booked"), width = 3),
      valueBoxOutput(ns("box_win_rate_amt"), width = 3),
      valueBoxOutput(ns("box_win_rate_qty"), width = 3)
    ),
    
    # --- Chart Row ---
    fluidRow(
      box(
        title       = "Cohort Win Rate vs. Same-Month Win Rate by Estimator",
        status      = "primary",
        solidHeader = TRUE,
        width       = 8,
        plotlyOutput(ns("comparison_chart"), height = "380px"),
        p(class = "text-muted", style = "font-size:11px;",
          "Blue = cohort win rate (bookings attributed forward across the full window).
           Orange = same-month win rate (bookings recorded within the cohort window only).
           A large positive gap means delayed wins are real — they just don't show up in-period.")
      ),
      box(
        title       = "Attribution Window Sensitivity",
        status      = "warning",
        solidHeader = TRUE,
        width       = 4,
        plotlyOutput(ns("sensitivity_chart"), height = "340px"),
        p(class = "text-muted", style = "font-size:11px;",
          "Aggregate cohort win rate at each forward window length.
           The plateau marks your empirical bid-to-close lag.")
      )
    ),
    
    # --- Table Row ---
    fluidRow(
      box(
        title       = "Cohort Detail by Estimator",
        status      = "warning",
        solidHeader = TRUE,
        width       = 12,
        dataTableOutput(ns("cohort_table")),
        p(class = "text-muted", style = "font-size:11px; margin-top:6px;",
          "Lag Gap = Cohort Win Rate minus Same-Month Win Rate.
           Green = bookings trailing bids (work is being rewarded, just later).
           Red = same-month rate exceeds cohort rate (bookings drawing from older pipelines).")
      )
    ),
    
    # --- Reading Guide ---
    fluidRow(
      box(
        title       = "Reading This Analysis",
        status      = "info",
        solidHeader = FALSE,
        width       = 12,
        collapsible = TRUE,
        collapsed   = TRUE,
        tags$p(HTML(
          "<b>Cohort Win Rate</b> answers: of every dollar bid during the selected window,
           how much eventually converted — even if the booking appeared in a later month?"
        )),
        tags$p(HTML(
          "<b>Same-Month Win Rate</b> (existing dashboard metric) answers: in the same period
           the bids were submitted, what was the ratio of bookings recorded to bids?
           These two figures diverge whenever your bid-to-award cycle spans multiple months."
        )),
        tags$p(HTML(
          "The <b>sensitivity curve</b> sweeps the forward window from 0 to 18 months.
           The plateau point is your approximate bid-to-close lag — where additional
           window months stop adding meaningful bookings. That number is useful context for
           the President/CEO: it's the lag between effort and outcome in the pipeline."
        ))
      )
    )
    
  )
}


# -----------------------------------------------------------------------------
# Server
# -----------------------------------------------------------------------------

cohortAnalysisServer <- function(id, get_data) {
  moduleServer(id, function(input, output, session) {
    
    ns <- session$ns
    
    # --- Populate estimator picker from live pool ---
    observe({
      df <- get_data()
      req(nrow(df) > 0)
      estimators <- sort(unique(df$primary_name))
      updatePickerInput(session, "estimator_filter",
                        choices  = estimators,
                        selected = estimators)
    })
    
    # --- Core computation helper ---
    # Returns per-estimator cohort stats for a given forward attribution window.
    compute_cohort <- function(df, cohort_start, cohort_end, forward_months) {
      
      attribution_end <- cohort_end %m+% months(forward_months)
      
      # Bids placed within the cohort window
      bids_in_cohort <- df %>%
        filter(report_date >= cohort_start, report_date <= cohort_end) %>%
        group_by(primary_name) %>%
        summarise(
          cohort_amt_bid = sum(amt_bid,  na.rm = TRUE),
          cohort_qty_bid = sum(qty_bid,  na.rm = TRUE),
          .groups = "drop"
        )
      
      # Bookings recorded from cohort_start through attribution_end (full window)
      bookings_forward <- df %>%
        filter(report_date >= cohort_start, report_date <= attribution_end) %>%
        group_by(primary_name) %>%
        summarise(
          cohort_amt_booked = sum(amt_booked, na.rm = TRUE),
          cohort_qty_booked = sum(qty_booked, na.rm = TRUE),
          .groups = "drop"
        )
      
      # Same-month baseline: bookings only within cohort_start → cohort_end
      bookings_same_mo <- df %>%
        filter(report_date >= cohort_start, report_date <= cohort_end) %>%
        group_by(primary_name) %>%
        summarise(
          sm_amt_booked = sum(amt_booked, na.rm = TRUE),
          sm_qty_booked = sum(qty_booked, na.rm = TRUE),
          .groups = "drop"
        )
      
      bids_in_cohort %>%
        left_join(bookings_forward,  by = "primary_name") %>%
        left_join(bookings_same_mo,  by = "primary_name") %>%
        mutate(
          cohort_win_rate_amt = cohort_amt_booked / cohort_amt_bid,
          cohort_win_rate_qty = cohort_qty_booked / cohort_qty_bid,
          sm_win_rate_amt     = sm_amt_booked     / cohort_amt_bid,
          sm_win_rate_qty     = sm_qty_booked     / cohort_qty_bid,
          lag_gap_amt         = cohort_win_rate_amt - sm_win_rate_amt
        )
    }
    
    # --- Reactive result — fires on button click ---
    cohort_result <- eventReactive(input$run_cohort, {
      df           <- get_data()
      cohort_start <- as.Date(input$cohort_range[1])
      cohort_end   <- as.Date(input$cohort_range[2])
      
      validate(
        need(nrow(df) > 0, "No data available."),
        need(cohort_start < cohort_end, "Cohort start must be before cohort end.")
      )
      
      sel <- input$estimator_filter
      if (!is.null(sel) && length(sel) > 0) {
        df <- df %>% filter(primary_name %in% sel)
      }
      
      compute_cohort(df, cohort_start, cohort_end, input$forward_window)
    }, ignoreNULL = FALSE)
    
    
    # --- KPI Value Boxes ---
    output$box_total_bid <- renderValueBox({
      r <- cohort_result(); req(r)
      valueBox(
        value    = dollar(sum(r$cohort_amt_bid, na.rm = TRUE), accuracy = 1),
        subtitle = "Total Bid in Cohort",
        icon     = icon("file-invoice-dollar"),
        color    = "blue"
      )
    })
    
    output$box_total_booked <- renderValueBox({
      r <- cohort_result(); req(r)
      valueBox(
        value    = dollar(sum(r$cohort_amt_booked, na.rm = TRUE), accuracy = 1),
        subtitle = "Attributed Bookings",
        icon     = icon("check-circle"),
        color    = "green"
      )
    })
    
    output$box_win_rate_amt <- renderValueBox({
      r <- cohort_result(); req(r)
      rate <- sum(r$cohort_amt_booked, na.rm = TRUE) /
        sum(r$cohort_amt_bid,    na.rm = TRUE)
      valueBox(
        value    = percent(rate, accuracy = 0.1),
        subtitle = HTML("Cohort Win Rate ($)<br><small style='font-size:10px;opacity:0.75;'>Forward-attributed</small>"),
        icon     = icon("trophy"),
        color    = "yellow"
      )
    })
    
    output$box_win_rate_qty <- renderValueBox({
      r <- cohort_result(); req(r)
      rate <- sum(r$cohort_qty_booked, na.rm = TRUE) /
        sum(r$cohort_qty_bid,    na.rm = TRUE)
      valueBox(
        value    = percent(rate, accuracy = 0.1),
        subtitle = HTML("Cohort Win Rate (Jobs)<br><small style='font-size:10px;opacity:0.75;'>Forward-attributed</small>"),
        icon     = icon("hard-hat"),
        color    = "purple"
      )
    })
    
    
    # --- Comparison Bar Chart ---
    output$comparison_chart <- renderPlotly({
      r <- cohort_result()
      validate(need(!is.null(r) && nrow(r) > 0,
                    "Click 'Calculate Cohort' to run the analysis."))
      
      plot_data <- r %>%
        select(primary_name,
               `Cohort (Forward)` = cohort_win_rate_amt,
               `Same-Month`       = sm_win_rate_amt) %>%
        tidyr::pivot_longer(-primary_name,
                            names_to  = "metric",
                            values_to = "win_rate") %>%
        mutate(
          primary_name = reorder(primary_name, win_rate, FUN = max),
          win_rate     = pmin(win_rate, 2)   # cap display at 200%
        )
      
      p <- ggplot(plot_data,
                  aes(x    = primary_name,
                      y    = win_rate,
                      fill = metric,
                      text = paste0("<b>", primary_name, "</b><br>",
                                    metric, ": ", percent(win_rate, 0.1)))) +
        geom_col(position = position_dodge(width = 0.7),
                 width = 0.6, alpha = 0.9) +
        geom_hline(yintercept = 1, linetype = "dashed",
                   color = "#95a5a6", linewidth = 0.5) +
        scale_y_continuous(
          labels = percent_format(accuracy = 1),
          expand = expansion(mult = c(0, 0.12))
        ) +
        scale_fill_manual(values = c("Cohort (Forward)" = "#3c8dbc",
                                     "Same-Month"       = "#f39c12")) +
        coord_flip() +
        labs(
          x       = NULL,
          y       = "Win Rate ($ Value)",
          fill    = NULL,
          caption = paste0(
            "Cohort: ", format(as.Date(input$cohort_range[1]), "%b %Y"),
            " - ",  format(as.Date(input$cohort_range[2]), "%b %Y"),
            "  |  Attribution window: +", input$forward_window, " months"
          )
        ) +
        theme_minimal() +
        theme(
          legend.position    = "top",
          panel.grid.major.y = element_blank(),
          plot.caption       = element_text(size = 9, color = "#95a5a6")
        )
      
      ggplotly(p, tooltip = "text") %>%
        layout(legend = list(orientation = "h", x = 0, y = 1.08))
    })
    
    
    # --- Sensitivity Curve ---
    # Sweeps forward window 0–18 months; shows plateau = empirical lag
    output$sensitivity_chart <- renderPlotly({
      df           <- get_data()
      cohort_start <- as.Date(input$cohort_range[1])
      cohort_end   <- as.Date(input$cohort_range[2])
      req(nrow(df) > 0, cohort_start, cohort_end)
      
      sel <- input$estimator_filter
      if (!is.null(sel) && length(sel) > 0) df <- df %>% filter(primary_name %in% sel)
      
      sensitivity <- purrr::map_dfr(0:18, function(m) {
        r <- compute_cohort(df, cohort_start, cohort_end, m)
        tibble(
          forward_months = m,
          win_rate = sum(r$cohort_amt_booked, na.rm = TRUE) /
            sum(r$cohort_amt_bid,    na.rm = TRUE)
        )
      })
      
      current_m <- input$forward_window
      
      p <- ggplot(sensitivity,
                  aes(x = forward_months, y = win_rate)) +
        geom_line(color = "#3c8dbc", linewidth = 1) +
        geom_point(aes(text = paste0(forward_months, " mo: ",
                                     percent(win_rate, 0.1))),
                   color = "#3c8dbc", size = 2) +
        geom_vline(xintercept = current_m,
                   linetype = "dashed", color = "#f39c12", linewidth = 0.7) +
        annotate("text",
                 x     = current_m + 0.4,
                 y     = max(sensitivity$win_rate, na.rm = TRUE) * 0.95,
                 label = paste0(current_m, " mo"),
                 color = "#f39c12", hjust = 0, size = 3.5) +
        scale_x_continuous(breaks = seq(0, 18, 3)) +
        scale_y_continuous(labels = percent_format(accuracy = 1)) +
        labs(x = "Forward Window (months)", y = "Aggregate Win Rate") +
        theme_minimal()
      
      ggplotly(p, tooltip = "text")
    })
    
    
    # --- Detail Table ---
    # FIXED: Replaced Unicode "\u2013" with standard hyphens "-" inside backticks.
    output$cohort_table <- renderDataTable({
      r <- cohort_result()
      validate(need(!is.null(r) && nrow(r) > 0,
                    "Run the analysis to populate this table."))
      
      display <- r %>%
        arrange(desc(cohort_win_rate_amt)) %>%
        transmute(
          Estimator                = primary_name,
          `Bid ($)`                = dollar(cohort_amt_bid,    accuracy = 1),
          `Bid (Jobs)`             = cohort_qty_bid,
          `Booked - Cohort ($)`    = dollar(cohort_amt_booked, accuracy = 1),
          `Booked - Cohort (Jobs)` = cohort_qty_booked,
          `Win Rate - Cohort`      = percent(cohort_win_rate_amt, accuracy = 0.1),
          `Win Rate - Same Mo.`    = percent(sm_win_rate_amt,     accuracy = 0.1),
          `Lag Gap`                = percent(lag_gap_amt,          accuracy = 0.1)
        )
      
      datatable(
        display,
        rownames = FALSE,
        options  = list(
          pageLength = 15,
          dom        = "tp",
          scrollX    = TRUE,
          order      = list(list(5, "desc"))
        )
      ) %>%
        formatStyle(
          "Lag Gap",
          color = styleInterval(
            c(-0.001, 0.001),
            c("#c0392b", "#888888", "#27ae60")
          )
        )
    })
    
  })
}