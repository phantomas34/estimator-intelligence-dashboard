# --- server.R ---
function(input, output, session) {
  
  data_trigger <- reactiveVal(0)
  
  # ── 1. FAST DATA FETCHING (VIA POOL) ──────────────────────────────────────
  get_data <- reactive({
    input$refresh
    data_trigger()
    
    # We ask the pool if the table exists. It handles the connection automatically.
    if (!dbExistsTable(db_pool, "monthly_sales")) return(data.frame())
    
    # We fetch data using the pool
    df <- dbGetQuery(db_pool, "
      SELECT e.primary_name, s.*, EXTRACT(YEAR FROM s.report_date) AS year_num
      FROM monthly_sales s
      JOIN estimators e ON s.estimator_id = e.id
    ")
    df
  })
  
  # ── 2. UPLOAD ─────────────────────────────────────────────────────────────
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
      
      dbExecute(db_pool, "CREATE TABLE IF NOT EXISTS estimators (id SERIAL PRIMARY KEY, primary_name VARCHAR(100) UNIQUE NOT NULL, active BOOLEAN DEFAULT TRUE);")
      dbExecute(db_pool, "CREATE TABLE IF NOT EXISTS monthly_sales (id SERIAL PRIMARY KEY, estimator_id INTEGER REFERENCES estimators(id), report_date DATE NOT NULL, amt_bid NUMERIC(15,2), qty_bid INTEGER, amt_booked NUMERIC(15,2), qty_booked INTEGER, UNIQUE(estimator_id, report_date));")
      
      for (nm in unique(clean$clean_name))
        tryCatch(dbExecute(db_pool, "INSERT INTO estimators (primary_name) VALUES ($1) ON CONFLICT DO NOTHING", params = list(nm)), error = function(e) NULL)
      
      est_map <- dbGetQuery(db_pool, "SELECT id, primary_name FROM estimators")
      upload  <- clean %>% left_join(est_map, by = c("clean_name" = "primary_name")) %>%
        select(estimator_id = id, report_date, amt_bid = AmtBid, qty_bid = QtyBid, amt_booked = AmtBooked, qty_booked = QtyBooked)
      
      dbWriteTable(db_pool, "staging_sales", upload, overwrite = TRUE, temporary = TRUE)
      n <- dbExecute(db_pool, "
        INSERT INTO monthly_sales (estimator_id,report_date,amt_bid,qty_bid,amt_booked,qty_booked)
        SELECT estimator_id,report_date,amt_bid,qty_bid,amt_booked,qty_booked FROM staging_sales
        ON CONFLICT (estimator_id, report_date) DO NOTHING;")
      dbExecute(db_pool, "DROP TABLE IF EXISTS staging_sales")
      
      showNotification(paste("Success! Added/Merged", n, "records."), type = "message")
      data_trigger(data_trigger() + 1)
    }, error = function(e) showNotification(paste("Error:", e$message), type = "error"))
  })
  
  # ── FILTERING ─────────────────────────────────────────────────────────────
  filtered_data <- reactive({
    req(input$dateRange)
    df <- get_data()
    if (nrow(df) == 0) return(df[0, ])
    df <- df %>% filter(report_date >= input$dateRange[1], report_date <= input$dateRange[2])
    sel <- input$estimatorSelect
    if (!is.null(sel) && length(sel) > 0) df <- df %>% filter(primary_name %in% sel)
    df
  })
  
  lag_win_rate <- reactive({
    df  <- filtered_data(); lag <- input$win_lag
    if (nrow(df) == 0 || lag == 0) return(NULL)
    all_data <- get_data()
    max_data_date <- max(all_data$report_date, na.rm = TRUE)
    cutoff_date <- max_data_date %m-% months(lag)
    
    bookings_lookup <- df %>% select(primary_name, report_date, amt_booked, qty_booked) %>% rename(book_date = report_date)
    bid_months <- df %>% select(primary_name, report_date, amt_bid, qty_bid) %>% distinct() %>% filter(report_date <= cutoff_date)
    if (nrow(bid_months) == 0) return(NULL)
    
    result <- bid_months %>% rowwise() %>% mutate(
      lag_booked_amt = sum(bookings_lookup$amt_booked[bookings_lookup$primary_name == .data$primary_name & bookings_lookup$book_date > report_date & bookings_lookup$book_date <= report_date %m+% months(lag)], na.rm = TRUE),
      lag_booked_qty = sum(bookings_lookup$qty_booked[bookings_lookup$primary_name == .data$primary_name & bookings_lookup$book_date > report_date & bookings_lookup$book_date <= report_date %m+% months(lag)], na.rm = TRUE)
    ) %>% ungroup()
    result
  })
  
  # ── UPDATERS ──────────────────────────────────────────────────────────────
  observe({
    df <- get_data()
    if (nrow(df) == 0) return()
    est_list <- sort(unique(df$primary_name))
    updatePickerInput(session, "estimatorSelect", choices = est_list, selected = isolate(input$estimatorSelect))
    yrs <- sort(unique(df$year_num), decreasing = TRUE)
    cur_yr <- isolate(input$quarter_year_select)
    updateSelectInput(session, "quarter_year_select", choices = yrs, selected = if (!is.null(cur_yr) && cur_yr %in% yrs) cur_yr else yrs[1])
    max_dt <- max(df$report_date, na.rm = TRUE)
    updateDateRangeInput(session, "dateRange", start = max_dt %m-% months(24), end = max_dt)
    updateDateRangeInput(session, "baselineRange", start = max_dt %m-% months(60), end = max_dt %m-% months(25))
  })
  
  observeEvent(input$reset_filters, {
    df <- get_data(); if (nrow(df) == 0) return()
    max_dt <- max(df$report_date, na.rm = TRUE)
    updateDateRangeInput(session, "dateRange", start = max_dt %m-% months(24), end = max_dt)
    updateDateRangeInput(session, "baselineRange", start = max_dt %m-% months(60), end = max_dt %m-% months(25))
    updatePickerInput(session, "estimatorSelect", selected = character(0))
    updateRadioGroupButtons(session, "view_mode", selected = "Aggregate")
    updateRadioGroupButtons(session, "time_granularity", selected = "Monthly")
    updateRadioGroupButtons(session, "annual_granularity", selected = "year")
    updateRadioGroupButtons(session, "yoy_position", selected = "dodge")
    updateRadioGroupButtons(session, "trend_series", selected = "booked")
    updateSliderInput(session, "win_lag", value = 3)
  })
  
  output$date_verification_table <- renderDataTable({
    d <- get_data(); if (nrow(d) == 0) return(NULL)
    d %>% arrange(report_date) %>% mutate(`Raw Format (MoYear)` = format(report_date, "%y-%m"), `Parsed Date` = format(report_date, "%B %Y"), `Full Date Stored` = as.character(report_date)) %>% select(Estimator = primary_name, `Raw Format (MoYear)`, `Parsed Date`, `Full Date Stored`) %>% distinct() %>% head(50)
  }, options = list(pageLength = 10, scrollX = TRUE, dom = "ftp"))
  
  output$trend_box_title <- renderText(paste("Statistical Process Control —", input$time_granularity, "View"))
  
  # ── KPIs ──────────────────────────────────────────────────────────────────
  output$kpi_total_bid <- renderValueBox(valueBox(dollar(sum(filtered_data()$amt_bid, na.rm = TRUE), accuracy = 1), "Total Bid Volume", icon = icon("file-invoice-dollar"), color = "blue"))
  output$kpi_total_booked <- renderValueBox(valueBox(dollar(sum(filtered_data()$amt_booked, na.rm = TRUE), accuracy = 1), "Total Booked Revenue", icon = icon("money-bill-wave"), color = "green"))
  output$kpi_win_rate_amt <- renderValueBox({
    d <- filtered_data(); r <- sum(d$amt_booked, na.rm = TRUE) / sum(d$amt_bid, na.rm = TRUE)
    valueBox(percent(r, 0.1), HTML("Same-Month Win Rate<br><small style='font-size:10px;opacity:0.75;'>Bids & bookings matched to same month</small>"), icon = icon("chart-line"), color = "yellow")
  })
  output$kpi_lag_win_rate <- renderValueBox({
    lag <- input$win_lag; if (lag == 0) return(valueBox("N/A", HTML("Lag-Adjusted Win Rate<br><small style='font-size:10px;opacity:0.75;'>Set lag > 0 in sidebar</small>"), icon = icon("clock"), color = "purple"))
    lwr <- lag_win_rate(); if (is.null(lwr) || nrow(lwr) == 0) return(valueBox("—", HTML("Lag-Adjusted Win Rate<br><small style='font-size:10px;opacity:0.75;'>Insufficient complete windows in range</small>"), icon = icon("clock"), color = "purple"))
    r <- sum(lwr$lag_booked_amt, na.rm = TRUE) / sum(lwr$amt_bid, na.rm = TRUE)
    max_dt <- max(get_data()$report_date, na.rm = TRUE); cutoff_dt <- max_dt %m-% months(lag)
    valueBox(percent(r, 0.1), HTML(paste0("Lag-Adjusted Win Rate<br><small style='font-size:10px;opacity:0.75;'>Bids after ", format(cutoff_dt, "%b %Y"), " excluded</small>")), icon = icon("clock"), color = "purple")
  })
  
  output$correlation_insight <- renderUI({
    d <- filtered_data()
    if (nrow(d) < 10) return(tags$p("Waiting for sufficient data points...", class = "text-muted"))
    if (sd(d$qty_bid, na.rm = TRUE) == 0 || sd(d$amt_booked, na.rm = TRUE) == 0) return(tags$p("Insufficient variance for correlation.", class = "text-muted"))
    r <- cor(d$qty_bid, d$amt_booked, use = "complete.obs")
    txt <- if (is.na(r)) "<span class='insight-text'>⚠️ Data insufficient for Pearson correlation.</span>" else if (r < 0) paste0("<span class='insight-text'>🔴 <b>Negative Correlation (r=", round(r,2), "):</b> More bids → lower revenue. Signals Estimator Fatigue.</span>") else if (r < 0.3) paste0("<span class='insight-text'>⚠️ <b>Weak Correlation (r=", round(r,2), "):</b> Bid quantity has minimal impact on revenue. Quality > quantity.</span>") else paste0("<span class='insight-text'>🟢 <b>Positive Correlation (r=", round(r,2), "):</b> Higher bid volume is translating to revenue. Pipeline mechanics are healthy.</span>")
    HTML(txt)
  })
  
  # ── SPC CHARTS ────────────────────────────────────────────────────────────
  output$trend_dollars <- renderPlotly({
    d <- filtered_data(); req(input$time_granularity, input$view_mode, input$baselineRange)
    validate(need(nrow(d) > 0, "No data.")); is_q <- input$time_granularity == "Quarterly"; by_person <- input$view_mode == "Individual"
    show_both <- isTRUE(input$trend_series == "both")
    if (by_person) { n <- length(unique(d$primary_name)); validate(need(n <= 4, paste0("⚠️ Select 4 or fewer estimators. Currently: ", n))) }
    
    # We call the global function, passing the UI dates explicitly
    ctrl <- spc_limits(d, "amt_booked", is_q, by_person, input$baselineRange[1], input$baselineRange[2])
    
    if (show_both) {
      grp_date <- function(dt) if (is_q) floor_date(dt, "quarter") else floor_date(dt, "month")
      if (by_person) { bid_trend <- d %>% mutate(date_group = grp_date(report_date)) %>% group_by(date_group, primary_name) %>% summarise(Bid = sum(amt_bid, na.rm = TRUE), .groups = "drop"); ctrl <- ctrl %>% left_join(bid_trend, by = c("date_group", "primary_name"))
      } else { bid_trend <- d %>% mutate(date_group = grp_date(report_date)) %>% group_by(date_group) %>% summarise(Bid = sum(amt_bid, na.rm = TRUE), .groups = "drop"); ctrl <- ctrl %>% left_join(bid_trend, by = "date_group") }
    }
    
    if (by_person) {
      p <- ggplot(ctrl, aes(x = date_group, y = Val, group = primary_name, text = paste0("<b>", format(date_group, "%b %Y"), "</b><br>", primary_name, "<br>Booked: ", dollar(round(Val), 1), "<br>Baseline Mean: ", dollar(round(BL_Mean), 1)))) +
        geom_line(aes(color = primary_name), linewidth = 0.9) + geom_point(aes(shape = Anomaly), size = 2) +
        { if (show_both) geom_line(aes(y = Bid, group = primary_name), linetype = "dotted", color = "#95a5a6", linewidth = 0.8) } +
        geom_hline(aes(yintercept = BL_Mean), color = "#2980b9", linetype = "solid",  alpha = 0.5) + geom_hline(aes(yintercept = UCL), color = "#c0392b", linetype = "dashed", alpha = 0.5) + geom_hline(aes(yintercept = LCL), color = "#c0392b", linetype = "dashed", alpha = 0.5) +
        scale_shape_manual(values = c("Normal" = 16, "Anomaly" = 8)) + scale_y_continuous(labels = dollar_format(accuracy = 1)) +
        facet_wrap(~primary_name, ncol = 2, scales = "free_y") + theme_minimal() + labs(x = "", y = "") + theme(legend.position = "none", strip.background = element_rect(fill = "#f4f6f9"), strip.text = element_text(face = "bold"))
      if (input$show_trend) { p <- p + geom_smooth(method = "lm", se = FALSE, color = "#8e44ad", linetype = "dashed", linewidth = 0.7) }
    } else {
      p <- ggplot(ctrl, aes(x = date_group, y = Val, group = 1, text = paste0("<b>", format(date_group, "%b %Y"), "</b><br>", "Booked: ", dollar(round(Val), 1), "<br>Baseline Mean: ", dollar(round(BL_Mean), 1), if (show_both) paste0("<br>Bid: ", dollar(round(Bid), 1)) else ""))) +
        geom_line(color = "#2c3e50", linewidth = 0.9) + geom_point(aes(color = Anomaly), size = 2) +
        { if (show_both) geom_line(aes(y = Bid, group = 1), color = "#95a5a6", linetype = "dotted", linewidth = 1) } +
        geom_line(aes(y = BL_Mean), color = "#2980b9", linetype = "solid",  alpha = 0.5) + geom_line(aes(y = UCL), color = "#c0392b", linetype = "dashed", alpha = 0.5) + geom_line(aes(y = LCL), color = "#c0392b", linetype = "dashed", alpha = 0.5) +
        scale_color_manual(values = c("Normal" = "#2c3e50", "Anomaly" = "#e74c3c")) + scale_y_continuous(labels = dollar_format(accuracy = 1)) + theme_minimal() + labs(x = "", y = "") + theme(legend.position = "none")
    }
    ggplotly(p, tooltip = "text")
  })
  
  output$trend_qty <- renderPlotly({
    d <- filtered_data(); req(input$time_granularity, input$view_mode, input$baselineRange)
    validate(need(nrow(d) > 0, "No data.")); is_q <- input$time_granularity == "Quarterly"; by_person <- input$view_mode == "Individual"
    if (by_person) { n <- length(unique(d$primary_name)); validate(need(n <= 4, paste0("⚠️ Select 4 or fewer estimators. Currently: ", n))) }
    
    ctrl <- spc_limits(d, "qty_booked", is_q, by_person, input$baselineRange[1], input$baselineRange[2])
    
    if (by_person) {
      p <- ggplot(ctrl, aes(x = date_group, y = Val, group = primary_name, text = paste0("<b>", format(date_group, "%b %Y"), "</b><br>", primary_name, "<br>Jobs: ", comma(Val, 1), "<br>Baseline Mean: ", comma(BL_Mean, 1)))) +
        geom_line(color = "#27ae60", linewidth = 0.9) + geom_point(aes(color = Anomaly), size = 2) +
        geom_hline(aes(yintercept = BL_Mean), color = "#2980b9", linetype = "solid",  alpha = 0.5) + geom_hline(aes(yintercept = UCL), color = "#c0392b", linetype = "dashed", alpha = 0.5) + geom_hline(aes(yintercept = LCL), color = "#c0392b", linetype = "dashed", alpha = 0.5) +
        scale_color_manual(values = c("Normal" = "#27ae60", "Anomaly" = "#e74c3c")) + scale_y_continuous(labels = comma_format(accuracy = 1)) + facet_wrap(~primary_name, ncol = 2, scales = "free_y") + theme_minimal() + labs(x = "", y = "") + theme(legend.position = "none", strip.background = element_rect(fill = "#f4f6f9"), strip.text = element_text(face = "bold"))
      if (input$show_trend) { p <- p + geom_smooth(method = "lm", se = FALSE, color = "#8e44ad", linetype = "dashed", linewidth = 0.7) }
    } else {
      p <- ggplot(ctrl, aes(x = date_group, y = Val, group = 1, text = paste0("<b>", format(date_group, "%b %Y"), "</b><br>", "Jobs Booked: ", comma(Val, 1), "<br>Baseline Mean: ", comma(BL_Mean, 1)))) +
        geom_line(color = "#27ae60", linewidth = 0.9) + geom_point(aes(color = Anomaly), size = 2) + geom_line(aes(y = BL_Mean), color = "#2980b9", linetype = "solid",  alpha = 0.5) + geom_line(aes(y = UCL), color = "#c0392b", linetype = "dashed", alpha = 0.5) + geom_line(aes(y = LCL), color = "#c0392b", linetype = "dashed", alpha = 0.5) +
        scale_color_manual(values = c("Normal" = "#27ae60", "Anomaly" = "#e74c3c")) + scale_y_continuous(labels = comma_format(accuracy = 1)) + theme_minimal() + labs(x = "", y = "") + theme(legend.position = "none")
    }
    ggplotly(p, tooltip = "text")
  })
  
  output$bid_funnel_chart <- renderPlotly({
    d <- filtered_data(); validate(need(nrow(d) > 0, "No data."))
    funnel <- d %>% group_by(primary_name) %>% summarise(Bid = sum(amt_bid, na.rm = TRUE), Booked = sum(amt_booked, na.rm = TRUE), .groups = "drop") %>% filter(Bid > 0) %>% mutate(Win_Rate = Booked / Bid, primary_name = fct_reorder(primary_name, Bid)) %>% pivot_longer(c(Bid, Booked), names_to = "Type", values_to = "Amount") %>% mutate(Win_Label = ifelse(Type == "Booked", paste0(percent(Win_Rate, 0.1), " win rate"), NA_character_))
    p <- ggplot(funnel, aes(x = primary_name, y = Amount, fill = Type, text = paste0("<b>", primary_name, "</b><br>", Type, ": ", dollar(Amount, accuracy = 1), ifelse(!is.na(Win_Label), paste0("<br>", Win_Label), "")))) + geom_col(position = "dodge", width = 0.65) + geom_text(aes(label = Win_Label), position = position_dodge(width = 0.65), hjust = -0.1, size = 3, na.rm = TRUE, color = "#2c3e50") + scale_fill_manual(values = c("Bid" = "#95a5a6", "Booked" = "#27ae60")) + scale_y_continuous(labels = dollar_format(accuracy = 1), expand = expansion(mult = c(0, 0.25))) + coord_flip() + labs(x = "", y = "Amount ($)", fill = "") + theme_minimal() + theme(legend.position = "top", panel.grid.major.y = element_blank())
    ggplotly(p, tooltip = "text")
  })
  
  # ── SEASONALITY HEATMAP (CEO TWEAK: Using input$heatmap_metric) ─────────
  output$seasonality_heatmap <- renderPlotly({
    df <- get_data(); req(input$heatmap_metric); sel <- input$estimatorSelect
    if (!is.null(sel) && length(sel) > 0) df <- df %>% filter(primary_name %in% sel)
    validate(need(nrow(df) > 0, "No data."))
    
    metric_sym <- sym(input$heatmap_metric)
    metric_label <- if(input$heatmap_metric == "amt_booked") "Booked ($)" else "Bid ($)"
    
    heat <- df %>% mutate(Month = month(report_date, label = TRUE, abbr = TRUE), Year = as.integer(year(report_date))) %>% group_by(Year, Month) %>% summarise(Value = sum(!!metric_sym, na.rm = TRUE), .groups = "drop") %>% complete(Year, Month, fill = list(Value = 0)) %>% mutate(Month = factor(Month, levels = rev(month.abb)))
    p <- ggplot(heat, aes(x = factor(Year), y = Month, fill = Value, text = paste0("<b>", Month, " ", Year, "</b><br>", metric_label, ": ", dollar(Value, accuracy = 1)))) + geom_tile(color = "white", linewidth = 0.4) + scale_fill_gradient(low = "#eaf4fb", high = if(input$heatmap_metric == "amt_booked") "#1a5276" else "#7f8c8d", labels = dollar_format(scale = 1e-3, suffix = "K", accuracy = 1), name = metric_label) + labs(x = "Year", y = "") + theme_minimal() + theme(axis.text.x = element_text(angle = 45, hjust = 1), panel.grid = element_blank(), legend.position = "right")
    ggplotly(p, tooltip = "text")
  })
  
  output$scatter_matrix <- renderPlotly({
    dat <- filtered_data(); validate(need(nrow(dat) > 0, "Upload data to begin.")); lwr <- lag_win_rate()
    ds_df <- dat %>% group_by(primary_name) %>% summarise(Total_Bid = sum(amt_bid, na.rm = TRUE), Total_Booked = sum(amt_booked, na.rm = TRUE), Jobs_Bid = sum(qty_bid, na.rm = TRUE), .groups = "drop") %>% mutate(Avg_Job_Size = Total_Bid / Jobs_Bid, Win_Rate_SM = Total_Booked / Total_Bid)
    if (!is.null(lwr) && nrow(lwr) > 0) { lag_summary <- lwr %>% group_by(primary_name) %>% summarise(Lag_Win_Rate = sum(lag_booked_amt, na.rm = TRUE) / sum(amt_bid, na.rm = TRUE), .groups = "drop"); ds_df <- ds_df %>% left_join(lag_summary, by = "primary_name") %>% mutate(Win_Rate = coalesce(Lag_Win_Rate, Win_Rate_SM)) } else { ds_df$Win_Rate <- ds_df$Win_Rate_SM }
    ds_df <- ds_df %>% filter(Total_Bid > 0, !is.na(Win_Rate), !is.infinite(Win_Rate), !is.na(Avg_Job_Size), !is.infinite(Avg_Job_Size)); validate(need(nrow(ds_df) >= 3, "Need at least 3 active estimators to cluster."))
    set.seed(42)
    if (any(sapply(ds_df[, c("Total_Bid", "Win_Rate", "Avg_Job_Size")], function(x) sd(x, na.rm = TRUE) == 0))) { ds_df$Archetype <- "Archetype 1" } else { feats <- ds_df %>% select(Total_Bid, Win_Rate, Avg_Job_Size) %>% scale(); k <- min(3, nrow(ds_df) - 1); km <- kmeans(feats, centers = k, nstart = 25); ds_df$Cluster <- as.factor(km$cluster); cl_means <- ds_df %>% group_by(Cluster) %>% summarise(mean_wr = mean(Win_Rate, na.rm = TRUE), .groups = "drop") %>% arrange(desc(mean_wr)); arch_map <- setNames(c("High-Efficiency", "Selective", "High-Volume")[seq_len(nrow(cl_means))], cl_means$Cluster); ds_df$Archetype <- arch_map[as.character(ds_df$Cluster)] }
    p <- ggplot(ds_df, aes(x = Total_Bid, y = Win_Rate, color = Archetype, size = Total_Booked, text = paste0("<b>", primary_name, "</b><br>", "Archetype: ", Archetype, "<br>", "Pipeline Effort: ", dollar(Total_Bid, 1), "<br>", "Win Rate: ", percent(Win_Rate, 0.1), "<br>", "Avg Job Size: ", dollar(Avg_Job_Size, 1), "<br>", "Total Booked: ", dollar(Total_Booked, 1)))) + geom_point(alpha = 0.85) + scale_color_manual(values = c("High-Efficiency" = "#27ae60", "Selective" = "#f39c12", "High-Volume" = "#e74c3c")) + scale_x_continuous(labels = dollar_format(accuracy = 1)) + scale_y_continuous(labels = percent_format(accuracy = 1)) + labs(x = "Total Amount Bid", y = "Win Rate", color = "Archetype") + theme_minimal() + theme(legend.position = "right")
    ggplotly(p, tooltip = "text")
  })
  
  output$annual_bar_chart <- renderPlotly({
    d <- filtered_data(); validate(need(nrow(d) > 0, "No data.")); gran <- input$annual_granularity; by_person <- !is.null(input$estimatorSelect) && length(input$estimatorSelect) <= 5
    d <- d %>% mutate(x_group = case_when(gran == "year" ~ as.character(year(report_date)), gran == "quarter" ~ paste0(year(report_date), " Q", quarter(report_date)), gran == "month" ~ format(report_date, "%Y-%m"), TRUE ~ as.character(year(report_date)))); x_levels <- d %>% arrange(report_date) %>% pull(x_group) %>% unique(); d <- d %>% mutate(x_group = factor(x_group, levels = x_levels))
    if (by_person) { annual <- d %>% group_by(x_group, primary_name) %>% summarise(Booked = sum(amt_booked, na.rm = TRUE), .groups = "drop"); p <- ggplot(annual, aes(x_group, Booked, fill = primary_name, text = paste0("<b>", x_group, "</b><br>", primary_name, "<br>", dollar(round(Booked), accuracy = 1)))) + geom_col(position = "dodge") + scale_fill_brewer(palette = "Paired")
    } else { annual <- d %>% group_by(x_group) %>% summarise(Bid = sum(amt_bid, na.rm = TRUE), Booked = sum(amt_booked, na.rm = TRUE), .groups = "drop") %>% pivot_longer(c(Bid, Booked), names_to = "Type", values_to = "Value"); p <- ggplot(annual, aes(x_group, Value, fill = Type, text = paste0("<b>", x_group, "</b><br>", Type, ": ", dollar(round(Value), accuracy = 1)))) + geom_col(position = "dodge") + scale_fill_manual(values = c("Bid" = "#95a5a6", "Booked" = "#27ae60")) }
    ggplotly(p + scale_y_continuous(labels = dollar_format(accuracy = 1)) + labs(x = "", y = "", fill = "") + theme_minimal() + theme(axis.text.x = element_text(angle = if (gran != "year") 45 else 0, hjust = 1)), tooltip = "text")
  })
  
  output$yoy_chart <- renderPlotly({
    d <- filtered_data(); req(input$yoy_metric, input$view_mode); validate(need(nrow(d) > 0, "No data.")); msym <- sym(input$yoy_metric); bar_pos <- input$yoy_position
    if (input$view_mode == "Individual") { validate(need(length(unique(d$primary_name)) <= 10, "Select 10 or fewer estimators for Individual YoY.")); ag <- d %>% group_by(year_num, primary_name) %>% summarise(V = sum(!!msym, na.rm = TRUE), .groups = "drop") %>% arrange(year_num) %>% group_by(primary_name) %>% mutate(G = (V - lag(V)) / lag(V)) %>% filter(!is.na(G)); p <- ggplot(ag, aes(factor(year_num), G, fill = primary_name, text = paste0(year_num, " — ", primary_name, "<br>Growth: ", percent(G, 0.1)))) + geom_col(position = bar_pos) + scale_fill_brewer(palette = "Paired")
    } else { ag <- d %>% group_by(year_num) %>% summarise(V = sum(!!msym, na.rm = TRUE), .groups = "drop") %>% arrange(year_num) %>% mutate(G = (V - lag(V)) / lag(V)) %>% filter(!is.na(G)); p <- ggplot(ag, aes(factor(year_num), G, fill = G > 0, text = paste0(year_num, "<br>Growth: ", percent(G, 0.1)))) + geom_col(show.legend = FALSE) + scale_fill_manual(values = c("TRUE" = "#27ae60", "FALSE" = "#c0392b")) }
    ggplotly(p + geom_hline(yintercept = 0) + scale_y_continuous(labels = percent_format()) + labs(x = "Year", y = "Growth %") + theme_minimal(), tooltip = "text")
  })
  
  output$quarterly_breakdown_chart <- renderPlotly({
    d <- filtered_data(); req(input$quarter_year_select); validate(need(nrow(d) > 0, "No data.")); q <- d %>% filter(year_num == as.numeric(input$quarter_year_select)) %>% mutate(Quarter = paste0("Q", quarter(report_date))) %>% group_by(Quarter) %>% summarise(Revenue = sum(amt_booked, na.rm = TRUE), .groups = "drop"); p <- ggplot(q, aes(Quarter, Revenue, text = paste0(Quarter, "<br>", dollar(Revenue, 1)))) + geom_col(fill = "#3c8dbc", width = 0.6) + scale_y_continuous(labels = dollar_format(accuracy = 1)) + labs(x = "", y = "Revenue ($)") + theme_minimal() + theme(panel.grid.major.x = element_blank()); ggplotly(p, tooltip = "text")
  })
  
  output$leaderboard_table <- renderDataTable({
    d <- filtered_data(); if (nrow(d) == 0) return(NULL); lb <- d %>% group_by(primary_name) %>% summarise(Total_Bids = sum(amt_bid, na.rm = TRUE), Total_Booked = sum(amt_booked, na.rm = TRUE), Jobs_Won = sum(qty_booked, na.rm = TRUE), Jobs_Bid = sum(qty_bid, na.rm = TRUE), .groups = "drop") %>% mutate(Win_Rate = Total_Booked / Total_Bids, Avg_Job_Size = Total_Bids / Jobs_Bid)
    if (input$win_lag > 0) { lwr <- lag_win_rate(); if (!is.null(lwr)) { lag_col <- lwr %>% group_by(primary_name) %>% summarise(Lag_WR = sum(lag_booked_amt, na.rm = TRUE) / sum(amt_bid, na.rm = TRUE), .groups = "drop"); lb <- lb %>% left_join(lag_col, by = "primary_name") } }
    lb <- lb %>% mutate(Total_Bids = dollar(Total_Bids, accuracy = 1), Total_Booked = dollar(Total_Booked, accuracy = 1), Win_Rate = percent(Win_Rate, accuracy = 0.1), Avg_Job_Size = dollar(Avg_Job_Size, accuracy = 1)) %>% rename(Estimator = primary_name, `Total Revenue` = Total_Booked, `Total Bids` = Total_Bids, `Win % (same-mo)` = Win_Rate, `Jobs Won` = Jobs_Won, `Jobs Bid` = Jobs_Bid, `Avg Job Size` = Avg_Job_Size)
    if ("Lag_WR" %in% names(lb)) lb <- lb %>% mutate(Lag_WR = percent(Lag_WR, accuracy = 0.1)) %>% rename(`Win % (lag-adj)` = Lag_WR)
    datatable(lb, options = list(pageLength = 15, scrollX = TRUE, order = list(list(2, "desc"))), rownames = FALSE)
  })
  
  output$db_status_text <- renderText({ paste("Database records loaded:", comma(nrow(get_data()))) })
  output$preview_table <- renderDataTable({ d <- get_data(); if (nrow(d) == 0) return(NULL); d %>% arrange(desc(report_date)) %>% head(5) %>% select(Estimator = primary_name, Date = report_date, Bid = amt_bid, Booked = amt_booked) }, options = list(dom = "t", searching = FALSE))
  
  output$download_report <- downloadHandler(
    filename = function() { paste0("Estimator_Report_", Sys.Date(), ".html") },
    content = function(file) {
      tempReport <- file.path(tempdir(), "report_auto.Rmd")
      rmd_code <- c(
        "---", "title: 'Estimator Intelligence — Executive Summary'", paste0("date: '", Sys.Date(), "'"), "output: html_document", "---", "", "### Filter Settings Applied", paste0("**Date Range:** ", format(input$dateRange[1], "%B %d, %Y"), " to ", format(input$dateRange[2], "%B %d, %Y"), "  "), paste0("**Estimators Included:** ", if(is.null(input$estimatorSelect)) "All Company" else paste(input$estimatorSelect, collapse=", "), "  "), "", "### Key Performance Indicators", paste0("Total Bid Volume: **", dollar(sum(filtered_data()$amt_bid, na.rm=T), accuracy=1), "**  "), paste0("Total Booked Revenue: **", dollar(sum(filtered_data()$amt_booked, na.rm=T), accuracy=1), "**  "), paste0("Average Win Rate (Same-Month): **", percent(sum(filtered_data()$amt_booked, na.rm=T)/sum(filtered_data()$amt_bid, na.rm=T), 0.1), "**  "), "", "### Instructions to Save as PDF", "1. Open this file in Google Chrome or Microsoft Edge.", "2. Press **Ctrl+P** (Windows) or **Cmd+P** (Mac).", "3. Change the Destination printer to **'Save as PDF'**.", "4. Ensure 'Background graphics' is checked for optimal styling."
      )
      writeLines(rmd_code, tempReport)
      rmarkdown::render(tempReport, output_file = file, envir = new.env(parent = globalenv()), quiet = TRUE)
    }
  )
}