library(shiny)
library(bslib)
library(arrow)
library(dplyr)
library(tidyr)
library(ggplot2)
library(scales)
library(patchwork)
library(stringr)
library(forcats)
library(DT)

# ── Configuration ──────────────────────────────────────────────────────────────
# We read from the GCS bucket mount path or a local 'data' folder for testing
output_dir <- Sys.getenv("GCS_MOUNT_PATH", unset = "./data")
spine_parquet <- file.path(output_dir, "dmd_full_mapping.parquet")

# ATC level 1 anatomical group labels (WHO standard)
atc_l1_labels <- c(
  A = "Alimentary & Metabolism", B = "Blood & Blood Forming Organs",
  C = "Cardiovascular", D = "Dermatologicals", G = "Genito-Urinary & Sex Hormones",
  H = "Hormones (Non-Sex)", J = "Anti-Infectives (Systemic)",
  L = "Antineoplastic & Immunomodulating", M = "Musculoskeletal",
  N = "Nervous System", P = "Antiparasitic", R = "Respiratory",
  S = "Sensory Organs", V = "Various"
)

j01_subclass_labels <- c(
  J01A = "Tetracyclines", J01B = "Amphenicols", J01C = "Beta-lactam Penicillins",
  J01D = "Other Beta-lactams (Cephalosporins etc.)", J01E = "Sulfonamides & Trimethoprim",
  J01F = "Macrolides, Lincosamides & Streptogramins", J01G = "Aminoglycoside Antibacterials",
  J01M = "Quinolone Antibacterials", J01R = "Combinations", J01X = "Other Antibacterials"
)

theme_dmd <- function() {
  theme_minimal(base_size = 12) +
    theme(
      plot.title    = element_text(face = "bold", size = 14),
      plot.subtitle = element_text(colour = "grey40", size = 11),
      plot.caption  = element_text(colour = "grey55", size = 9),
      axis.title    = element_text(face = "bold"),
      legend.position = "bottom",
      panel.grid.minor = element_blank()
    )
}

status_colours <- c(
  "ATC + BNF" = "#2E7D32",
  "ATC only"  = "#1976D2",
  "BNF only"  = "#F57C00",
  "Unmapped"  = "#C62828"
)

# ── Shiny UI ───────────────────────────────────────────────────────────────────
ui <- page_navbar(
  title = "NHS TRUD dm+d Mapping Dashboard",
  theme = bs_theme(version = 5, preset = "lumen"),
  sidebar = sidebar(
    title = "Filters",
    helpText("Load status:"),
    textOutput("load_status"),
    hr(),
    selectInput("group_filter", "ATC Anatomical Group (L1)",
                choices = c("All" = "All", atc_l1_labels),
                selected = "All"),
    hr(),
    p("Data sourced from NHS TRUD Items 24 and 25.")
  ),
  
  nav_panel("Overview",
    layout_columns(
      card(card_header("Overall Mapping Coverage"), plotOutput("plot_overall", height = "300px")),
      card(card_header("Coverage by Anatomical Group"), plotOutput("plot_atc_groups", height = "500px")),
      col_widths = c(12, 12)
    )
  ),
  
  nav_panel("Antibiotic Focus (J01)",
    layout_columns(
      card(card_header("Coverage by Subclass"), plotOutput("plot_abx_subclass", height = "400px")),
      card(card_header("DDD Availability"), plotOutput("plot_abx_ddd", height = "400px")),
      col_widths = c(6, 6)
    ),
    card(card_header("ATC Level 4 Chemical Subgroup"), plotOutput("plot_abx_l4", height = "600px"))
  ),
  
  nav_panel("Antibiotic Gap Analysis",
    layout_columns(
      card(card_header("Expected vs Mapped Antibiotics"), plotOutput("plot_gap_coverage", height = "300px")),
      card(card_header("Missing Map Status"), plotOutput("plot_gap_status", height = "300px")),
      col_widths = c(6, 6)
    ),
    card(
      card_header("Unmapped Antibiotic Agents"),
      p("These VMPs have a BNF code indicating they are antibacterials (0501...) but lack an ATC code."),
      DTOutput("table_missing_abx")
    )
  ),

  nav_panel("DDD Explorer",
    card(
      card_header("Search DDD Values"),
      p("Search for an ATC code or drug name to view its Defined Daily Dose (DDD), Unit of Measure (UOM), and the normalised value in milligrams."),
      DTOutput("table_ddd_search")
    )
  ),

  nav_panel("Raw Data Settings",
    card(
      card_header("Data File Location"),
      verbatimTextOutput("file_info")
    )
  )
)

source("R/data_utils.R")

# ── Shiny Server ───────────────────────────────────────────────────────────────
server <- function(input, output, session) {
  
  # Reactive data load
  dataset <- reactive({
    req(file.exists(spine_parquet))
    prepare_dmd_data(spine_parquet)
  })
  
  filtered_data <- reactive({
    df <- dataset()
    if (input$group_filter != "All") {
      # Filter to selected label
      df <- df %>%
        mutate(atc_group = atc_l1_labels[atc_level1_code]) %>%
        filter(atc_group == input$group_filter)
    }
    df
  })
  
  output$load_status <- renderText({
    if (file.exists(spine_parquet)) {
      paste("Loaded", scales::comma(nrow(dataset())), "records.")
    } else {
      "Data file not found. Run pipeline first."
    }
  })
  
  output$file_info <- renderPrint({
    cat("Spine Parquet Path:", spine_parquet, "\n")
    cat("Exists:", file.exists(spine_parquet), "\n")
  })
  
  # OVERVIEW PLOTS
  output$plot_overall <- renderPlot({
    req(nrow(filtered_data()) > 0)
    
    coverage_summary <- filtered_data() %>%
      count(mapping_status) %>%
      mutate(
        pct   = n / sum(n),
        label = paste0(comma(n), "\n(", percent(pct, accuracy = 0.1), ")")
      )
    
    ggplot(coverage_summary, aes(x = "", y = pct, fill = mapping_status)) +
      geom_col(width = 0.5, colour = "white", linewidth = 0.6) +
      geom_text(
        aes(label = label),
        position = position_stack(vjust = 0.5),
        colour = "white", fontface = "bold", size = 4
      ) +
      coord_flip() +
      scale_fill_manual(values = status_colours) +
      scale_y_continuous(labels = percent_format()) +
      labs(
        title    = "VMP Mapping Coverage",
        x = NULL, y = "Proportion of VMPs",
        fill = "Coverage Status"
      ) +
      theme_dmd()
  })
  
  output$plot_atc_groups <- renderPlot({
    df <- dataset()
    req("All" == input$group_filter) # Only meaningful if not filtered single
    
    p2_data <- df %>%
      filter(has_atc) %>%
      mutate(
        atc_group = atc_l1_labels[atc_level1_code],
        atc_group = coalesce(atc_group, paste0("Other (", atc_level1_code, ")"))
      ) %>%
      count(atc_group, mapping_status) %>%
      group_by(atc_group) %>%
      mutate(pct = n / sum(n)) %>%
      ungroup()
      
    ggplot(
      p2_data,
      aes(x = fct_reorder(atc_group, n, sum), y = n, fill = mapping_status)
    ) +
      geom_col() +
      coord_flip() +
      scale_fill_manual(values = status_colours) +
      scale_y_continuous(labels = comma) +
      labs(
        title    = "VMP Count by ATC Anatomical Group (Level 1)",
        x = NULL, y = "Number of VMPs", fill = "Coverage Status"
      ) +
      theme_dmd()
  })
  
  # ANTIBIOTIC PLOTS
  antibiotics_data <- reactive({
    dataset() %>%
      filter(is_antibiotic) %>%
      mutate(
        j01_class = j01_subclass_labels[atc_level3_code],
        j01_class = coalesce(j01_class, paste0("Other (", atc_level3_code, ")"))
      )
  })
  
  output$plot_abx_subclass <- renderPlot({
    abx <- antibiotics_data()
    req(nrow(abx) > 0)
    
    p4_data <- abx %>%
      count(j01_class, mapping_status) %>%
      group_by(j01_class) %>%
      mutate(total = sum(n)) %>%
      ungroup()
      
    ggplot(
      p4_data,
      aes(x = fct_reorder(j01_class, total), y = n, fill = mapping_status)
    ) +
      geom_col() +
      geom_text(
        data = p4_data %>% group_by(j01_class) %>% summarise(total = sum(n)),
        aes(x = j01_class, y = total, label = comma(total), fill = NULL),
        hjust = -0.15, size = 3.5, fontface = "bold"
      ) +
      coord_flip(clip = "off") +
      scale_fill_manual(values = status_colours) +
      scale_y_continuous(labels = comma, expand = expansion(mult = c(0, 0.15))) +
      labs(
        title    = "Coverage by Subclass",
        x = NULL, y = "Number of VMPs", fill = "Coverage Status"
      ) +
      theme_dmd()
  })
  
  output$plot_abx_ddd <- renderPlot({
    abx <- antibiotics_data()
    req(nrow(abx) > 0)
    
    p6_data <- abx %>%
      mutate(has_ddd = !is.na(ddd) & ddd != "") %>%
      group_by(j01_class) %>%
      summarise(
        total    = n(),
        with_ddd = sum(has_ddd),
        pct_ddd  = with_ddd / total,
        .groups  = "drop"
      )
      
    ggplot(
      p6_data,
      aes(x = fct_reorder(j01_class, pct_ddd), y = pct_ddd, fill = pct_ddd)
    ) +
      geom_col() +
      geom_text(
        aes(label = paste0(percent(pct_ddd, accuracy = 1), "\n(", with_ddd, "/", total, ")")),
        hjust = -0.05, size = 3.5
      ) +
      coord_flip(clip = "off") +
      scale_fill_gradient(low = "#E3F2FD", high = "#1565C0", labels = percent) +
      scale_y_continuous(labels = percent_format(), expand = expansion(mult = c(0, 0.2))) +
      labs(
        title    = "DDD Availability",
        x = NULL, y = "% with DDD value", fill = "% with DDD"
      ) +
      theme_dmd() +
      theme(legend.position = "none")
  })
  
  output$plot_abx_l4 <- renderPlot({
    abx <- antibiotics_data()
    req(nrow(abx) > 0)
    
    p5_data <- abx %>%
      filter(has_atc) %>%
      count(atc_level4_code, atc_level3_code) %>%
      mutate(
        j01_class = coalesce(j01_subclass_labels[atc_level3_code], atc_level3_code)
      ) %>%
      group_by(j01_class) %>%
      mutate(pct_within_class = n / sum(n)) %>%
      ungroup() %>%
      arrange(j01_class, desc(n))
      
    ggplot(
      p5_data,
      aes(x = fct_reorder(atc_level4_code, n), y = n, fill = j01_class)
    ) +
      geom_col() +
      coord_flip() +
      scale_y_continuous(labels = comma) +
      scale_fill_brewer(palette = "Set3") +
      labs(
        title    = "Distribution by Chemical Subgroup",
        x = "ATC Level 4 Code", y = "Number of VMPs", fill = "J01 Subclass"
      ) +
      theme_dmd() +
      theme(legend.position = "right")
  })
  
  # GAP ANALYSIS PLOTS & TABLE
  output$plot_gap_coverage <- renderPlot({
    abx <- antibiotics_data()
    req(nrow(abx) > 0)
    
    gap_summary <- abx %>%
      mutate(status = if_else(has_atc, "Mapped to ATC", "Missing ATC")) %>%
      count(status) %>%
      mutate(
        pct = n / sum(n),
        label = paste0(comma(n), "\n(", percent(pct, accuracy = 0.1), ")")
      )
      
    ggplot(gap_summary, aes(x = "", y = pct, fill = status)) +
      geom_col(width = 0.5, colour = "white", linewidth = 0.6) +
      geom_text(aes(label = label), position = position_stack(vjust = 0.5), colour = "white", fontface = "bold", size = 4) +
      coord_flip() +
      scale_fill_manual(values = c("Mapped to ATC" = "#2E7D32", "Missing ATC" = "#C62828")) +
      scale_y_continuous(labels = percent_format()) +
      labs(title = "Antimicrobial ATC Coverage", x = NULL, y = "Proportion of Expected Antibiotics", fill = "Status") +
      theme_dmd()
  })
  
  output$plot_gap_status <- renderPlot({
    abx <- antibiotics_data() %>% filter(!has_atc)
    req(nrow(abx) > 0)
    
    ggplot(abx, aes(x = mapping_status, fill = mapping_status)) +
      geom_bar() +
      scale_fill_manual(values = status_colours) +
      scale_y_continuous(labels = comma) +
      labs(title = "Why are they missing?", x = "Current Mapping Status", y = "Count") +
      theme_dmd() + theme(legend.position = "none")
  })
  
  output$table_missing_abx <- renderDT({
    abx <- antibiotics_data() %>% 
      filter(!has_atc) %>%
      select(vpid, vmp_nm, bnf_code, mapping_status) %>%
      arrange(vmp_nm)
      
    datatable(
      abx, 
      colnames = c("Virtual Product ID (VPID)", "Virtual Medicinal Product (VMP) Name", "British National Formulary (BNF) Code", "Current NHS Mapping Status"),
      options = list(pageLength = 10, scrollX = TRUE),
      rownames = FALSE
    )
  })
  
  output$table_ddd_search <- renderDT({
    df <- dataset()
    
    # 1. Base grouping columns
    grp_cols <- c("atc_code", "ddd")
    if("ddd_uom" %in% names(df)) grp_cols <- c(grp_cols, "ddd_uom")
    if("ddd_mg" %in% names(df)) grp_cols <- c(grp_cols, "ddd_mg")
    
    # 2. Group and summarize
    search_df <- df %>%
      filter(!is.na(atc_code)) %>%
      group_by(across(all_of(grp_cols))) %>%
      summarise(
        associated_drugs = paste(unique(vmp_nm[!is.na(vmp_nm) & vmp_nm != ""]), collapse = " | "),
        .groups = "drop"
      ) %>%
      arrange(atc_code)
      
    # Clean tibble properties (safeguard against json serialization issues)
    search_df <- as.data.frame(search_df)
    
    # 3. Use standard DT colnames replacement (passing an unnamed array matches by position)
    display_names <- c(
        "ATC Code", 
        "Defined Daily Dose (DDD)"
    )
    if("ddd_uom" %in% names(df)) display_names <- c(display_names, "Unit of Measure")
    if("ddd_mg" %in% names(df)) display_names <- c(display_names, "Normalised (mg)")
    
    display_names <- c(display_names, "Associated Drug Names (Searchable)")
    
    dt <- datatable(
      search_df,
      colnames = display_names,
      options = list(pageLength = 15, scrollX = TRUE, searchHighlight = TRUE),
      rownames = FALSE,
      filter = "top"
    )
    
    if("ddd_mg" %in% names(search_df)) {
      # Use the real dataframe name for formatting!
      dt <- dt %>% formatRound(columns = "ddd_mg", digits = 2)
    }
    
    dt
  })
}

shinyApp(ui, server)
