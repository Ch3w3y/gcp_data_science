library(dotenv)
library(arrow)
library(dplyr)
library(tidyr)
library(ggplot2)
library(scales)
library(patchwork)
library(stringr)
library(forcats)

# ── Configuration ──────────────────────────────────────────────────────────────
load_dot_env()
output_dir <- Sys.getenv("GCS_MOUNT_PATH", unset = "./data")
plot_dir   <- file.path(output_dir, "plots")
dir.create(plot_dir, showWarnings = FALSE, recursive = TRUE)

# ── Load Data ──────────────────────────────────────────────────────────────────
df <- read_parquet(file.path(output_dir, "item_24.parquet")) |>
  mutate(
    # Derive ATC hierarchy levels from the ATC code
    atc_level1_code = str_sub(atc_code, 1, 1),   # e.g. "J"
    atc_level2_code = str_sub(atc_code, 1, 3),   # e.g. "J01"
    atc_level3_code = str_sub(atc_code, 1, 4),   # e.g. "J01C"
    atc_level4_code = str_sub(atc_code, 1, 5),   # e.g. "J01CA"

    # Coverage flags
    has_bnf = !is.na(bnf_code) & bnf_code != "",
    has_atc = !is.na(atc_code) & atc_code != "",

    # Mapping status for traffic-light colouring
    mapping_status = case_when(
      has_atc & has_bnf ~ "ATC + BNF",
      has_bnf & !has_atc ~ "BNF only",
      !has_bnf & !has_atc ~ "Unmapped"
    ) |> factor(levels = c("ATC + BNF", "BNF only", "Unmapped")),

    # Antibiotic flag: ATC level 2 = J01 (Antibacterials for systemic use)
    is_antibiotic = str_starts(atc_code, "J01")
  )

# ATC level 1 anatomical group labels (WHO standard)
atc_l1_labels <- c(
  A = "Alimentary & Metabolism",
  B = "Blood & Blood Forming Organs",
  C = "Cardiovascular",
  D = "Dermatologicals",
  G = "Genito-Urinary & Sex Hormones",
  H = "Hormones (Non-Sex)",
  J = "Anti-Infectives (Systemic)",
  L = "Antineoplastic & Immunomodulating",
  M = "Musculoskeletal",
  N = "Nervous System",
  P = "Antiparasitic",
  R = "Respiratory",
  S = "Sensory Organs",
  V = "Various"
)

# ATC J01 subclass labels (level 3 — pharmacological subgroup)
j01_subclass_labels <- c(
  J01A = "Tetracyclines",
  J01B = "Amphenicols",
  J01C = "Beta-lactam Penicillins",
  J01D = "Other Beta-lactams (Cephalosporins etc.)",
  J01E = "Sulfonamides & Trimethoprim",
  J01F = "Macrolides, Lincosamides & Streptogramins",
  J01G = "Aminoglycoside Antibacterials",
  J01M = "Quinolone Antibacterials",
  J01R = "Combinations",
  J01X = "Other Antibacterials"
)

# ── Shared Theme ───────────────────────────────────────────────────────────────
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
  "BNF only"  = "#F57C00",
  "Unmapped"  = "#C62828"
)

save_plot <- function(plot, filename, width = 12, height = 7) {
  ggsave(
    file.path(plot_dir, filename),
    plot  = plot,
    width = width, height = height, dpi = 150
  )
  message("Saved: ", filename)
}

# ══════════════════════════════════════════════════════════════════════════════
# PLOT 1 — Overall Mapping Coverage Summary (stacked bar)
# ══════════════════════════════════════════════════════════════════════════════
coverage_summary <- df |>
  count(mapping_status) |>
  mutate(
    pct   = n / sum(n),
    label = paste0(comma(n), "\n(", percent(pct, accuracy = 0.1), ")")
  )

p1 <- ggplot(coverage_summary, aes(x = "", y = pct, fill = mapping_status)) +
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
    title    = "Overall dm+d VMP Mapping Coverage",
    subtitle = paste("Total VMPs:", comma(nrow(df))),
    x = NULL, y = "Proportion of VMPs",
    fill = "Coverage Status",
    caption = "Source: NHS TRUD Items 24 & 25"
  ) +
  theme_dmd()

save_plot(p1, "01_overall_coverage.png", height = 4)

# ══════════════════════════════════════════════════════════════════════════════
# PLOT 2 — Coverage by ATC Anatomical Level 1 Group
# ══════════════════════════════════════════════════════════════════════════════
p2_data <- df |>
  filter(has_atc) |>
  mutate(
    atc_group = atc_l1_labels[atc_level1_code],
    atc_group = coalesce(atc_group, paste0("Other (", atc_level1_code, ")"))
  ) |>
  count(atc_group, mapping_status) |>
  group_by(atc_group) |>
  mutate(pct = n / sum(n)) |>
  ungroup()

p2 <- ggplot(
  p2_data,
  aes(x = fct_reorder(atc_group, n, sum), y = n, fill = mapping_status)
) +
  geom_col() +
  coord_flip() +
  scale_fill_manual(values = status_colours) +
  scale_y_continuous(labels = comma) +
  labs(
    title    = "VMP Count by ATC Anatomical Group (Level 1)",
    subtitle = "Showing VMPs that have at least a BNF code",
    x = NULL, y = "Number of VMPs",
    fill = "Coverage Status",
    caption = "Source: NHS TRUD Items 24 & 25"
  ) +
  theme_dmd()

save_plot(p2, "02_coverage_by_atc_group.png")

# ══════════════════════════════════════════════════════════════════════════════
# PLOT 3 — Unmapped VMP Rate by ATC Level 1 Group
# ══════════════════════════════════════════════════════════════════════════════
p3_data <- df |>
  filter(!is.na(atc_level1_code) & atc_level1_code != "") |>
  mutate(atc_group = coalesce(atc_l1_labels[atc_level1_code], atc_level1_code)) |>
  group_by(atc_group) |>
  summarise(
    total    = n(),
    unmapped = sum(!has_atc),
    pct_unmapped = unmapped / total,
    .groups = "drop"
  ) |>
  filter(total >= 20) |>  # exclude tiny groups
  arrange(desc(pct_unmapped))

p3 <- ggplot(
  p3_data,
  aes(
    x    = fct_reorder(atc_group, pct_unmapped),
    y    = pct_unmapped,
    fill = pct_unmapped
  )
) +
  geom_col() +
  geom_text(
    aes(label = percent(pct_unmapped, accuracy = 1)),
    hjust = -0.1, size = 3.5, fontface = "bold"
  ) +
  coord_flip(clip = "off") +
  scale_fill_gradient(low = "#FFF9C4", high = "#B71C1C", labels = percent) +
  scale_y_continuous(labels = percent_format(), expand = expansion(mult = c(0, 0.15))) +
  labs(
    title    = "ATC Mapping Gap Rate by Anatomical Group",
    subtitle = "% of VMPs in each group that are missing an ATC code",
    x = NULL, y = "% Unmapped",
    fill = "% Unmapped",
    caption = "Source: NHS TRUD Items 24 & 25"
  ) +
  theme_dmd() +
  theme(legend.position = "none")

save_plot(p3, "03_unmapped_rate_by_group.png")

# ══════════════════════════════════════════════════════════════════════════════
# PLOT 4 — ANTIBIOTIC FOCUS: Coverage by J01 Subclass (Level 3)
# ══════════════════════════════════════════════════════════════════════════════
antibiotics <- df |>
  filter(is_antibiotic) |>
  mutate(
    j01_class = j01_subclass_labels[atc_level3_code],
    j01_class = coalesce(j01_class, paste0("Other (", atc_level3_code, ")"))
  )

p4_data <- antibiotics |>
  count(j01_class, mapping_status) |>
  group_by(j01_class) |>
  mutate(
    total = sum(n),
    pct   = n / total
  ) |>
  ungroup()

p4 <- ggplot(
  p4_data,
  aes(
    x    = fct_reorder(j01_class, total),
    y    = n,
    fill = mapping_status
  )
) +
  geom_col() +
  geom_text(
    data = p4_data |> group_by(j01_class) |> summarise(total = sum(n)),
    aes(x = j01_class, y = total, label = comma(total), fill = NULL),
    hjust = -0.15, size = 3.5, fontface = "bold"
  ) +
  coord_flip(clip = "off") +
  scale_fill_manual(values = status_colours) +
  scale_y_continuous(labels = comma, expand = expansion(mult = c(0, 0.15))) +
  labs(
    title    = "Antibiotic VMPs (ATC J01): Coverage by Subclass",
    subtitle  = "J01 = Antibacterials for systemic use (WHO ATC classification)",
    x = NULL, y = "Number of VMPs",
    fill = "Coverage Status",
    caption = "Source: NHS TRUD Items 24 & 25"
  ) +
  theme_dmd()

save_plot(p4, "04_antibiotic_coverage_by_subclass.png")

# ══════════════════════════════════════════════════════════════════════════════
# PLOT 5 — ANTIBIOTIC FOCUS: ATC Level 4 Breakdown (Chemical Subgroup)
# ══════════════════════════════════════════════════════════════════════════════
p5_data <- antibiotics |>
  filter(has_atc) |>
  count(atc_level4_code, atc_level3_code) |>
  mutate(
    j01_class = coalesce(j01_subclass_labels[atc_level3_code], atc_level3_code)
  ) |>
  group_by(j01_class) |>
  mutate(pct_within_class = n / sum(n)) |>
  ungroup() |>
  arrange(j01_class, desc(n))

p5 <- ggplot(
  p5_data,
  aes(
    x    = fct_reorder(atc_level4_code, n),
    y    = n,
    fill = j01_class
  )
) +
  geom_col() +
  coord_flip() +
  scale_y_continuous(labels = comma) +
  scale_fill_brewer(palette = "Set3") +
  labs(
    title    = "Antibiotic VMPs: ATC Level 4 Chemical Subgroup Distribution",
    subtitle = "Each bar = one ATC level-4 code, coloured by J01 pharmacological subclass",
    x = "ATC Level 4 Code", y = "Number of VMPs",
    fill = "J01 Subclass",
    caption = "Source: NHS TRUD Items 24 & 25"
  ) +
  theme_dmd() +
  theme(legend.position = "right")

save_plot(p5, "05_antibiotic_atc_level4.png", height = 10)

# ══════════════════════════════════════════════════════════════════════════════
# PLOT 6 — ANTIBIOTIC FOCUS: DDD Coverage (which J01 drugs have DDD values)
# ══════════════════════════════════════════════════════════════════════════════
p6_data <- antibiotics |>
  mutate(has_ddd = !is.na(ddd) & ddd != "") |>
  group_by(j01_class) |>
  summarise(
    total    = n(),
    with_ddd = sum(has_ddd),
    pct_ddd  = with_ddd / total,
    .groups  = "drop"
  )

p6 <- ggplot(
  p6_data,
  aes(
    x    = fct_reorder(j01_class, pct_ddd),
    y    = pct_ddd,
    fill = pct_ddd
  )
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
    title    = "Defined Daily Dose (DDD) Availability for Antibiotic VMPs",
    subtitle = "% of VMPs within each J01 subclass that have a DDD value recorded",
    x = NULL, y = "% with DDD value",
    fill = "% with DDD",
    caption = "Source: NHS TRUD Item 25 — BNF/ATC supplementary data"
  ) +
  theme_dmd() +
  theme(legend.position = "none")

save_plot(p6, "06_antibiotic_ddd_coverage.png")

# ══════════════════════════════════════════════════════════════════════════════
# PLOT 7 — Summary Dashboard (patchwork composite)
# ══════════════════════════════════════════════════════════════════════════════

# Compact versions of key plots for the dashboard
p_dash_overall <- coverage_summary |>
  ggplot(aes(x = fct_rev(mapping_status), y = pct, fill = mapping_status)) +
  geom_col(show.legend = FALSE) +
  geom_text(
    aes(label = percent(pct, accuracy = 0.1)),
    hjust = -0.1, fontface = "bold", size = 4
  ) +
  coord_flip(clip = "off") +
  scale_fill_manual(values = status_colours) +
  scale_y_continuous(expand = expansion(mult = c(0, 0.2))) +
  labs(title = "A) Overall Coverage", x = NULL, y = NULL) +
  theme_dmd()

antibiotic_summary <- antibiotics |>
  summarise(
    total      = n(),
    fully_mapped = sum(has_atc & has_bnf),
    bnf_only   = sum(has_bnf & !has_atc),
    unmapped   = sum(!has_bnf & !has_atc)
  ) |>
  pivot_longer(c(fully_mapped, bnf_only, unmapped),
               names_to = "status", values_to = "n") |>
  mutate(
    pct = n / total,
    status = recode(status,
      fully_mapped = "ATC + BNF",
      bnf_only     = "BNF only",
      unmapped     = "Unmapped"
    ) |> factor(levels = c("ATC + BNF", "BNF only", "Unmapped"))
  )

p_dash_abx <- ggplot(antibiotic_summary, aes(x = fct_rev(status), y = pct, fill = status)) +
  geom_col(show.legend = FALSE) +
  geom_text(
    aes(label = percent(pct, accuracy = 0.1)),
    hjust = -0.1, fontface = "bold", size = 4
  ) +
  coord_flip(clip = "off") +
  scale_fill_manual(values = status_colours) +
  scale_y_continuous(expand = expansion(mult = c(0, 0.2))) +
  labs(title = "B) Antibiotic (J01) Coverage", x = NULL, y = NULL) +
  theme_dmd()

p_dash_subclass <- p4_data |>
  group_by(j01_class) |>
  summarise(total = sum(n), .groups = "drop") |>
  ggplot(aes(x = fct_reorder(j01_class, total), y = total)) +
  geom_col(fill = "#1565C0") +
  geom_text(aes(label = comma(total)), hjust = -0.1, size = 3) +
  coord_flip(clip = "off") +
  scale_y_continuous(labels = comma, expand = expansion(mult = c(0, 0.2))) +
  labs(title = "C) VMP Count by J01 Subclass", x = NULL, y = NULL) +
  theme_dmd()

dashboard <- (p_dash_overall / p_dash_abx) | p_dash_subclass +
  plot_annotation(
    title    = "dm+d SNOMED → BNF → ATC Mapping Coverage Dashboard",
    subtitle = paste("Generated:", format(Sys.time(), "%Y-%m-%d %H:%M %Z")),
    caption  = "Source: NHS TRUD Items 24 & 25",
    theme = theme(
      plot.title    = element_text(face = "bold", size = 16),
      plot.subtitle = element_text(colour = "grey40")
    )
  )

save_plot(dashboard, "07_dashboard.png", width = 16, height = 10)

# ── Console Summary ────────────────────────────────────────────────────────────
cat("\n════════════════════════════════════════\n")
cat(" dm+d Mapping Coverage Summary\n")
cat("════════════════════════════════════════\n")
cat(sprintf(" Total VMPs              : %s\n",   comma(nrow(df))))
cat(sprintf(" With BNF code           : %s (%.1f%%)\n",
    comma(sum(df$has_bnf)), mean(df$has_bnf) * 100))
cat(sprintf(" With ATC code           : %s (%.1f%%)\n",
    comma(sum(df$has_atc)), mean(df$has_atc) * 100))
cat(sprintf(" Antibiotic VMPs (J01)   : %s\n",   comma(sum(df$is_antibiotic))))
cat(sprintf(" Plots saved to          : %s\n",   plot_dir))
cat("════════════════════════════════════════\n\n")