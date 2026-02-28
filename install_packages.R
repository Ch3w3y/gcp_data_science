# Point to Noble (Ubuntu 24.04) binary repository for speed
options(repos = c(CRAN = "https://packagemanager.posit.co/cran/__linux__/noble/latest"))

install.packages(c(
  "DBI", 
  "odbc", 
  "tidyverse", 
  "zoo", 
  "ggplot2", 
  "plotly", 
  "duckdb", 
  "arrow", 
  "dotenv",
  "stringr",
  "patchwork",
  "scales",
  "forcats"
))
