library(testthat)
library(dplyr)
library(arrow)

source("../../R/data_utils.R")

test_that("Antibiotics are correctly identified from ATC and BNF codes", {
  
  # Create a mock dataframe mimicking the parquet file
  mock_data <- data.frame(
    vpid = c("1", "2", "3", "4"),
    atc_code = c("J01CA04", NA, "N02BE01", NA),
    bnf_code = c("050101", "050102", "040701", NA),
    stringsAsFactors = FALSE
  )
  
  # Write temporarily to parquet to use the function logic natively
  temp_file <- tempfile(fileext = ".parquet")
  write_parquet(mock_data, temp_file)
  
  # Process using data_utils
  result <- prepare_dmd_data(temp_file)
  
  # Clean up
  file.remove(temp_file)
  
  # Assertions
  expect_equal(nrow(result), 4)
  
  # 1: J01 and 0501 -> Should be antibiotic
  expect_true(result$is_antibiotic[1])
  
  # 2: NA ATC, 0501 BNF -> Should be antibiotic (Catches the unmapped ones!)
  expect_true(result$is_antibiotic[2])
  
  # 3: N02 ATC, 0407 BNF -> Should NOT be antibiotic
  expect_false(result$is_antibiotic[3])
  
  # 4: Completely unmapped -> Should NOT be antibiotic
  expect_false(result$is_antibiotic[4])
})
