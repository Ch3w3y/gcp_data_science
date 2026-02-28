#' Prepare dm+d Mapping Data
#'
#' @description Reads the Parquet spine dataset and appends calculated attributes
#' for ATC levels, mapping continuity, and antimicrobial classification.
#'
#' @param file_path character. The absolute path to the Parquet mapping file.
#'
#' @return A tbl_df / data.frame with the newly constructed attributes.
#' @export
#'
#' @importFrom arrow read_parquet
#' @importFrom dplyr mutate case_when
#' @importFrom stringr str_sub str_starts
prepare_dmd_data <- function(file_path) {
  df <- arrow::read_parquet(file_path) %>%
    dplyr::mutate(
      atc_level1_code = stringr::str_sub(atc_code, 1, 1),
      atc_level2_code = stringr::str_sub(atc_code, 1, 3),
      atc_level3_code = stringr::str_sub(atc_code, 1, 4),
      atc_level4_code = stringr::str_sub(atc_code, 1, 5),
      
      has_bnf = !is.na(bnf_code) & bnf_code != "",
      has_atc = !is.na(atc_code) & atc_code != "",
      
      mapping_status = dplyr::case_when(
        has_atc & has_bnf ~ "ATC + BNF",
        has_bnf & !has_atc ~ "BNF only",
        !has_bnf & !has_atc ~ "Unmapped"
      ) %>% factor(levels = c("ATC + BNF", "BNF only", "Unmapped")),
      
      is_antibiotic = stringr::str_starts(atc_code, "J01") | stringr::str_starts(bnf_code, "0501")
    )
  return(df)
}
