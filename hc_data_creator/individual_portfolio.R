# load required packages -------------------------------------------------------

require(R.utils, quietly = TRUE, warn.conflicts = FALSE)
require(tibble, quietly = TRUE)
require(fs, quietly = TRUE)
require(cli, quietly = TRUE)
require(stringi, quietly = TRUE)
require(wand, quietly = TRUE)
require(stringr, quietly = TRUE)

library(dplyr, warn.conflicts = FALSE)
library(devtools, quietly = TRUE, warn.conflicts = FALSE)
library(purrr)
library(stringr)
library(fs)
library(r2dii.utils)  # must install with # devtools::install_github("2DegreesInvesting/r2dii.utils")
library(readr)
library(yaml)

devtools::load_all(quiet = TRUE)

# Input:
# Two portfolios and params giving evaluation-timestamps


# manually set certain values and paths ----------------------------------------

# WARNING!!! These filepaths are easy to mess up. You're much better off
# copy-pasting them from your filesystem rather than trying to manually edit
# bits of it. Seriously. Trust me.

# Timestamps and params:
timestamps <- c("2019Q4", "2020Q4")
data_location_timestamps <- c("../pacta-data/2019Q4/", "../pacta-data/2020Q4/")
dataprep_timestamps <- c("2019Q4_transitionmonitor", "2020Q4_transitionmonitor")
start_year_timestamps <- c(2020, 2021)

# Data paths for portfolios timestamp t1 and t2
portfolio_path_t1 <- "../portfolios_lux_t1/portfolios/29740.csv"
portfolio_path_t2 <- "../portfolios_lux_t2/portfolios/29740.csv"

# Meta
project_code <- "PA2021LU"
default_language <- "EN"
given_investor_name <- "test"
given_portfolio_name <- "test"

output_dir <- file.path("output", given_portfolio_name)


# Change parameters file with parameters for current abcd timestamp so that PACTA is run with correct abcd
change_project_params <- function(project_code, data_location_timestamp, timestamp, dataprep_timestamp, start_year_timestamp){
  params_file_path <- file.path("parameter_files", paste0("ProjectParameters_", project_code, ".yml"))

  project_parameters <- yaml::read_yaml(params_file_path)
  project_parameters$default$paths$data_location_ext <- data_location_timestamp
  project_parameters$default$parameters$timestamp <- timestamp
  project_parameters$default$parameters$dataprep_timestamp <- dataprep_timestamp
  project_parameters$default$parameters$start_year <- start_year_timestamp
  yaml::write_yaml(project_parameters, params_file_path)
}

# Write portfolio_csv
write_portfolio_csv <- function(portfolio, output_dir){
  portfolio %>%
    select(given_investor_name, given_portfolio_name, isin, market_value, currency) %>%
    group_by_all() %>% ungroup(market_value) %>%
    summarise(market_value = sum(market_value, na.rm = TRUE), .groups = "drop") %>%
    write_csv(file = file.path(output_dir, "20_Raw_Inputs", paste0(given_portfolio_name, "_user_", given_investor_name, ".csv")))

}

# Write  config_file
write_config_file <- function(output_dir, holdings_date){
  config_list <-
    list(
      default = list(
        parameters = list(
          portfolio_name = given_investor_name,
          investor_name = given_portfolio_name,
          peer_group = paste0(given_portfolio_name, "_meta"),
          language = default_language,
          project_code = project_code,
          holdings_date = holdings_date
        )
      )
    )
  write_yaml(config_list, file = file.path(output_dir, "10_Parameter_File", paste0(given_portfolio_name, "_user_", given_investor_name, "_PortfolioParameters.yml")))

}

# write and check paths and directories calculations ------------------------------------------------------

output_dir_fin_data_t1 <- file.path(output_dir, paste0("fin_data_", timestamps[1]))
output_dir_fin_data_t2 <- file.path(output_dir, paste0("fin_data_", timestamps[2]))

dir.create(output_dir, showWarnings = FALSE)
dir.create(output_dir_fin_data_t1, showWarnings = FALSE)
dir.create(output_dir_fin_data_t2, showWarnings = FALSE)

stopifnot(dir.exists(output_dir))
stopifnot(dir.exists(output_dir_fin_data_t1))
stopifnot(dir.exists(output_dir_fin_data_t2))


# read in all the CSVs ---------------------------------------------------------

# load needed function and set needed values
source("hc_data_creator/read_portfolio_csv.R")

portfolio_t1 <- read_portfolio_csv(portfolio_path_t1) %>% mutate(given_investor_name = given_investor_name, given_portfolio_name = given_portfolio_name)
portfolio_t2 <- read_portfolio_csv(portfolio_path_t2) %>% mutate(given_investor_name = given_investor_name, given_portfolio_name = given_portfolio_name)


# prepare PACTA project ---------------------------------------------------

pacta_directories <- c("00_Log_Files", "10_Parameter_File", "20_Raw_Inputs", "30_Processed_Inputs", "40_Results", "50_Outputs")
dir_create(file.path(output_dir_fin_data_t1, pacta_directories))
dir_create(file.path(output_dir_fin_data_t2, pacta_directories))

write_portfolio_csv(portfolio_t1, output_dir_fin_data_t1)
write_config_file(output_dir_fin_data_t1, timestamps[1])

write_portfolio_csv(portfolio_t2, output_dir_fin_data_t2)
write_config_file(output_dir_fin_data_t2, timestamps[2])


# fin_data t1: run portfolio through PACTA ---------------------------------------------

message("running t1 portfolio through PACTA")

# Setup:
change_project_params(project_code, data_location_timestamps[1], timestamps[1], dataprep_timestamps[1], start_year_timestamps[1])
dir_delete("working_dir")
dir_copy(output_dir_fin_data_t1, "working_dir", overwrite = TRUE)

# Run financial data processing part of pacta
portfolio_name_ref_all <- paste0(given_portfolio_name, "_user_", given_investor_name)
investor_name <- given_investor_name
portfolio_name <- given_portfolio_name
source("web_tool_script_1.R", local = TRUE)
dir_copy(file.path("working_dir", "10_Parameter_File"), file.path(output_dir_fin_data_t1, "10_Parameter_File"), overwrite = TRUE)
dir_copy(file.path("working_dir", "20_Raw_Inputs"), file.path(output_dir_fin_data_t1, "20_Raw_Inputs"), overwrite = TRUE)
dir_copy(file.path("working_dir", "30_Processed_Inputs"), file.path(output_dir_fin_data_t1, "30_Processed_Inputs"), overwrite = TRUE)

# abcd t1
source("web_tool_script_2.R", local = TRUE)
dir.create(file.path(output_dir_fin_data_t1, "40_Results", paste0("abcd_", timestamps[1])))
dir_copy("working_dir/40_Results", file.path(output_dir_fin_data_t1, "40_Results", paste0("abcd_", timestamps[1])), overwrite = TRUE)

# abcd t2
change_project_params(project_code, data_location_timestamps[2], timestamps[2], dataprep_timestamps[2], start_year_timestamps[2]) # This is here, because the rollup giving 30_Processed_Outputs needs to happen with t1 financial data. Otherwise new investments by funds will be missing if done with t2 financial data
source("web_tool_script_2.R", local = TRUE)
dir.create(file.path(output_dir_fin_data_t1, "40_Results", paste0("abcd_", timestamps[2])))
dir_copy("working_dir/40_Results", file.path(output_dir_fin_data_t1, "40_Results", paste0("abcd_", timestamps[2])), overwrite = TRUE)


# fin_data t2: run portfolio through PACTA ---------------------------------------------

message("running t2 portfolio through PACTA")

# Setup:
change_project_params(project_code, data_location_timestamps[2], timestamps[2], dataprep_timestamps[2], start_year_timestamps[2])
dir_delete("working_dir")
dir_copy(output_dir_fin_data_t2, "working_dir", overwrite = TRUE)

# Run financial data processing part of pacta
portfolio_name_ref_all <- paste0(given_portfolio_name, "_user_", given_investor_name)
investor_name <- given_investor_name
portfolio_name <- given_portfolio_name
source("web_tool_script_1.R", local = TRUE)
dir_copy(file.path("working_dir", "10_Parameter_File"), output_dir_fin_data_t2, overwrite = TRUE)
dir_copy(file.path("working_dir", "20_Raw_Inputs"), output_dir_fin_data_t2, overwrite = TRUE)
dir_copy(file.path("working_dir", "30_Processed_Inputs"), output_dir_fin_data_t2, overwrite = TRUE)

# abcd t2
source("web_tool_script_2.R", local = TRUE)
dir.create(file.path(output_dir_fin_data_t2, "40_Results", paste0("abcd_", timestamps[2])))
dir_copy("working_dir/40_Results", file.path(output_dir_fin_data_t2, "40_Results", paste0("abcd_", timestamps[2])), overwrite = TRUE)

# abcd t1
change_project_params(project_code, data_location_timestamps[1], timestamps[1], dataprep_timestamps[1], start_year_timestamps[1])# This is here, because the rollup giving 30_Processed_Outputs needs to happen with t2 financial data. Otherwise divestments by funds will be missing if done with t1 financial data
source("web_tool_script_2.R", local = TRUE)
dir.create(file.path(output_dir_fin_data_t2, "40_Results", paste0("abcd_", timestamps[1])))
dir_copy("working_dir/40_Results", file.path(output_dir_fin_data_t2, "40_Results", paste0("abcd_", timestamps[1])), overwrite = TRUE)

