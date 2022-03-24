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
# Portfolio csvs from both years
# Portfolios meta csv from both years
# Users meta csv from both years



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
data_paths <- c(
  "../portfolios_lux_t1", # r2dii.utils::path_dropbox_2dii("2° Investing Team/1. RESEARCH/1. Studies (projects)/PACTA . Regulator Monitoring/PACTA COP/03_Initiative_Level/04_PACTACOP_LU/04_Input_Cleaning"),
  "../portfolios_lux_t2"  # r2dii.utils::path_dropbox_2dii("2° Investing Team/1. RESEARCH/1. Studies (projects)/PACTA . Regulator Monitoring/PACTA COP/03_Initiative_Level/04_PACTACOP_LU/04_Input_Cleaning")
)
portfolios_paths <- file.path(data_paths, "portfolios")
portfolios_meta_csvs <- file.path(data_paths, "portfolios.csv")
users_meta_csvs <- file.path(data_paths, "users.csv")


# Meta
project_code <- "PA2021LU"
default_language <- "EN"

project_prefix <- "luxembourg"

bogus_csvs_to_be_ignored <- c()  # if none, this should be c()



# Start calculations ------------------------------------------------------

dir.create(project_prefix)


# Subset users so that only the ones which participated in both years are included --------

users_meta_t1 <- read_csv(users_meta_csvs[1], col_types = "ncccccn", skip = 1L, col_names = c("id", "email_canonical", "organization_type", "organization_name", "job_title", "country", "has_portfolios"))
users_meta_t2 <- read_csv(users_meta_csvs[2], col_types = "ncccccn", skip = 1L, col_names = c("id", "email_canonical", "organization_type", "organization_name", "job_title", "country", "has_portfolios"))
users_meta_both_years <- users_meta_t2 %>% filter(id %in% users_meta_t1$id) # Take metainformation from t2, but filter to make sure that also attended t1
users_meta_both_years <- users_meta_both_years %>% mutate(
  path_fin_data_t1_abcd_t1 = NA,
  path_fin_data_t1_abcd_t2 = NA,
  path_fin_data_t2_abcd_t1 = NA,
  path_fin_data_t2_abcd_t2 = NA
)


# Iterate through ---------------------------------------------------------

t_type <- c("t1", "t2")
for(fin_data_timestamp in 1:2){#1:2

  data_path <- data_paths[fin_data_timestamp]
  portfolios_path <- portfolios_paths[fin_data_timestamp]
  portfolios_meta_csv <- portfolios_meta_csvs[fin_data_timestamp]

  stopifnot(dir.exists(portfolios_path))
  stopifnot(file.exists(portfolios_meta_csv))

  for(abcd_timestamp in 1:2){#1:2

    message(paste0("running financial data ", timestamps[fin_data_timestamp], " with abcd ", timestamps[abcd_timestamp], " through PACTA"))

    holdings_date <- timestamps[abcd_timestamp]

    # Change parameters file with parameters for current abcd timestamp so that PACTA is run with correct abcd
    params_file_path <- file.path("parameter_files", paste0("ProjectParameters_", project_code, ".yml"))

    project_parameters <- yaml::read_yaml(params_file_path)
    project_parameters$default$paths$data_location_ext <- data_location_timestamps[abcd_timestamp]
    project_parameters$default$parameters$timestamp <- timestamps[abcd_timestamp]
    project_parameters$default$parameters$dataprep_timestamp <- dataprep_timestamps[abcd_timestamp]
    project_parameters$default$parameters$start_year <- start_year_timestamps[abcd_timestamp]
    yaml::write_yaml(project_parameters, params_file_path)
    rm(project_parameters)

    # check paths and directories --------------------------------------------------
    output_dir <- file.path(project_prefix, paste0("fin_data_", timestamps[fin_data_timestamp]), paste0("abcd_", timestamps[abcd_timestamp]))  # this will likely not work on Windows, so change it!
    combined_user_results_output_dir <- file.path(output_dir, "combined", "user_level")

    dir.create(output_dir, showWarnings = FALSE)
    dir.create(combined_user_results_output_dir, showWarnings = FALSE, recursive = TRUE)
    dir.create(file.path(combined_user_results_output_dir, "30_Processed_inputs"), showWarnings = FALSE)
    dir.create(file.path(combined_user_results_output_dir, "40_Results"), showWarnings = FALSE)

    stopifnot(dir.exists(output_dir))
    stopifnot(dir.exists(file.path(combined_user_results_output_dir, "30_Processed_inputs")))
    stopifnot(dir.exists(file.path(combined_user_results_output_dir, "40_Results")))


    # load needed function and set needed values

    source("meta_report_data_creator/read_portfolio_csv.R")

    pacta_directories <- c("00_Log_Files", "10_Parameter_File", "20_Raw_Inputs", "30_Processed_Inputs", "40_Results", "50_Outputs")


    # prepare a list of all the CSVs to import -------------------------------------

    portfolio_csvs <- list.files(portfolios_path, pattern = "[.]csv$", full.names = TRUE)


    # read in portfolio meta data CSVs -------------------------------------------------------

    portfolios_meta <- read_csv(portfolios_meta_csv, col_types = "nnnccnc")


    # remove unsubmitted CSVs ------------------------------------------------------

    unsubmitted_ids <- portfolios_meta$id[portfolios_meta$submitted == 0]
    portfolio_csvs <- portfolio_csvs[!tools::file_path_sans_ext(basename(portfolio_csvs)) %in% unsubmitted_ids]


    # remove bogus CSVs ------------------------------------------------------------
    portfolio_csvs <- portfolio_csvs[! tools::file_path_sans_ext(basename(portfolio_csvs)) %in% bogus_csvs_to_be_ignored]


    # read in all the specs --------------------------------------------------------

    specs <- get_csv_specs(portfolio_csvs)
    saveRDS(specs, file.path(output_dir, paste0(project_prefix, "_csv_specs.rds")))
    portfolio_csvs <- specs$filepath


    # read in all the CSVs ---------------------------------------------------------

    data <- map_dfr(set_names(portfolio_csvs, portfolio_csvs), read_portfolio_csv, .id = "csv_name")


    # add meta data to full data and save it ---------------------------------------

    data <-
      data %>%
      mutate(port_id = suppressWarnings(as.numeric(tools::file_path_sans_ext(basename(csv_name))))) %>%
      left_join(portfolios_meta[, c("id", "user_id")], by = c(port_id = "id")) %>%
      left_join(users_meta_both_years[, c("id", "organization_type")], by = c(user_id = "id"))

    data <-
      data %>%
      filter(!is.na(port_id)) %>%
      filter(!is.na(user_id))

    # `write_csv()` sometimes fails on Windows and is not necessary, so commented out until solved
    # write_csv(data, file = file.path(output_dir, paste0(project_prefix, "_full.csv")))
    saveRDS(data, file.path(output_dir, paste0(project_prefix, "_full.rds")))


    # prepare meta PACTA project ---------------------------------------------------

    meta_output_dir <- file.path(output_dir, "meta")
    dir.create(meta_output_dir, showWarnings = FALSE)

    dir_create(file.path(meta_output_dir, pacta_directories))

    data %>%
      mutate(portfolio_name = "Meta Portfolio") %>%
      mutate(investor_name = "Meta Investor") %>%
      select(investor_name, portfolio_name, isin, market_value, currency) %>%
      group_by_all() %>% ungroup(market_value) %>%
      summarise(market_value = sum(market_value, na.rm = TRUE), .groups = "drop") %>%
      write_csv(file = file.path(meta_output_dir, "20_Raw_Inputs", paste0(project_prefix, "_meta.csv")))

    config_list <-
      list(
        default = list(
          parameters = list(
            portfolio_name = "Meta Portfolio",
            investor_name = "Meta Investor",
            peer_group = paste0(project_prefix, "_meta"),
            language = default_language,
            project_code = project_code,
            holdings_date = holdings_date
          )
        )
      )
    write_yaml(config_list, file = file.path(meta_output_dir, "10_Parameter_File", paste0(project_prefix, "_meta", "_PortfolioParameters.yml")))


    # slices for per user_id -------------------------------------------------------

    users_output_dir <- file.path(output_dir, "user_id")
    dir.create(users_output_dir, showWarnings = FALSE)

    all_user_ids <- unique(data$user_id)

    for (user_id in all_user_ids) {
      user_data <- data %>% dplyr::filter(user_id == .env$user_id)

      investor_name <- encodeString(as.character(unique(user_data$investor_name)))
      if (length(investor_name) > 1) {
        investor_name <- investor_name[[1]]
        user_data <- user_data %>% mutate(investor_name = .env$investor_name)
      }

      user_data <- user_data %>% mutate(portfolio_name = .env$investor_name)

      peer_group <- unique(user_data$organization_type)
      if (length(peer_group) > 1) { peer_group <- peer_group[[1]] }

      config_list <-
        list(
          default = list(
            parameters = list(
              portfolio_name = as.character(investor_name),
              investor_name = as.character(investor_name),
              peer_group = peer_group,
              language = default_language,
              project_code = project_code,
              holdings_date = holdings_date
            )
          )
        )

      user_id_output_dir <- file.path(users_output_dir, paste0(project_prefix, "_user_", user_id))
      dir_create(file.path(user_id_output_dir, pacta_directories))

      write_yaml(config_list, file = file.path(user_id_output_dir, "10_Parameter_File", paste0(project_prefix, "_user_", user_id, "_PortfolioParameters.yml")))

      user_data %>%
        select(investor_name, portfolio_name, isin, market_value, currency) %>%
        write_csv(file.path(user_id_output_dir, "20_Raw_Inputs", paste0(project_prefix, "_user_", user_id, ".csv")))
    }


    # run meta portfolio through PACTA ---------------------------------------------

    message("running meta portfolio through PACTA")

    dir_delete("working_dir")
    dir_copy(meta_output_dir, "working_dir", overwrite = TRUE)

    portfolio_name_ref_all <- paste0(project_prefix, "_meta")

    source("web_tool_script_1.R", local = TRUE)
    source("web_tool_script_2.R", local = TRUE)

    dir_delete(meta_output_dir)
    dir_copy("working_dir", meta_output_dir, overwrite = TRUE)


    # run user files through PACTA -------------------------------------------------

    for (user_id in all_user_ids) {
      message(paste0("running user_id: ", user_id, " through PACTA"))

      user_id_output_dir <- file.path(users_output_dir, paste0(project_prefix, "_user_", user_id))

      # Save in user-df where the results were saved
      results_calculated <- paste0("fin_data_", t_type[fin_data_timestamp], "_abcd_", t_type[abcd_timestamp])
      users_meta_both_years <- users_meta_both_years %>%
        mutate(
          !!as.symbol(paste0("path_", results_calculated)) := replace(!!as.symbol(paste0("path_", results_calculated)), id == user_id, user_id_output_dir)
        )

      dir_delete("working_dir")
      dir_copy(user_id_output_dir, "working_dir", overwrite = TRUE)

      portfolio_name_ref_all <- paste0(project_prefix, "_user_", user_id)

      source("web_tool_script_1.R", local = TRUE)
      source("web_tool_script_2.R", local = TRUE)

      dir_delete(user_id_output_dir)
      dir_copy("working_dir", user_id_output_dir, overwrite = TRUE)

    }


    # # combine all user level results -----------------------------------------------
    #
    # data_filenames <-
    #   c(
    #     "Bonds_results_portfolio.rda",
    #     "Bonds_results_company.rda",
    #     "Bonds_results_map.rda",
    #     "Equity_results_portfolio.rda",
    #     "Equity_results_company.rda",
    #     "Equity_results_map.rda"
    #   )
    #
    # lapply(data_filenames, function(data_filename) {
    #   portfolio_result_filepaths <- list.files(users_output_dir, pattern = data_filename, recursive = TRUE, full.names = TRUE)
    #   meta_result_filepaths <- list.files(meta_output_dir, pattern = data_filename, recursive = TRUE, full.names = TRUE)
    #   all_result_filepaths <- c(portfolio_result_filepaths, meta_result_filepaths)
    #   all_result_filepaths <- all_result_filepaths <- setNames(
    #     all_result_filepaths,
    #     c(
    #       sub(paste0("^", project_prefix, "_user_"), "", basename(dirname(portfolio_result_filepaths))),
    #       "Meta Portfolio"
    #     )
    #   )
    #
    #   all_results <-
    #     map_df(all_result_filepaths, readRDS, .id = "user_id") %>%
    #     mutate(portfolio_name = user_id) %>%
    #     left_join(mutate(users_meta_both_years, id = as.character(id)) %>% select(user_id = id, organization_type), by = "user_id") %>%
    #     rename(peergroup = organization_type) %>%
    #     mutate(investor_name = if_else(portfolio_name == "Meta Portfolio", "Meta Investor", peergroup)) %>%
    #     select(-peergroup, -user_id)
    #
    #   saveRDS(all_results, file.path(combined_user_results_output_dir, "40_Results", data_filename))
    # })
    #
    # data_filenames <-
    #   c(
    #     "overview_portfolio.rda",
    #     "total_portfolio.rda",
    #     "emissions.rda"
    #   )
    #
    # lapply(data_filenames, function(data_filename) {
    #   portfolio_result_filepaths <- list.files(users_output_dir, pattern = data_filename, recursive = TRUE, full.names = TRUE)
    #   meta_result_filepaths <- list.files(meta_output_dir, pattern = data_filename, recursive = TRUE, full.names = TRUE)
    #   all_result_filepaths <- c(portfolio_result_filepaths, meta_result_filepaths)
    #   all_result_filepaths <- all_result_filepaths <- setNames(
    #     all_result_filepaths,
    #     c(
    #       sub(paste0("^", project_prefix, "_user_"), "", basename(dirname(portfolio_result_filepaths))),
    #       "Meta Portfolio"
    #     )
    #   )
    #
    #   all_results <-
    #     map_df(all_result_filepaths, readRDS, .id = "user_id") %>%
    #     mutate(portfolio_name = user_id) %>%
    #     left_join(mutate(users_meta_both_years, id = as.character(id)) %>% select(user_id = id, organization_type), by = "user_id") %>%
    #     rename(peergroup = organization_type) %>%
    #     mutate(investor_name = if_else(portfolio_name == "Meta Portfolio", "Meta Investor", peergroup)) %>%
    #     select(-c(peergroup, user_id))
    #
    #   saveRDS(all_results, file.path(combined_user_results_output_dir, "30_Processed_inputs", data_filename))
    # })

    # Write to file at the end of each iteration
    write_csv(users_meta_both_years, file.path(project_prefix, "users_meta_both_years.csv"))
  }
}

# Filter the users that participated in both years but where for whatever reason one of the portfolio-combinations is not given (ie. they did not submit their portfolio in one of the years)
# users_meta_both_years <- users_meta_both_years %>% filter(
#   !is.na(path_fin_data_t1_abcd_t1),
#   !is.na(path_fin_data_t1_abcd_t2),
#   !is.na(path_fin_data_t2_abcd_t1),
#   !is.na(path_fin_data_t2_abcd_t2)
# )
