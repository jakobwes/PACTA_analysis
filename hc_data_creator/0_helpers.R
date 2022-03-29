# Helpers -----------------------------------------------------------------

# Change parameters file with parameters for current abcd timestamp so that PACTA is run with correct abcd
change_project_params <- function(project_code, data_location_timestamp, timestamp, dataprep_timestamp, start_year_timestamp){
  params_file_path <- file.path("parameter_files", paste0("ProjectParameters_", project_code, ".yml"))

  project_parameters <- yaml::read_yaml(params_file_path)
  project_parameters$default$paths$data_location_ext <- data_location_timestamp
  project_parameters$default$parameters$timestamp <- timestamp
  project_parameters$default$parameters$dataprep_timestamp <- dataprep_timestamp
  project_parameters$default$parameters$start_year <- as.integer(start_year_timestamp)
  yaml::write_yaml(project_parameters, params_file_path)
}

# Write portfolio_csv
write_portfolio_csv <- function(portfolio, output_dir, portfolio_name_ref_all){
  portfolio %>%
    select(investor_name, portfolio_name, isin, market_value, currency) %>%
    group_by_all() %>% ungroup(market_value) %>%
    summarise(market_value = sum(market_value, na.rm = TRUE), .groups = "drop") %>%
    write_csv(file = file.path(output_dir, "20_Raw_Inputs", paste0(portfolio_name_ref_all,".csv")))

}

# Write  config_file
write_config_file <- function(output_dir, holdings_date, given_investor_name, given_portfolio_name, peer_group = paste0(given_portfolio_name, "_meta"), portfolio_name_ref_all = paste0(given_portfolio_name, "_user_", given_investor_name)){
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
  write_yaml(config_list, file = file.path(output_dir, "10_Parameter_File", paste0(portfolio_name_ref_all, "_PortfolioParameters.yml")))

}

get_asymmetric_combination <- function(fin_data_timestamp){
  if(fin_data_timestamp == 1) return(2) else if(fin_data_timestamp == 2) return(1) else stop("Invalid fin_data_timestamp")
}
