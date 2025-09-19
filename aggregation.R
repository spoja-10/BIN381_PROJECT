suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(purrr)
})

# CONFIG
transformed_folder <- "C:/Users/charl/OneDrive/Documents/BIN381_PROJECT/data/transformed_data"
aggregated_folder  <- "C:/Users/charl/OneDrive/Documents/BIN381_PROJECT/data/aggregated_data"

clean_output_dirs_first <- TRUE

# HELPER FUNCTIONS
read_csv_safe <- function(path) {
  tryCatch(read_csv(path, show_col_types = FALSE), error = function(e) { message("READ ERROR: ", e$message); NULL })
}

prepare_folder <- function() {
  if (!dir.exists(aggregated_folder)) dir.create(aggregated_folder, recursive = TRUE)
  if (clean_output_dirs_first) file.remove(list.files(aggregated_folder, full.names = TRUE))
}

# Prepare folder
prepare_folder()

# List transformed files
files <- list.files(transformed_folder, pattern = "\\.csv$", full.names = TRUE)
if (length(files) == 0) stop("No CSV files found in: ", transformed_folder)
message("Found files: ", paste(basename(files), collapse = ", "))

# Process each file
process_file <- function(f) {
  base <- tools::file_path_sans_ext(basename(f))
  message("\n--- Aggregating: ", base, " ---")
  
  df <- read_csv_safe(f)
  if (is.null(df)) return(NULL)
  
  # Grouping columns
  group_cols <- intersect(c("CountryName", "SurveyYear", "Indicator"), names(df))
  
  if (length(group_cols) < 2) {
    message("Insufficient columns for aggregation in ", base, ". Saving file with message.")
    out_file <- file.path(aggregated_folder, paste0(base, "_aggregated.csv"))
    readr::write_csv(tibble(message="Insufficient columns for aggregation"), out_file)
    return(out_file)
  }
  
  # Perform aggregation
  df_agg <- df %>%
    group_by(across(all_of(group_cols))) %>%
    summarise(
      mean_value = if("Value" %in% names(df)) mean(Value, na.rm = TRUE) else NA_real_,
      median_value = if("Value" %in% names(df)) median(Value, na.rm = TRUE) else NA_real_,
      nummber_of_observations = n(),
      .groups = "drop"
    )
  
  # Save aggregated file
  out_file <- file.path(aggregated_folder, paste0(base, "_aggregated.csv"))
  readr::write_csv(df_agg, out_file)
  message("Aggregated file saved: ", out_file)
  
  return(out_file)
}

# Run batch
aggregated_files <- map_chr(files, process_file)
message("\nAll files aggregated. Aggregated files saved in: ", aggregated_folder)
