suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(purrr)
  library(stringr)
})

# CONFIG
raw_folder         <- "C:/Users/charl/OneDrive/Documents/BIN381_PROJECT/data/raw" 
transformed_folder <- "C:/Users/charl/OneDrive/Documents/BIN381_PROJECT/data/transformed_data"  

manual_drop_cols <- c(
  "DataId","ISO3","SurveyId","IndicatorId","IndicatorOrder","Precision", "DHS_CountryCode", "CountryName", "IndicatorType",
  "CharacteristicId","CharacteristicOrder","ByVariableId",
  "SDRID","RegionId","CILow","CIHigh","LevelRank","SurveyType","SurveyYearLabel",
  "DenominatorWeighted", "DenominatorUnweighted", "IsTotal", "IsPreferred", "CharacteristicLabel"
)

clean_output_dirs_first <- TRUE

# HELPER FUNCTIONS
read_csv_safe <- function(path) {
  tryCatch(read_csv(path, show_col_types = FALSE), error = function(e) { message("READ ERROR: ", e$message); NULL })
}

parse_numeric_safe <- function(x) suppressWarnings(as.numeric(str_replace_all(as.character(x), "%", "")))

prepare_folder <- function() {
  if (!dir.exists(transformed_folder)) dir.create(transformed_folder, recursive = TRUE)
  if (clean_output_dirs_first) file.remove(list.files(transformed_folder, full.names = TRUE))
}

#Prepare folder
prepare_folder()

# List files
files <- list.files(raw_folder, pattern = "\\.csv$", full.names = TRUE)
if (length(files) == 0) stop("No CSV files found in: ", raw_folder)
message("Found files: ", paste(basename(files), collapse = ", "))

# Process each file
process_file <- function(f) {
  base <- tools::file_path_sans_ext(basename(f))
  message("\n--- Processing: ", base, " ---")
  
  df <- read_csv_safe(f)
  if (is.null(df)) return(NULL)
  
  # Remove unwanted columns
  df <- df %>% select(-any_of(manual_drop_cols))
  
  # Remove duplicate rows
  df <- df %>% distinct()
  
  # Transformations
  if ("Value" %in% names(df)) df$Value <- parse_numeric_safe(df$Value)
  if ("SurveyYear" %in% names(df)) df$SurveyYear <- as.integer(df$SurveyYear)
  
  # Save transformed file
  out_file <- file.path(transformed_folder, paste0(base, ".csv"))
  readr::write_csv(df, out_file)
  message("Transformed file saved: ", out_file)
  
  return(out_file)
}

# Run batch
transformed_files <- map_chr(files, process_file)
message("\nAll files processed. Transformed files saved in: ", transformed_folder)

