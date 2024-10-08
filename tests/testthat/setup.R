example_path <- tempfile()
example_df <- infert
example_df$education <- as.character(example_df$education)
arrow::write_parquet(example_df, example_path)
