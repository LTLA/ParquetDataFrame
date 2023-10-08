example_path <- tempfile()
example_df <- mtcars
arrow::write_parquet(example_df, example_path)
