# Titanic dataset
titanic_array <- unclass(Titanic)
storage.mode(titanic_array) <- "integer"

titanic_df <- do.call(expand.grid, c(dimnames(Titanic), stringsAsFactors = FALSE))
titanic_df$fate <- as.integer(Titanic[as.matrix(titanic_df)])

titanic_path <- tempfile()
arrow::write_parquet(titanic_df, titanic_path)

# State dataset
state_df <- data.frame(
  region = rep(state.region, times = ncol(state.x77)),
  division = rep(state.division, times = ncol(state.x77)),
  rowname = rep(rownames(state.x77), times = ncol(state.x77)),
  colname = rep(colnames(state.x77), each = nrow(state.x77)),
  value = as.vector(state.x77)
)

state_path <- tempfile()
arrow::write_dataset(state_df, state_path, format = "parquet", partitioning = c("region", "division"))

# Infert dataset
infert_path <- tempfile()
infert_df <- infert
infert_df$education <- as.character(infert_df$education)
arrow::write_parquet(infert_df, infert_path)
