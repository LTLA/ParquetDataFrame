# Smoking, Alcohol and (O)esophageal Cancer
esoph_path <- tempfile()
arrow::write_parquet(esoph, esoph_path)

# Motor Trend Car Road Tests
mtcars_df <- cbind(model = rownames(mtcars), mtcars)
rownames(mtcars_df) <- NULL
mtcars_path <- tempfile()
arrow::write_parquet(mtcars_df, mtcars_path)

# State dataset
state_df <- data.frame(
  region = rep(as.character(state.region), times = ncol(state.x77)),
  division = rep(as.character(state.division), times = ncol(state.x77)),
  rowname = rep(rownames(state.x77), times = ncol(state.x77)),
  colname = rep(colnames(state.x77), each = nrow(state.x77)),
  value = as.vector(state.x77)
)
state_path <- tempfile()
arrow::write_dataset(state_df, state_path, format = "parquet", partitioning = c("region", "division"))

# Titanic dataset
titanic_array <- unclass(Titanic)
storage.mode(titanic_array) <- "integer"
titanic_df <- do.call(expand.grid, c(dimnames(Titanic), stringsAsFactors = FALSE))
titanic_df$fate <- as.integer(Titanic[as.matrix(titanic_df)])
titanic_path <- tempfile()
arrow::write_parquet(titanic_df, titanic_path)

# Helper functions
checkParquetFactTable <- function(object, expected) {
    expect_s4_class(object, "ParquetFactTable")
    expect_gte(nrow(object), nrow(expected))
    expect_equal(nkey(object) + ncol(object), ncol(expected))
    expect_identical(c(keynames(object), colnames(object)), colnames(expected))
    df <- as.data.frame(object)
    df <- df[match(do.call(paste, expected[, keynames(object), drop = FALSE]),
                   do.call(paste, df[, keynames(object), drop = FALSE])), ]
    rownames(df) <- NULL
    expect_equivalent(df, expected)
}

checkParquetArraySeed <- function(object, expected) {
    expect_s4_class(object, "ParquetArraySeed")
    expect_identical(type(object), type(expected))
    expect_identical(length(object), length(expected))
    expect_identical(dim(object), dim(expected))
    expect_identical(dimnames(object), dimnames(expected))
    expect_equal(as.array(object), expected)
}

checkParquetArray <- function(object, expected) {
    expect_s4_class(object, "ParquetArray")
    expect_identical(type(object), type(expected))
    expect_identical(length(object), length(expected))
    expect_identical(dim(object), dim(expected))
    expect_identical(dimnames(object), dimnames(expected))
    expect_equal(as.array(object), expected)
}

checkParquetMatrix <- function(object, expected) {
    expect_s4_class(object, "ParquetMatrix")
    expect_identical(type(object), typeof(expected))
    expect_identical(length(object), length(expected))
    expect_identical(dim(object), dim(expected))
    expect_identical(dimnames(object), dimnames(expected))
    expect_equal(as.matrix(object), expected)
}

checkParquetDataFrame <- function(object, expected) {
    expect_s4_class(object, "ParquetDataFrame")
    expect_identical(ncol(object), ncol(expected))
    expect_identical(nrow(object), nrow(expected))
    expect_setequal(rownames(object), rownames(expected))
    expect_identical(colnames(object), colnames(expected))
    expect_identical(as.data.frame(object)[rownames(expected), , drop=FALSE], expected)
}

checkParquetColumn <- function(object, expected) {
    expect_s4_class(object, "ParquetColumn")
    expect_identical(length(object), length(expected))
    expect_identical(names(object), names(expected))
    expect_equal(as.vector(object), expected)
}
