# Tests the basic functions of a ParquetColumnSeed.
# library(testthat); library(ParquetDataFrame); source("setup.R"); source("test-ParquetColumnSeed.R")

x <- ParquetColumnSeed(example_path, column="wt")
y <- DelayedArray(x)

test_that("basic methods work as expected for a ParquetColumnSeed", {
    expect_s4_class(y, "ParquetColumnVector")
    expect_identical(length(y), nrow(example_df))
    expect_identical(type(y), typeof(example_df$wt))
    expect_identical(as.vector(y), example_df$wt)
})

test_that("extraction methods work as expected for a ParquetColumnSeed", {
    keep <- seq(1, length(y), by=2)
    expect_identical(as.vector(extract_array(y, list(keep))), example_df$wt[keep])

    # Not sorted.
    keep <- sample(length(y))
    expect_identical(as.vector(extract_array(y, list(keep))), example_df$wt[keep])

    # duplicates
    keep <- c(1,2,3,1,3,5,2,4,6)
    expect_identical(as.vector(extract_array(y, list(keep))), example_df$wt[keep])
})
