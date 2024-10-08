# Tests the basic functions of a ParquetColumnSeed.
# library(testthat); library(ParquetDataFrame); source("setup.R"); source("test-ParquetColumnSeed.R")

stratum <- DelayedArray(ParquetColumnSeed(example_path, column="stratum"))
case <- DelayedArray(ParquetColumnSeed(example_path, column="case"))
education <- DelayedArray(ParquetColumnSeed(example_path, column="education"))

test_that("basic methods work as expected for a ParquetColumnSeed", {
    expect_s4_class(stratum, "ParquetColumnVector")
    expect_identical(length(stratum), nrow(example_df))
    expect_identical(type(stratum), "integer")
    expect_identical(type(stratum), typeof(example_df$stratum))
    expect_identical(as.vector(stratum), example_df$stratum)

    expect_s4_class(case, "ParquetColumnVector")
    expect_identical(length(case), nrow(example_df))
    expect_identical(type(case), "double")
    expect_identical(type(case), typeof(example_df$case))
    expect_identical(as.vector(case), example_df$case)

    expect_s4_class(education, "ParquetColumnVector")
    expect_identical(length(education), nrow(example_df))
    expect_identical(type(education), "character")
    expect_identical(type(education), typeof(example_df$education))
    expect_identical(as.vector(education), example_df$education)
})

test_that("extraction methods work as expected for a ParquetColumnSeed", {
    keep <- seq(1, length(case), by=2)
    expect_identical(as.vector(extract_array(stratum, list(keep))), example_df$stratum[keep])
    expect_identical(as.vector(extract_array(case, list(keep))), example_df$case[keep])
    expect_identical(as.vector(extract_array(education, list(keep))), example_df$education[keep])

    # Not sorted.
    keep <- sample(length(case))
    expect_identical(as.vector(extract_array(stratum, list(keep))), example_df$stratum[keep])
    expect_identical(as.vector(extract_array(case, list(keep))), example_df$case[keep])
    expect_identical(as.vector(extract_array(education, list(keep))), example_df$education[keep])

    # duplicates
    keep <- c(1,2,3,1,3,5,2,4,6)
    expect_identical(as.vector(extract_array(stratum, list(keep))), example_df$stratum[keep])
    expect_identical(as.vector(extract_array(case, list(keep))), example_df$case[keep])
    expect_identical(as.vector(extract_array(education, list(keep))), example_df$education[keep])
})
