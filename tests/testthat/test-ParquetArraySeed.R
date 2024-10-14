# Tests the basic functions of a ParquetArraySeed.
# library(testthat); library(ParquetDataFrame); source("setup.R"); source("test-ParquetArraySeed.R")

test_that("basic methods work as expected for a ParquetArraySeed", {
    seed <- ParquetArraySeed(titanic_path, key = c("Class", "Sex", "Age", "Survived"), value = "fate")
    expect_s4_class(seed, "ParquetArraySeed")
    expect_identical(type(seed), "integer")
    expect_identical(type(seed), typeof(titanic_df$fate))
    expect_identical(length(seed), length(titanic_array))
    expect_identical(dim(seed), dim(titanic_array))
    expect_identical(dimnames(seed), dimnames(titanic_array))
    expect_equal(as.array(seed), titanic_array)

    seed <- ParquetArraySeed(titanic_path, key = c("Class", "Sex", "Age", "Survived"), value = "fate", type = "double")
    expect_s4_class(seed, "ParquetArraySeed")
    expect_identical(type(seed), "double")
    expect_identical(length(seed), length(titanic_array))
    expect_identical(dim(seed), dim(titanic_array))
    expect_identical(dimnames(seed), dimnames(titanic_array))
    expect_equal(as.array(seed), titanic_array)

    seed <- ParquetArraySeed(titanic_path, key = c("Class", "Sex", "Age", "Survived"), value = "fate", type = "character")
    expect_s4_class(seed, "ParquetArraySeed")
    expect_identical(type(seed), "character")
    expect_identical(length(seed), length(titanic_array))
    expect_identical(dim(seed), dim(titanic_array))
    expect_identical(dimnames(seed), dimnames(titanic_array))
})

test_that("extraction methods work as expected for a ParquetArraySeed", {
    seed <- ParquetArraySeed(titanic_path, key = c("Class", "Sex", "Age", "Survived"), value = "fate")
    expect_identical(extract_array(seed, list(c(2,4), 2:1, 1, 2)), titanic_array[c(2,4), 2:1, 1, 2, drop = FALSE])
})
