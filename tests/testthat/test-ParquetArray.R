# Tests the basic functions of a ParquetArray.
# library(testthat); library(ParquetDataFrame); source("setup.R"); source("test-ParquetArray.R")

test_that("basic methods work as expected for a ParquetArray", {
    pqarray <- ParquetArray(titanic_path, dimensions = c("Class", "Sex", "Age", "Survived"), value = "fate")
    expect_s4_class(pqarray, "ParquetArray")
    expect_identical(type(pqarray), "integer")
    expect_identical(type(pqarray), typeof(titanic_df$fate))
    expect_identical(length(pqarray), length(titanic_array))
    expect_identical(dim(pqarray), dim(titanic_array))
    expect_identical(dimnames(pqarray), dimnames(titanic_array))
    expect_equal(as.array(pqarray), titanic_array)

    pqarray <- ParquetArray(titanic_path, dimensions = c("Class", "Sex", "Age", "Survived"), value = "fate", type = "double")
    expect_s4_class(pqarray, "ParquetArray")
    expect_identical(type(pqarray), "double")
    expect_identical(length(pqarray), length(titanic_array))
    expect_identical(dim(pqarray), dim(titanic_array))
    expect_identical(dimnames(pqarray), dimnames(titanic_array))
    expect_equal(as.array(pqarray), titanic_array)

    pqarray <- ParquetArray(titanic_path, dimensions = c("Class", "Sex", "Age", "Survived"), value = "fate", type = "character")
    expect_s4_class(pqarray, "ParquetArray")
    expect_identical(type(pqarray), "character")
    expect_identical(length(pqarray), length(titanic_array))
    expect_identical(dim(pqarray), dim(titanic_array))
    expect_identical(dimnames(pqarray), dimnames(titanic_array))
})
