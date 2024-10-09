# Tests the basic functions of a ParquetColumnSeed.
# library(testthat); library(ParquetDataFrame); source("setup.R"); source("test-ParquetColumnSeed.R")

stratum <- DelayedArray(ParquetColumnSeed(example_path, column="stratum"))
age <- DelayedArray(ParquetColumnSeed(example_path, column="age"))
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

test_that("Math methods work as expected for a ParquetColumnVector", {
    x <- abs(age)
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), abs(as.vector(age)))

    x <- sign(age)
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), sign(as.vector(age)))

    x <- sqrt(age)
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), sqrt(as.vector(age)))

    x <- ceiling(age)
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), ceiling(as.vector(age)))

    x <- floor(age)
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), floor(as.vector(age)))

    x <- trunc(age)
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), trunc(as.vector(age)))

    expect_error(cummax(age))
    expect_error(cummin(age))
    expect_error(cumprod(age))
    expect_error(cumsum(age))

    x <- log(age)
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), log(as.vector(age)))

    x <- log10(age)
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), log10(as.vector(age)))

    x <- log2(age)
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), log2(as.vector(age)))

    x <- log1p(age)
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), log1p(as.vector(age)))

    x <- acos(case)
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), acos(as.vector(case)))

    expect_error(acosh(age))

    x <- asin(case)
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), asin(as.vector(case)))

    expect_error(asinh(age))
    expect_error(atan(age))
    expect_error(atanh(age))

    x <- exp(age)
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), exp(as.vector(age)))

    expect_error(expm1(age))

    x <- cos(age)
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), cos(as.vector(age)))

    expect_error(cosh(age))
    expect_error(cospi(age))

    x <- sin(age)
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), sin(as.vector(age)))

    expect_error(sinh(age))
    expect_error(sinpi(age))

    x <- tan(age)
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), tan(as.vector(age)))

    expect_error(tanh(age))
    expect_error(tanpi(age))

    expect_error(gamma(age))
    expect_error(lgamma(age))
    expect_error(digamma(age))
    expect_error(trigamma(age))
})
