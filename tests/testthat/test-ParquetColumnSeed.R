# Tests the basic functions of a ParquetColumnSeed.
# library(testthat); library(ParquetDataFrame); source("setup.R"); source("test-ParquetColumnSeed.R")

stratum <- DelayedArray(ParquetColumnSeed(infert_path, column="stratum"))
age <- DelayedArray(ParquetColumnSeed(infert_path, column="age"))
induced <- DelayedArray(ParquetColumnSeed(infert_path, column="induced"))
case <- DelayedArray(ParquetColumnSeed(infert_path, column="case"))
spontaneous <- DelayedArray(ParquetColumnSeed(infert_path, column="spontaneous"))
education <- DelayedArray(ParquetColumnSeed(infert_path, column="education"))

test_that("basic methods work as expected for a ParquetColumnSeed", {
    expect_s4_class(stratum, "ParquetColumnVector")
    expect_identical(length(stratum), nrow(infert_df))
    expect_identical(type(stratum), "integer")
    expect_identical(type(stratum), typeof(infert_df$stratum))
    expect_identical(as.vector(stratum), infert_df$stratum)

    expect_s4_class(case, "ParquetColumnVector")
    expect_identical(length(case), nrow(infert_df))
    expect_identical(type(case), "double")
    expect_identical(type(case), typeof(infert_df$case))
    expect_identical(as.vector(case), infert_df$case)

    expect_s4_class(education, "ParquetColumnVector")
    expect_identical(length(education), nrow(infert_df))
    expect_identical(type(education), "character")
    expect_identical(type(education), typeof(infert_df$education))
    expect_identical(as.vector(education), infert_df$education)
})

test_that("extraction methods work as expected for a ParquetColumnSeed", {
    keep <- seq(1, length(case), by=2)
    expect_identical(as.vector(extract_array(stratum, list(keep))), infert_df$stratum[keep])
    expect_identical(as.vector(extract_array(case, list(keep))), infert_df$case[keep])
    expect_identical(as.vector(extract_array(education, list(keep))), infert_df$education[keep])

    # Not sorted.
    keep <- sample(length(case))
    expect_identical(as.vector(extract_array(stratum, list(keep))), infert_df$stratum[keep])
    expect_identical(as.vector(extract_array(case, list(keep))), infert_df$case[keep])
    expect_identical(as.vector(extract_array(education, list(keep))), infert_df$education[keep])

    # duplicates
    keep <- c(1,2,3,1,3,5,2,4,6)
    expect_identical(as.vector(extract_array(stratum, list(keep))), infert_df$stratum[keep])
    expect_identical(as.vector(extract_array(case, list(keep))), infert_df$case[keep])
    expect_identical(as.vector(extract_array(education, list(keep))), infert_df$education[keep])
})

test_that("Arith methods work as expected for a ParquetColumnVector", {
    ## "+"
    x <- age + stratum
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(age) + as.vector(stratum))

    x <- age + 1L
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(age) + 1L)

    x <- age + 3.14
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(age) + 3.14)

    x <- 1L + age
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), 1L + as.vector(age))

    x <- 3.14 + age
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), 3.14 + as.vector(age))

    ## "-"
    x <- age - stratum
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(age) - as.vector(stratum))

    x <- age - 1L
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(age) - 1L)

    x <- age - 3.14
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(age) - 3.14)

    x <- 1L - age
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), 1L - as.vector(age))

    x <- 3.14 - age
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), 3.14 - as.vector(age))

    ## "*"
    x <- age * stratum
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(age) * as.vector(stratum))

    x <- age * 1L
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(age) * 1L)

    x <- age * 3.14
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(age) * 3.14)

    x <- 1L * age
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), 1L * as.vector(age))

    x <- 3.14 * age
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), 3.14 * as.vector(age))

    ## "/"
    x <- age / stratum
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(age) / as.vector(stratum))

    x <- age / 1L
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(age) / 1L)

    x <- age / 3.14
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(age) / 3.14)

    x <- 1L / age
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), 1L / as.vector(age))

    x <- 3.14 / age
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), 3.14 / as.vector(age))

    ## "^"
    x <- age ^ stratum
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(age) ^ as.vector(stratum))

    x <- age ^ 1L
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(age) ^ 1L)

    x <- age ^ 3.14
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(age) ^ 3.14)

    x <- 1L ^ age
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), 1L ^ as.vector(age))

    x <- 3.14 ^ age
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), 3.14 ^ as.vector(age))

    ## "%%"
    x <- age %% stratum
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(age) %% as.vector(stratum))

    x <- age %% 1L
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(age) %% 1L)

    x <- age %% 3.14
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(age) %% 3.14)

    x <- 1L %% age
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), 1L %% as.vector(age))

    x <- 3.14 %% age
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), 3.14 %% as.vector(age))

    ## "%/%"
    x <- age %/% stratum
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(age) %/% as.vector(stratum))

    x <- age %/% 1L
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(age) %/% 1L)

    x <- age %/% 3.14
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(age) %/% 3.14)

    x <- 1L %/% age
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), 1L %/% as.vector(age))

    x <- 3.14 %/% age
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), 3.14 %/% as.vector(age))
})

test_that("Compare methods work as expected for a ParquetColumnVector", {
    ## "=="
    x <- induced == spontaneous
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(induced) == as.vector(spontaneous))

    x <- induced == 1L
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(induced) == 1L)

    x <- induced == 3.14
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(induced) == 3.14)

    x <- 1L == spontaneous
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), 1L == as.vector(spontaneous))

    x <- 3.14 == spontaneous
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), 3.14 == as.vector(spontaneous))

    ## ">"
    x <- induced > spontaneous
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(induced) > as.vector(spontaneous))

    x <- induced > 1L
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(induced) > 1L)

    x <- induced > 3.14
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(induced) > 3.14)

    x <- 1L > spontaneous
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), 1L > as.vector(spontaneous))

    x <- 3.14 > spontaneous
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), 3.14 > as.vector(spontaneous))

    ## "<"
    x <- induced < spontaneous
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(induced) < as.vector(spontaneous))

    x <- induced < 1L
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(induced) < 1L)

    x <- induced < 3.14
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(induced) < 3.14)

    x <- 1L < spontaneous
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), 1L < as.vector(spontaneous))

    x <- 3.14 < spontaneous
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), 3.14 < as.vector(spontaneous))

    ## "!="
    x <- induced != spontaneous
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(induced) != as.vector(spontaneous))

    x <- induced != 1L
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(induced) != 1L)

    x <- induced != 3.14
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(spontaneous) != 3.14)

    x <- 1L != spontaneous
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), 1L != as.vector(spontaneous))

    x <- 3.14 != spontaneous
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), 3.14 != as.vector(spontaneous))

    ## "<="
    x <- induced <= spontaneous
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(induced) <= as.vector(spontaneous))

    x <- induced <= 1L
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(induced) <= 1L)

    x <- induced <= 3.14
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(induced) <= 3.14)

    x <- 1L <= spontaneous
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), 1L <= as.vector(spontaneous))

    x <- 3.14 <= spontaneous
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), 3.14 <= as.vector(spontaneous))

    ## ">="
    x <- induced >= spontaneous
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(induced) >= as.vector(spontaneous))

    x <- induced >= 1L
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(induced) >= 1L)

    x <- induced >= 3.14
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), as.vector(induced) >= 3.14)

    x <- 1L >= spontaneous
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), 1L >= as.vector(spontaneous))

    x <- 3.14 >= spontaneous
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(x), 3.14 >= as.vector(spontaneous))
})

test_that("Logic methods work as expected for a ParquetColumnVector", {
    ## "&"
    x <- age > 30
    y <- case > 0
    z <- x & y
    expect_s4_class(z, "ParquetColumnVector")
    expect_equal(as.vector(z), as.vector(age > 30) & as.vector(case > 0))

    ## "|"
    x <- age > 30
    y <- case > 0
    z <- x | y
    expect_s4_class(x, "ParquetColumnVector")
    expect_equal(as.vector(z), as.vector(age > 30) | as.vector(case > 0))
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
