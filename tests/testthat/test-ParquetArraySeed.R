# Tests the basic functions of a ParquetArraySeed.
# library(testthat); library(ParquetDataFrame); source("setup.R"); source("test-ParquetArraySeed.R")

test_that("basic methods work as expected for a ParquetArraySeed", {
    seed <- ParquetArraySeed(titanic_path, key = c("Class", "Sex", "Age", "Survived"), value = "fate")
    checkParquetArraySeed(seed, titanic_array)

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

    expect_error(seed[,])

    object <- seed[]
    checkParquetArraySeed(object, titanic_array)

    object <- seed[, 2:1, , ]
    expected <- titanic_array[, 2:1, , ]
    checkParquetArraySeed(object, expected)

    object <- seed[c(4, 2), , 1, ]
    expected <- titanic_array[c(4, 2), , 1, ]
    checkParquetArraySeed(object, expected)

    object <- seed[c(4, 2), , 1, , drop = FALSE]
    expected <- titanic_array[c(4, 2), , 1, , drop = FALSE]
    checkParquetArraySeed(object, expected)

    object <- seed[4, 2, 1, 2]
    expected <- as.array(titanic_array[4, 2, 1, 2])
    checkParquetArraySeed(object, expected)

    object <- seed[4, 2, 1, 2, drop = FALSE]
    expected <- titanic_array[4, 2, 1, 2, drop = FALSE]
    checkParquetArraySeed(object, expected)

    object <- seed[c("1st", "2nd", "3rd"), "Female", "Child", ]
    expected <- titanic_array[c("1st", "2nd", "3rd"), "Female", "Child", ]
    checkParquetArraySeed(object, expected)
})

test_that("aperm and t methods work as expected for a ParquetArraySeed", {
    seed <- ParquetArraySeed(titanic_path, key = c("Class", "Sex", "Age", "Survived"), value = "fate")

    object <- aperm(seed, c(4, 2, 1, 3))
    expected <- aperm(titanic_array, c(4, 2, 1, 3))
    checkParquetArraySeed(object, expected)

    names(dimnames(state.x77)) <- c("rowname", "colname")
    seed <- ParquetArraySeed(state_path, key = c("rowname", "colname"), value = "value")

    object <- t(seed)
    expected <- t(state.x77)
    expect_s4_class(object, "ParquetArraySeed")
    expect_identical(type(object), "double")
    expect_identical(length(object), length(expected))
    expect_identical(dim(object), dim(expected))
    expect_identical(dimnames(object)[[1L]], dimnames(expected)[[1L]])
    expect_setequal(dimnames(object)[[2L]], dimnames(expected)[[2L]])
    expect_identical(as.array(object)[, colnames(expected)], expected)
})
