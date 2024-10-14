# Tests the basic functions of a ParquetArray.
# library(testthat); library(ParquetDataFrame); source("setup.R"); source("test-ParquetArray.R")

test_that("basic methods work as expected for a ParquetArray", {
    pqarray <- ParquetArray(titanic_path, key = c("Class", "Sex", "Age", "Survived"), value = "fate")
    expect_s4_class(pqarray, "ParquetArray")
    expect_identical(type(pqarray), "integer")
    expect_identical(type(pqarray), typeof(titanic_df$fate))
    expect_identical(length(pqarray), length(titanic_array))
    expect_identical(dim(pqarray), dim(titanic_array))
    expect_identical(dimnames(pqarray), dimnames(titanic_array))
    expect_equal(as.array(pqarray), titanic_array)

    pqarray <- ParquetArray(titanic_path, key = c("Class", "Sex", "Age", "Survived"), value = "fate", type = "double")
    expect_s4_class(pqarray, "ParquetArray")
    expect_identical(type(pqarray), "double")
    expect_identical(length(pqarray), length(titanic_array))
    expect_identical(dim(pqarray), dim(titanic_array))
    expect_identical(dimnames(pqarray), dimnames(titanic_array))
    expect_equal(as.array(pqarray), titanic_array)

    pqarray <- ParquetArray(titanic_path, key = c("Class", "Sex", "Age", "Survived"), value = "fate", type = "character")
    expect_s4_class(pqarray, "ParquetArray")
    expect_identical(type(pqarray), "character")
    expect_identical(length(pqarray), length(titanic_array))
    expect_identical(dim(pqarray), dim(titanic_array))
    expect_identical(dimnames(pqarray), dimnames(titanic_array))
})

test_that("extraction methods work as expected for a ParquetArray", {
    pqarray <- ParquetArray(titanic_path, key = c("Class", "Sex", "Age", "Survived"), value = "fate")

    expect_error(pqarray[,])

    object <- pqarray[]
    expect_s4_class(object, "ParquetArray")
    expect_identical(type(object), "integer")
    expect_identical(length(object), length(titanic_array))
    expect_identical(dim(object), dim(titanic_array))
    expect_identical(dimnames(object), dimnames(titanic_array))
    expect_identical(as.array(object), titanic_array)

    object <- pqarray[, 2:1, , ]
    expected <- titanic_array[, 2:1, , ]
    expect_s4_class(object, "ParquetArray")
    expect_identical(type(object), "integer")
    expect_identical(length(object), length(expected))
    expect_identical(dim(object), dim(expected))
    expect_identical(dimnames(object), dimnames(expected))
    expect_identical(as.array(object), expected)

    object <- pqarray[c(4, 2), , 1, ]
    expected <- titanic_array[c(4, 2), , 1, ]
    expect_s4_class(object, "ParquetArray")
    expect_identical(type(object), "integer")
    expect_identical(length(object), length(expected))
    expect_identical(dim(object), dim(expected))
    expect_identical(dimnames(object), dimnames(expected))
    expect_identical(as.array(object), expected)

    object <- pqarray[c(4, 2), , 1, , drop = FALSE]
    expected <- titanic_array[c(4, 2), , 1, , drop = FALSE]
    expect_s4_class(object, "ParquetArray")
    expect_identical(type(object), "integer")
    expect_identical(length(object), length(expected))
    expect_identical(dim(object), dim(expected))
    expect_identical(dimnames(object), dimnames(expected))
    expect_identical(as.array(object), expected)

    object <- pqarray[c("1st", "2nd", "3rd"), "Female", "Child", ]
    expected <- titanic_array[c("1st", "2nd", "3rd"), "Female", "Child", ]
    expect_s4_class(object, "ParquetArray")
    expect_identical(type(object), "integer")
    expect_identical(length(object), length(expected))
    expect_identical(dim(object), dim(expected))
    expect_identical(dimnames(object), dimnames(expected))
    expect_identical(as.array(object), expected)
})

test_that("aperm and t methods work as expected for a ParquetArray", {
    seed <- ParquetArray(titanic_path, key = c("Class", "Sex", "Age", "Survived"), value = "fate")

    object <- aperm(seed, c(4, 2, 1, 3))
    expected <- aperm(titanic_array, c(4, 2, 1, 3))
    expect_s4_class(object, "ParquetArray")
    expect_identical(type(object), "integer")
    expect_identical(length(object), length(expected))
    expect_identical(dim(object), dim(expected))
    expect_identical(dimnames(object), dimnames(expected))
    expect_identical(as.array(object), expected)

    names(dimnames(state.x77)) <- c("rowname", "colname")
    seed <- ParquetArray(state_path, key = c("rowname", "colname"), value = "value")

    object <- t(seed)
    expected <- t(state.x77)
    expect_s4_class(object, "ParquetArray")
    expect_identical(type(object), "double")
    expect_identical(length(object), length(expected))
    expect_identical(dim(object), dim(expected))
    expect_identical(dimnames(object)[[1L]], dimnames(expected)[[1L]])
    expect_setequal(dimnames(object)[[2L]], dimnames(expected)[[2L]])
    expect_identical(as.array(object)[, colnames(expected)], expected)
})
