# Tests the basic functions of a ParquetMatrix.
# library(testthat); library(ParquetDataFrame); source("setup.R"); source("test-ParquetMatrix.R")

checkParquetMatrix <- function(object, expected) {
    expect_s4_class(object, "ParquetMatrix")
    expect_identical(type(object), typeof(expected))
    expect_identical(length(object), length(expected))
    expect_identical(dim(object), dim(expected))
    expect_identical(dimnames(object), dimnames(expected))
    expect_equal(as.matrix(object), expected)
}

test_that("basic methods work as expected for a ParquetMatrix", {
    names(dimnames(state.x77)) <- c("rowname", "colname")

    pqmat <- ParquetMatrix(state_path, row = "rowname", col = "colname", value = "value")
    expect_s4_class(pqmat, "ParquetMatrix")
    expect_identical(type(pqmat), "double")
    expect_identical(type(pqmat), typeof(state.x77))
    expect_identical(length(pqmat), length(state.x77))
    expect_identical(dim(pqmat), dim(state.x77))
    expect_setequal(rownames(pqmat), rownames(state.x77))
    expect_identical(colnames(pqmat), colnames(state.x77))
    expect_equal(as.matrix(pqmat)[rownames(state.x77), ], state.x77)

    pqmat <- ParquetMatrix(state_path, row = list("rowname" = row.names(state.x77)), col = list("colname" = colnames(state.x77)), value = "value")
    checkParquetMatrix(pqmat, state.x77)

    pqmat <- ParquetMatrix(state_path, key = list("rowname" = row.names(state.x77), "colname" = colnames(state.x77)), value = "value")
    checkParquetMatrix(pqmat, state.x77)
})

test_that("aperm and t methods work as expected for a ParquetMatrix", {
    names(dimnames(state.x77)) <- c("rowname", "colname")

    pqmat <- ParquetMatrix(state_path, row = list("rowname" = row.names(state.x77)), col = list("colname" = colnames(state.x77)), value = "value")

    object <- aperm(pqmat, c(2, 1))
    expected <- aperm(state.x77, c(2, 1))
    checkParquetMatrix(object, expected)

    object <- t(pqmat)
    expected <- t(state.x77)
    checkParquetMatrix(object, expected)
})
