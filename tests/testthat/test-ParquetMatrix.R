# Tests the basic functions of a ParquetMatrix.
# library(testthat); library(ParquetDataFrame); source("setup.R"); source("test-ParquetMatrix.R")


test_that("basic methods work as expected for a ParquetMatrix", {
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
    expect_s4_class(pqmat, "ParquetMatrix")
    expect_identical(type(pqmat), "double")
    expect_identical(type(pqmat), typeof(state.x77))
    expect_identical(length(pqmat), length(state.x77))
    expect_identical(dim(pqmat), dim(state.x77))
    expect_identical(dimnames(pqmat), dimnames(state.x77))
    expect_equal(as.matrix(pqmat), state.x77)

    pqmat <- ParquetMatrix(state_path, key = list("rowname" = row.names(state.x77), "colname" = colnames(state.x77)), value = "value")
    expect_s4_class(pqmat, "ParquetMatrix")
    expect_identical(type(pqmat), "double")
    expect_identical(type(pqmat), typeof(state.x77))
    expect_identical(length(pqmat), length(state.x77))
    expect_identical(dim(pqmat), dim(state.x77))
    expect_identical(dimnames(pqmat), dimnames(state.x77))
    expect_equal(as.matrix(pqmat), state.x77)
})
