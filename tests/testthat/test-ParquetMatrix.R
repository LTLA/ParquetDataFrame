# Tests the basic functions of a ParquetMatrix.
# library(testthat); library(ParquetDataFrame); source("setup.R"); source("test-ParquetMatrix.R")

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

test_that("extraction methods work as expected for a ParquetMatrix", {
    names(dimnames(state.x77)) <- c("rowname", "colname")

    pqmat <- ParquetMatrix(state_path, row = list("rowname" = row.names(state.x77)), col = list("colname" = colnames(state.x77)), value = "value")

    expected <- as.array(state.x77[1, ])
    names(dimnames(expected)) <- "colname"
    checkParquetArray(pqmat[1, ], expected)
    checkParquetMatrix(pqmat[1, , drop = FALSE], state.x77[1, , drop = FALSE])

    expected <- as.array(state.x77["New Jersey", ])
    names(dimnames(expected)) <- "colname"
    checkParquetArray(pqmat["New Jersey", ], expected)
    checkParquetMatrix(pqmat["New Jersey", , drop = FALSE], state.x77["New Jersey", , drop = FALSE])

    checkParquetMatrix(pqmat[c("New Jersey", "Washington"), ], state.x77[c("New Jersey", "Washington"), ])

    expected <- as.array(state.x77[, 4])
    names(dimnames(expected)) <- "rowname"
    checkParquetArray(pqmat[, 4], expected)
    checkParquetMatrix(pqmat[, 4, drop = FALSE], state.x77[, 4, drop = FALSE])

    expected <- as.array(state.x77[, "Murder"])
    names(dimnames(expected)) <- "rowname"
    checkParquetArray(pqmat[, "Murder"], expected)
    checkParquetMatrix(pqmat[, "Murder", drop = FALSE], state.x77[, "Murder", drop = FALSE])

    checkParquetMatrix(pqmat[, c("Income", "Life Exp", "Murder")], state.x77[, c("Income", "Life Exp", "Murder")])

    checkParquetMatrix(pqmat[c(13, 7), c(1, 3, 5, 7)], state.x77[c(13, 7), c(1, 3, 5, 7)])
    checkParquetMatrix(pqmat[c("New Jersey", "Washington"), c("Income", "Life Exp", "Murder")],
                       state.x77[c("New Jersey", "Washington"), c("Income", "Life Exp", "Murder")])
})

test_that("aperm and t methods work as expected for a ParquetMatrix", {
    names(dimnames(state.x77)) <- c("rowname", "colname")

    pqmat <- ParquetMatrix(state_path, row = list("rowname" = row.names(state.x77)), col = list("colname" = colnames(state.x77)), value = "value")

    checkParquetMatrix(aperm(pqmat, c(2, 1)), aperm(state.x77, c(2, 1)))

    checkParquetMatrix(t(pqmat), t(state.x77))
})
