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

test_that("Arith methods work as expected for a ParquetMatrix", {
    pqmat <- ParquetMatrix(state_path, row = list("rowname" = row.names(state.x77)), col = list("colname" = colnames(state.x77)), value = "value")

    ## "+"
    checkParquetMatrix(pqmat + sqrt(pqmat), as.array(pqmat) + sqrt(as.array(pqmat)))
    checkParquetMatrix(pqmat + 1L, as.array(pqmat) + 1L)
    checkParquetMatrix(pqmat + 3.14, as.array(pqmat) + 3.14)
    checkParquetMatrix(1L + pqmat, 1L + as.array(pqmat))
    checkParquetMatrix(3.14 + pqmat, 3.14 + as.array(pqmat))

    ## "-"
    checkParquetMatrix(pqmat - sqrt(pqmat), as.array(pqmat) - sqrt(as.array(pqmat)))
    checkParquetMatrix(pqmat - 1L, as.array(pqmat) - 1L)
    checkParquetMatrix(pqmat - 3.14, as.array(pqmat) - 3.14)
    checkParquetMatrix(1L - pqmat, 1L - as.array(pqmat))
    checkParquetMatrix(3.14 - pqmat, 3.14 - as.array(pqmat))

    ## "*"
    checkParquetMatrix(pqmat * sqrt(pqmat), as.array(pqmat) * sqrt(as.array(pqmat)))
    checkParquetMatrix(pqmat * 1L, as.array(pqmat) * 1L)
    checkParquetMatrix(pqmat * 3.14, as.array(pqmat) * 3.14)
    checkParquetMatrix(1L * pqmat, 1L * as.array(pqmat))
    checkParquetMatrix(3.14 * pqmat, 3.14 * as.array(pqmat))

    ## "/"
    checkParquetMatrix(pqmat / sqrt(pqmat), as.array(pqmat) / sqrt(as.array(pqmat)))
    checkParquetMatrix(pqmat / 1L, as.array(pqmat) / 1L)
    checkParquetMatrix(pqmat / 3.14, as.array(pqmat) / 3.14)
    checkParquetMatrix(1L / pqmat, 1L / as.array(pqmat))
    checkParquetMatrix(3.14 / pqmat, 3.14 / as.array(pqmat))

    ## "^"
    checkParquetMatrix(pqmat ^ sqrt(pqmat), as.array(pqmat) ^ sqrt(as.array(pqmat)))
    checkParquetMatrix(pqmat ^ 3.14, as.array(pqmat) ^ 3.14)
    checkParquetMatrix(3.14 ^ pqmat, 3.14 ^ as.array(pqmat))

    ## "%%"
    checkParquetMatrix(pqmat %% sqrt(pqmat), as.array(pqmat) %% sqrt(as.array(pqmat)))
    checkParquetMatrix(pqmat %% 1L, as.array(pqmat) %% 1L)
    checkParquetMatrix(pqmat %% 3.14, as.array(pqmat) %% 3.14)
    checkParquetMatrix(1L %% pqmat, 1L %% as.array(pqmat))
    checkParquetMatrix(3.14 %% pqmat, 3.14 %% as.array(pqmat))

    ## "%/%"
    checkParquetMatrix(pqmat %/% sqrt(pqmat), as.array(pqmat) %/% sqrt(as.array(pqmat)))
    checkParquetMatrix(pqmat %/% 1L, as.array(pqmat) %/% 1L)
    checkParquetMatrix(pqmat %/% 3.14, as.array(pqmat) %/% 3.14)
    checkParquetMatrix(1L %/% pqmat, 1L %/% as.array(pqmat))
    checkParquetMatrix(3.14 %/% pqmat, 3.14 %/% as.array(pqmat))
})

test_that("Compare methods work as expected for a ParquetMatrix", {
    pqmat <- ParquetMatrix(state_path, row = list("rowname" = row.names(state.x77)), col = list("colname" = colnames(state.x77)), value = "value")

    ## "=="
    checkParquetMatrix(pqmat == sqrt(pqmat), as.array(pqmat) == sqrt(as.array(pqmat)))
    checkParquetMatrix(pqmat == 1L, as.array(pqmat) == 1L)
    checkParquetMatrix(pqmat == 3.14, as.array(pqmat) == 3.14)
    checkParquetMatrix(1L == pqmat, 1L == as.array(pqmat))
    checkParquetMatrix(3.14 == pqmat, 3.14 == as.array(pqmat))

    ## ">"
    checkParquetMatrix(pqmat > sqrt(pqmat), as.array(pqmat) > sqrt(as.array(pqmat)))
    checkParquetMatrix(pqmat > 1L, as.array(pqmat) > 1L)
    checkParquetMatrix(pqmat > 3.14, as.array(pqmat) > 3.14)
    checkParquetMatrix(1L > pqmat, 1L > as.array(pqmat))
    checkParquetMatrix(3.14 > pqmat, 3.14 > as.array(pqmat))

    ## "<"
    checkParquetMatrix(pqmat < sqrt(pqmat), as.array(pqmat) < sqrt(as.array(pqmat)))
    checkParquetMatrix(pqmat < 1L, as.array(pqmat) < 1L)
    checkParquetMatrix(pqmat < 3.14, as.array(pqmat) < 3.14)
    checkParquetMatrix(1L < pqmat, 1L < as.array(pqmat))
    checkParquetMatrix(3.14 < pqmat, 3.14 < as.array(pqmat))

    ## "!="
    checkParquetMatrix(pqmat != sqrt(pqmat), as.array(pqmat) != sqrt(as.array(pqmat)))
    checkParquetMatrix(pqmat != 1L, as.array(pqmat) != 1L)
    checkParquetMatrix(pqmat != 3.14, as.array(pqmat) != 3.14)
    checkParquetMatrix(1L != pqmat, 1L != as.array(pqmat))
    checkParquetMatrix(3.14 != pqmat, 3.14 != as.array(pqmat))

    ## "<="
    checkParquetMatrix(pqmat <= sqrt(pqmat), as.array(pqmat) <= sqrt(as.array(pqmat)))
    checkParquetMatrix(pqmat <= 1L, as.array(pqmat) <= 1L)
    checkParquetMatrix(pqmat <= 3.14, as.array(pqmat) <= 3.14)
    checkParquetMatrix(1L <= pqmat, 1L <= as.array(pqmat))
    checkParquetMatrix(3.14 <= pqmat, 3.14 <= as.array(pqmat))

    ## ">="
    checkParquetMatrix(pqmat >= sqrt(pqmat), as.array(pqmat) >= sqrt(as.array(pqmat)))
    checkParquetMatrix(pqmat >= 1L, as.array(pqmat) >= 1L)
    checkParquetMatrix(pqmat >= 3.14, as.array(pqmat) >= 3.14)
    checkParquetMatrix(1L >= pqmat, 1L >= as.array(pqmat))
    checkParquetMatrix(3.14 >= pqmat, 3.14 >= as.array(pqmat))
})

test_that("Logic methods work as expected for a ParquetMatrix", {
    pqmat <- ParquetMatrix(state_path, row = list("rowname" = row.names(state.x77)), col = list("colname" = colnames(state.x77)), value = "value")

    ## "&"
    x <- pqmat > 70
    y <- pqmat < 4000
    checkParquetMatrix(x & y, as.array(x) & as.array(y))

    ## "|"
    x <- pqmat > 70
    y <- sqrt(pqmat) > 0
    checkParquetMatrix(x | y, as.array(x) | as.array(y))
})

test_that("Math methods work as expected for a ParquetMatrix", {
    pqmat <- ParquetMatrix(state_path, row = list("rowname" = row.names(state.x77)), col = list("colname" = colnames(state.x77)), value = "value")

    income <- pqmat[, "Income", drop = FALSE]
    ikeep <-
      c("Colorado", "Delaware", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas",
        "Maine", "Maryland", "Michigan", "Minnesota", "Missouri", "Montana",
        "Nebraska", "Nevada", "New Hampshire", "North Dakota", "Ohio", "Oregon",
        "Pennsylvania", "South Dakota", "Utah", "Vermont", "Washington", "Wisconsin",
        "Wyoming")
    illiteracy <- pqmat[ikeep, "Illiteracy", drop = FALSE]

    checkParquetMatrix(abs(income), abs(as.array(income)))
    checkParquetMatrix(sign(income), sign(as.array(income)))
    checkParquetMatrix(sqrt(income), sqrt(as.array(income)))
    checkParquetMatrix(ceiling(income), ceiling(as.array(income)))
    checkParquetMatrix(floor(income), floor(as.array(income)))
    checkParquetMatrix(trunc(income), trunc(as.array(income)))

    expect_error(cummax(income))
    expect_error(cummin(income))
    expect_error(cumprod(income))
    expect_error(cumsum(income))

    checkParquetMatrix(log(income), log(as.array(income)))
    checkParquetMatrix(log10(income), log10(as.array(income)))
    checkParquetMatrix(log2(income), log2(as.array(income)))
    checkParquetMatrix(log1p(income), log1p(as.array(income)))

    checkParquetMatrix(acos(illiteracy), acos(as.array(illiteracy)))

    expect_error(acosh(illiteracy))

    checkParquetMatrix(asin(illiteracy), asin(as.array(illiteracy)))

    expect_error(asinh(illiteracy))
    expect_error(atan(illiteracy))
    expect_error(atanh(illiteracy))

    checkParquetMatrix(exp(income), exp(as.array(income)))

    expect_error(expm1(income))

    checkParquetMatrix(cos(income), cos(as.array(income)))

    expect_error(cosh(income))
    expect_error(cospi(income))

    checkParquetMatrix(sin(income), sin(as.array(income)))

    expect_error(sinh(income))
    expect_error(sinpi(income))

    checkParquetMatrix(tan(income), tan(as.array(income)))

    expect_error(tanh(income))
    expect_error(tanpi(income))

    expect_error(gamma(income))
    expect_error(lgamma(income))
    expect_error(digamma(income))
    expect_error(trigamma(income))
})
