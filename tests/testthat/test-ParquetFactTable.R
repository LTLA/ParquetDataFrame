# Tests the basic functions of a ParquetFactTable.
# library(testthat); library(ParquetFactTable); source("setup.R"); source("test-ParquetFactTable.R")

test_that("basic methods work as expected for a ParquetFactTable", {
    tbl <- ParquetFactTable(esoph_path, key = c("agegp", "alcgp", "tobgp"), fact = c("ncases", "ncontrols"))
    checkParquetFactTable(tbl, esoph)

    tbl <- ParquetFactTable(mtcars_path, key = "model", fact = colnames(mtcars))
    checkParquetFactTable(tbl, mtcars_df)

    tbl <- ParquetFactTable(state_path, key = c("region", "division", "rowname", "colname"), fact = "value")
    checkParquetFactTable(tbl, state_df)

    tbl <- ParquetFactTable(titanic_path, key = c("Class", "Sex", "Age", "Survived"), fact = "fate")
    checkParquetFactTable(tbl, titanic_df)
})

test_that("fact columns of a ParquetFactTable can be cast to a different type", {
    tbl <- ParquetFactTable(esoph_path, key = c("agegp", "alcgp", "tobgp"), fact = c("ncases", "ncontrols"))
    expect_is(as.data.frame(tbl)[["ncases"]], "numeric")
    expect_is(as.data.frame(tbl)[["ncontrols"]], "numeric")

    tbl <- ParquetFactTable(esoph_path, key = c("agegp", "alcgp", "tobgp"),
                            fact = c("ncases" = "integer", "ncontrols" = "integer"))
    checkParquetFactTable(tbl, esoph)
    expect_is(as.data.frame(tbl)[["ncases"]], "integer")
    expect_is(as.data.frame(tbl)[["ncontrols"]], "integer")
})

test_that("ParquetFactTable column names can be modified", {
    tbl <- ParquetFactTable(mtcars_path, key = "model", fact = colnames(mtcars))
    replacements <- sprintf("COL%d", seq_len(ncol(tbl)))
    colnames(tbl) <- replacements
    expected <- mtcars_df 
    colnames(expected)[-1L] <- replacements
    checkParquetFactTable(tbl, expected)
})

test_that("ParquetFactTable can be bound across columns", {
    tbl <- ParquetFactTable(mtcars_path, key = "model", fact = colnames(mtcars))

    # Same path, we get another PDF.
    checkParquetFactTable(cbind(tbl, foo=tbl[,"carb"]), cbind(mtcars_df, foo=mtcars[["carb"]]))

    # Duplicate names causes unique renaming.
    expected <- cbind(mtcars_df, mtcars)
    colnames(expected) <- make.unique(colnames(expected), sep="_")
    checkParquetFactTable(cbind(tbl, tbl), expected)

    # Duplicate names causes unique renaming.
    expected <- cbind(mtcars_df, carb=mtcars[,"carb"])
    colnames(expected) <- make.unique(colnames(expected), sep="_")
    checkParquetFactTable(cbind(tbl, carb=tbl[,"carb"]), expected)
})

test_that("Arith methods work as expected for a ParquetFactTable", {
    tbl <- ParquetFactTable(mtcars_path, key = "model", fact = colnames(mtcars))

    ## "+"
    checkParquetFactTable(tbl + sqrt(tbl), cbind(model = rownames(mtcars), mtcars + sqrt(mtcars)))
    checkParquetFactTable(tbl + tbl[,"carb"], cbind(model = rownames(mtcars), mtcars + mtcars[, "carb"]))
    checkParquetFactTable(tbl + 1L, cbind(model = rownames(mtcars), mtcars + 1L))
    checkParquetFactTable(tbl + 3.14, cbind(model = rownames(mtcars), mtcars + 3.14))
    checkParquetFactTable(sqrt(tbl) + tbl, cbind(model = rownames(mtcars), sqrt(mtcars) + mtcars))
    checkParquetFactTable(tbl[,"carb"] + tbl, cbind(model = rownames(mtcars), mtcars[, "carb"] + mtcars))
    checkParquetFactTable(1L + tbl, cbind(model = rownames(mtcars), 1L + mtcars))
    checkParquetFactTable(3.14 + tbl, cbind(model = rownames(mtcars), 3.14 + mtcars))

    ## "-"
    checkParquetFactTable(tbl - sqrt(tbl), cbind(model = rownames(mtcars), mtcars - sqrt(mtcars)))
    checkParquetFactTable(tbl - tbl[,"carb"], cbind(model = rownames(mtcars), mtcars - mtcars[, "carb"]))
    checkParquetFactTable(tbl - 1L, cbind(model = rownames(mtcars), mtcars - 1L))
    checkParquetFactTable(tbl - 3.14, cbind(model = rownames(mtcars), mtcars - 3.14))
    checkParquetFactTable(sqrt(tbl) - tbl, cbind(model = rownames(mtcars), sqrt(mtcars) - mtcars))
    checkParquetFactTable(tbl[,"carb"] - tbl, cbind(model = rownames(mtcars), mtcars[, "carb"] - mtcars))
    checkParquetFactTable(1L - tbl, cbind(model = rownames(mtcars), 1L - mtcars))
    checkParquetFactTable(3.14 - tbl, cbind(model = rownames(mtcars), 3.14 - mtcars))

    ## "*"
    checkParquetFactTable(tbl * sqrt(tbl), cbind(model = rownames(mtcars), mtcars * sqrt(mtcars)))
    checkParquetFactTable(tbl * tbl[,"carb"], cbind(model = rownames(mtcars), mtcars * mtcars[, "carb"]))
    checkParquetFactTable(tbl * 1L, cbind(model = rownames(mtcars), mtcars * 1L))
    checkParquetFactTable(tbl * 3.14, cbind(model = rownames(mtcars), mtcars * 3.14))
    checkParquetFactTable(sqrt(tbl) * tbl, cbind(model = rownames(mtcars), sqrt(mtcars) * mtcars))
    checkParquetFactTable(tbl[,"carb"] * tbl, cbind(model = rownames(mtcars), mtcars[, "carb"] * mtcars))
    checkParquetFactTable(1L * tbl, cbind(model = rownames(mtcars), 1L * mtcars))
    checkParquetFactTable(3.14 * tbl, cbind(model = rownames(mtcars), 3.14 * mtcars))

    ## "/"
    checkParquetFactTable(tbl / sqrt(tbl), cbind(model = rownames(mtcars), mtcars / sqrt(mtcars)))
    checkParquetFactTable(tbl / tbl[,"carb"], cbind(model = rownames(mtcars), mtcars / mtcars[, "carb"]))
    checkParquetFactTable(tbl / 1L, cbind(model = rownames(mtcars), mtcars / 1L))
    checkParquetFactTable(tbl / 3.14, cbind(model = rownames(mtcars), mtcars / 3.14))
    checkParquetFactTable(sqrt(tbl) / tbl, cbind(model = rownames(mtcars), sqrt(mtcars) / mtcars))
    checkParquetFactTable(tbl[,"carb"] / tbl, cbind(model = rownames(mtcars), mtcars[, "carb"] / mtcars))
    checkParquetFactTable(1L / tbl, cbind(model = rownames(mtcars), 1L / mtcars))
    checkParquetFactTable(3.14 / tbl, cbind(model = rownames(mtcars), 3.14 / mtcars))

    ## "^"
    checkParquetFactTable(tbl ^ sqrt(tbl), cbind(model = rownames(mtcars), mtcars ^ sqrt(mtcars)))
    checkParquetFactTable(tbl ^ tbl[,"carb"], cbind(model = rownames(mtcars), mtcars ^ mtcars[, "carb"]))
    checkParquetFactTable(tbl ^ 1L, cbind(model = rownames(mtcars), mtcars ^ 1L))
    checkParquetFactTable(tbl ^ 3.14, cbind(model = rownames(mtcars), mtcars ^ 3.14))
    checkParquetFactTable(sqrt(tbl) ^ tbl, cbind(model = rownames(mtcars), sqrt(mtcars) ^ mtcars))
    checkParquetFactTable(tbl[,"carb"] ^ tbl, cbind(model = rownames(mtcars), mtcars[, "carb"] ^ mtcars))
    checkParquetFactTable(1L ^ tbl, cbind(model = rownames(mtcars), 1L ^ mtcars))
    checkParquetFactTable(3.14 ^ tbl, cbind(model = rownames(mtcars), 3.14 ^ mtcars))

    ## "%%"
    checkParquetFactTable(tbl %% sqrt(tbl), cbind(model = rownames(mtcars), mtcars %% sqrt(mtcars)))
    checkParquetFactTable(tbl %% tbl[,"carb"], cbind(model = rownames(mtcars), mtcars %% mtcars[, "carb"]))
    checkParquetFactTable(tbl %% 1L, cbind(model = rownames(mtcars), mtcars %% 1L))
    checkParquetFactTable(tbl %% 3.14, cbind(model = rownames(mtcars), mtcars %% 3.14))
    checkParquetFactTable(sqrt(tbl) %% tbl, cbind(model = rownames(mtcars), sqrt(mtcars) %% mtcars))
    checkParquetFactTable(tbl[,"carb"] %% tbl, cbind(model = rownames(mtcars), mtcars[, "carb"] %% mtcars))
    checkParquetFactTable(1L %% tbl, cbind(model = rownames(mtcars), 1L %% mtcars))
    checkParquetFactTable(3.14 %% tbl, cbind(model = rownames(mtcars), 3.14 %% mtcars))

    ## "%/%"
    checkParquetFactTable(tbl %/% sqrt(tbl), cbind(model = rownames(mtcars), mtcars %/% sqrt(mtcars)))
    checkParquetFactTable(tbl %/% tbl[,"carb"], cbind(model = rownames(mtcars), mtcars %/% mtcars[, "carb"]))
    checkParquetFactTable(tbl %/% 1L, cbind(model = rownames(mtcars), mtcars %/% 1L))
    checkParquetFactTable(tbl %/% 3.14, cbind(model = rownames(mtcars), mtcars %/% 3.14))
    checkParquetFactTable(sqrt(tbl) %/% tbl, cbind(model = rownames(mtcars), sqrt(mtcars) %/% mtcars))
    checkParquetFactTable(tbl[,"carb"] %/% tbl, cbind(model = rownames(mtcars), mtcars[, "carb"] %/% mtcars))
    checkParquetFactTable(1L %/% tbl, cbind(model = rownames(mtcars), 1L %/% mtcars))
    checkParquetFactTable(3.14 %/% tbl, cbind(model = rownames(mtcars), 3.14 %/% mtcars))
})

test_that("Compare methods work as expected for a ParquetFactTable", {
    tbl <- ParquetFactTable(titanic_path, key = c("Class", "Sex", "Age", "Survived"), fact = "fate")

    ## "=="
    checkParquetFactTable(tbl == sqrt(tbl), cbind(titanic_df[,1:4], fate = titanic_df$fate == sqrt(titanic_df$fate)))
    checkParquetFactTable(tbl == 1L, cbind(titanic_df[,1:4], fate = titanic_df$fate == 1L))
    checkParquetFactTable(tbl == 3.14, cbind(titanic_df[,1:4], fate = titanic_df$fate == 3.14))
    checkParquetFactTable(1L == tbl, cbind(titanic_df[,1:4], fate = 1L == titanic_df$fate))
    checkParquetFactTable(3.14 == tbl, cbind(titanic_df[,1:4], fate = 3.14 == titanic_df$fate))

    ## ">"
    checkParquetFactTable(tbl > sqrt(tbl), cbind(titanic_df[,1:4], fate = titanic_df$fate > sqrt(titanic_df$fate)))
    checkParquetFactTable(tbl > 1L, cbind(titanic_df[,1:4], fate = titanic_df$fate > 1L))
    checkParquetFactTable(tbl > 3.14, cbind(titanic_df[,1:4], fate = titanic_df$fate > 3.14))
    checkParquetFactTable(1L > tbl, cbind(titanic_df[,1:4], fate = 1L > titanic_df$fate))
    checkParquetFactTable(3.14 > tbl, cbind(titanic_df[,1:4], fate = 3.14 > titanic_df$fate))

    ## "<"
    checkParquetFactTable(tbl < sqrt(tbl), cbind(titanic_df[,1:4], fate = titanic_df$fate < sqrt(titanic_df$fate)))
    checkParquetFactTable(tbl < 1L, cbind(titanic_df[,1:4], fate = titanic_df$fate < 1L))
    checkParquetFactTable(tbl < 3.14, cbind(titanic_df[,1:4], fate = titanic_df$fate < 3.14))
    checkParquetFactTable(1L < tbl, cbind(titanic_df[,1:4], fate = 1L < titanic_df$fate))
    checkParquetFactTable(3.14 < tbl, cbind(titanic_df[,1:4], fate = 3.14 < titanic_df$fate))

    ## "!="
    checkParquetFactTable(tbl != sqrt(tbl), cbind(titanic_df[,1:4], fate = titanic_df$fate != sqrt(titanic_df$fate)))
    checkParquetFactTable(tbl != 1L, cbind(titanic_df[,1:4], fate = titanic_df$fate != 1L))
    checkParquetFactTable(tbl != 3.14, cbind(titanic_df[,1:4], fate = titanic_df$fate != 3.14))
    checkParquetFactTable(1L != tbl, cbind(titanic_df[,1:4], fate = 1L != titanic_df$fate))
    checkParquetFactTable(3.14 != tbl, cbind(titanic_df[,1:4], fate = 3.14 != titanic_df$fate))

    ## "<="
    checkParquetFactTable(tbl <= sqrt(tbl), cbind(titanic_df[,1:4], fate = titanic_df$fate <= sqrt(titanic_df$fate)))
    checkParquetFactTable(tbl <= 1L, cbind(titanic_df[,1:4], fate = titanic_df$fate <= 1L))
    checkParquetFactTable(tbl <= 3.14, cbind(titanic_df[,1:4], fate = titanic_df$fate <= 3.14))
    checkParquetFactTable(1L <= tbl, cbind(titanic_df[,1:4], fate = 1L <= titanic_df$fate))
    checkParquetFactTable(3.14 <= tbl, cbind(titanic_df[,1:4], fate = 3.14 <= titanic_df$fate))

    ## ">="
    checkParquetFactTable(tbl >= sqrt(tbl), cbind(titanic_df[,1:4], fate = titanic_df$fate >= sqrt(titanic_df$fate)))
    checkParquetFactTable(tbl >= 1L, cbind(titanic_df[,1:4], fate = titanic_df$fate >= 1L))
    checkParquetFactTable(tbl >= 3.14, cbind(titanic_df[,1:4], fate = titanic_df$fate >= 3.14))
    checkParquetFactTable(1L >= tbl, cbind(titanic_df[,1:4], fate = 1L >= titanic_df$fate))
    checkParquetFactTable(3.14 >= tbl, cbind(titanic_df[,1:4], fate = 3.14 >= titanic_df$fate))
})

test_that("Logic methods work as expected for a ParquetFactTable", {
    tbl <- ParquetFactTable(titanic_path, key = c("Class", "Sex", "Age", "Survived"), fact = "fate")

    ## "&"
    x <- tbl > 70
    y <- tbl < 4000
    checkParquetFactTable(x & y, cbind(titanic_df[,1:4], fate = titanic_df$fate > 70 & titanic_df$fate < 4000))

    ## "|"
    x <- tbl > 70
    y <- sqrt(tbl) > 0
    checkParquetFactTable(x | y, cbind(titanic_df[,1:4], fate = titanic_df$fate > 70 | sqrt(titanic_df$fate) > 0))
})

test_that("Math methods work as expected for a ParquetFactTable", {
    tbl <- ParquetFactTable(state_path, key = c("region", "division", "rowname", "colname"), fact = "value")

    income <- tbl[list(colname = "Income"), ]
    income_df <- state_df[state_df$colname == "Income", ]

    ikeep <-
      c("Colorado", "Delaware", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas",
        "Maine", "Maryland", "Michigan", "Minnesota", "Missouri", "Montana",
        "Nebraska", "Nevada", "New Hampshire", "North Dakota", "Ohio", "Oregon",
        "Pennsylvania", "South Dakota", "Utah", "Vermont", "Washington", "Wisconsin",
        "Wyoming")
    illiteracy <- tbl[list(rowname = ikeep, colname = "Illiteracy"), ]
    illiteracy_df <- state_df[state_df$rowname %in% ikeep & state_df$colname == "Illiteracy", ]

    checkParquetFactTable(abs(income), cbind(income_df[, 1:4], value = abs(income_df$value)))
    checkParquetFactTable(sign(income), cbind(income_df[, 1:4], value = sign(income_df$value)))
    checkParquetFactTable(sqrt(income), cbind(income_df[, 1:4], value = sqrt(income_df$value)))
    checkParquetFactTable(ceiling(income), cbind(income_df[, 1:4], value = ceiling(income_df$value)))
    checkParquetFactTable(floor(income), cbind(income_df[, 1:4], value = floor(income_df$value)))
    checkParquetFactTable(trunc(income), cbind(income_df[, 1:4], value = trunc(income_df$value)))

    expect_error(cummax(income))
    expect_error(cummin(income))
    expect_error(cumprod(income))
    expect_error(cumsum(income))

    checkParquetFactTable(log(income), cbind(income_df[, 1:4], value = log(income_df$value)))
    checkParquetFactTable(log10(income), cbind(income_df[, 1:4], value = log10(income_df$value)))
    checkParquetFactTable(log2(income), cbind(income_df[, 1:4], value = log2(income_df$value)))
    checkParquetFactTable(log1p(income), cbind(income_df[, 1:4], value = log1p(income_df$value)))

    checkParquetFactTable(acos(illiteracy), cbind(illiteracy_df[, 1:4], value = acos(illiteracy_df$value)))

    expect_error(acosh(illiteracy))

    checkParquetFactTable(asin(illiteracy), cbind(illiteracy_df[, 1:4], value = asin(illiteracy_df$value)))

    expect_error(asinh(illiteracy))
    expect_error(atan(illiteracy))
    expect_error(atanh(illiteracy))

    checkParquetFactTable(exp(income), cbind(income_df[, 1:4], value = exp(income_df$value)))

    expect_error(expm1(income))

    checkParquetFactTable(cos(income), cbind(income_df[, 1:4], value = cos(income_df$value)))

    expect_error(cosh(income))
    expect_error(cospi(income))

    checkParquetFactTable(sin(income), cbind(income_df[, 1:4], value = sin(income_df$value)))

    expect_error(sinh(income))
    expect_error(sinpi(income))

    checkParquetFactTable(tan(income), cbind(income_df[, 1:4], value = tan(income_df$value)))

    expect_error(tanh(income))
    expect_error(tanpi(income))

    expect_error(gamma(income))
    expect_error(lgamma(income))
    expect_error(digamma(income))
    expect_error(trigamma(income))
})
