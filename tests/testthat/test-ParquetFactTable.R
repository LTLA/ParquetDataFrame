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
