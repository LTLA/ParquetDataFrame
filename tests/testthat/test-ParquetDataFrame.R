# Tests the basic functions of a ParquetDataFrame.
# library(testthat); library(ParquetDataFrame); source("setup.R"); source("test-ParquetDataFrame.R")

test_that("basic methods work for a ParquetDataFrame", {
    df <- ParquetDataFrame(mtcars_path, key = "model")
    checkParquetDataFrame(df, mtcars)

    df <- ParquetDataFrame(mtcars_path, key = list(model = rownames(mtcars)))
    checkParquetDataFrame(df, mtcars)
    expect_identical(rownames(df), rownames(mtcars))
    expect_identical(as.data.frame(df), mtcars)
})

test_that("renaming columns creates a new ParquetDataFrame", {
    df <- ParquetDataFrame(mtcars_path, key = list(model = rownames(mtcars)))
    expected <- mtcars

    replacements <- sprintf("COL%i", seq_len(ncol(df)))
    colnames(df) <- replacements
    colnames(expected) <- replacements
    checkParquetDataFrame(df, expected)
})

test_that("adding rownames creates a new ParquetDataFrame", {
    df <- ParquetDataFrame(mtcars_path, key = "model")
    expected <- mtcars

    replacements <- sprintf("ROW%i", seq_len(nrow(df)))
    rownames(df) <- replacements
    rownames(expected) <- setNames(names(df@key[[1L]]), df@key[[1L]])[rownames(expected)]
    checkParquetDataFrame(df, expected)

    df <- ParquetDataFrame(mtcars_path, key = list(model = rownames(mtcars)))
    expected <- mtcars

    replacements <- sprintf("ROW%i", seq_len(nrow(df)))
    rownames(df) <- replacements
    rownames(expected) <- replacements
    checkParquetDataFrame(df, expected)
})

test_that("slicing by columns preserves type of a ParquetDataFrame", {
    df <- ParquetDataFrame(mtcars_path, key = list(model = rownames(mtcars)))

    keep <- 1:2
    checkParquetDataFrame(df[,keep], mtcars[,keep])

    keep <- colnames(df)[c(4,2,3)]
    checkParquetDataFrame(df[,keep], mtcars[,keep])

    keep <- startsWith(colnames(df), "d")
    checkParquetDataFrame(df[,keep], mtcars[,keep])

    keep <- 5
    checkParquetDataFrame(df[,keep, drop=FALSE], mtcars[,keep, drop=FALSE])

    # Respects mcols.
    copy <- df
    mcols(copy) <- DataFrame(whee=seq_len(ncol(df)))
    copy <- copy[,3:1]
    expect_identical(mcols(copy)$whee, 3:1)
})

test_that("extraction of a column yields a ParquetColumn", {
    df <- ParquetDataFrame(mtcars_path, key = list(model = rownames(mtcars)))

    keep <- 5
    checkParquetColumn(df[,keep], setNames(mtcars[,keep], rownames(mtcars)))

    keep <- colnames(df)[5]
    checkParquetColumn(df[,keep], setNames(mtcars[,keep], rownames(mtcars)))
})

test_that("conditional slicing by rows preserves type of a ParquetDataFrame", {
    df <- ParquetDataFrame(mtcars_path, key = list(model = rownames(mtcars)))
    checkParquetDataFrame(df[df$cyl > 6,], mtcars[mtcars$cyl > 6,])
})

test_that("positional slicing by rows preserves type of a ParquetDataFrame", {
    df <- ParquetDataFrame(mtcars_path, key = list(model = rownames(mtcars)))
    i <- sample(nrow(df))
    checkParquetDataFrame(df[i,], mtcars[i,])
})

test_that("head preserves type of a ParquetDataFrame", {
    df <- ParquetDataFrame(mtcars_path, key = list(model = rownames(mtcars)))
    checkParquetDataFrame(head(df, 20), head(mtcars, 20))
})

test_that("tail preserves type of a ParquetDataFrame", {
    df <- ParquetDataFrame(mtcars_path, key = list(model = rownames(mtcars)))
    checkParquetDataFrame(tail(df, 20), tail(mtcars, 20))
})

test_that("subset assignments that produce errors", {
    df <- ParquetDataFrame(mtcars_path, key = list(model = rownames(mtcars)))
    expect_error(df[1:5,] <- df[9:13,])
    expect_error(df[,"foobar"] <- runif(nrow(df)))
    expect_error(df$some_random_thing <- runif(nrow(df)))
})

test_that("subset assignments that return a ParquetDataFrame", {
    df <- ParquetDataFrame(mtcars_path, key = list(model = rownames(mtcars)))

    copy <- df
    copy[,1] <- copy[,1]
    checkParquetDataFrame(copy, mtcars)

    copy <- df
    copy[,colnames(df)[2]] <- copy[,colnames(df)[2],drop=FALSE]
    checkParquetDataFrame(copy, mtcars)

    copy <- df
    copy[[3]] <- copy[[3]]
    checkParquetDataFrame(copy, mtcars)

    copy <- df
    copy[[1]] <- copy[[3]]
    mtcars2 <- mtcars
    mtcars2[[1]] <- mtcars2[[3]]
    checkParquetDataFrame(copy, mtcars2)

    copy <- df
    copy[,c(1,2,3)] <- copy[,c(4,5,6)]
    mtcars2 <- mtcars
    mtcars2[,c(1,2,3)] <- mtcars2[,c(4,5,6)]
    checkParquetDataFrame(copy, mtcars2)
})

test_that("rbinding produces errors", {
    df <- ParquetDataFrame(mtcars_path, key = list(model = rownames(mtcars)))
    expect_error(rbind(df, df))
})

test_that("cbinding operations that return a ParquetDataFrame", {
    df <- ParquetDataFrame(mtcars_path, key = list(model = rownames(mtcars)))

    # Same path, we get another PDF.
    checkParquetDataFrame(cbind(df, foo=df[["carb"]]), cbind(mtcars, foo=mtcars[["carb"]]))

    # Duplicate names causes unique renaming.
    expected <- cbind(mtcars, mtcars)
    colnames(expected) <- make.unique(colnames(expected), sep="_")
    checkParquetDataFrame(cbind(df, df), expected)

    # Duplicate names causes unique renaming.
    expected <- cbind(mtcars, carb=mtcars[["carb"]])
    colnames(expected) <- make.unique(colnames(expected), sep="_")
    checkParquetDataFrame(cbind(df, carb=df[["carb"]]), expected)

    # Duplicate names causes unique renaming.
    expected <- cbind(carb=mtcars[["carb"]], mtcars)
    colnames(expected) <- make.unique(colnames(expected), sep="_")
    checkParquetDataFrame(cbind(carb=df[["carb"]], df), expected)
})

test_that("cbinding operations that produce errors", {
    df <- ParquetDataFrame(mtcars_path, key = list(model = rownames(mtcars)))

    expect_error(cbind(df, mtcars))

    # Different paths causes an error.
    tmp <- tempfile()
    file.symlink(mtcars_path, tmp)
    df2 <- ParquetDataFrame(tmp, key = list(model = rownames(mtcars)))
    expect_error(cbind(df, df2))
    expect_error(cbind(df, carb=df2[["carb"]]))
})

test_that("cbinding carries forward any metadata", {
    df <- ParquetDataFrame(mtcars_path, key = list(model = rownames(mtcars)))

    df1 <- df
    colnames(df1) <- paste0(colnames(df1), "_1")
    mcols(df1) <- DataFrame(whee="A")

    df2 <- df
    colnames(df2) <- paste0(colnames(df2), "_2")
    mcols(df2) <- DataFrame(whee="B")

    copy <- cbind(df1, df2)
    expect_s4_class(copy, "ParquetDataFrame")
    expect_identical(mcols(copy)$whee, rep(c("A", "B"), each=ncol(df)))

    mcols(df1) <- NULL
    copy <- cbind(df1, df2)
    expect_s4_class(copy, "ParquetDataFrame")
    expect_identical(mcols(copy)$whee, rep(c(NA, "B"), each=ncol(df)))

    metadata(df1) <- list(a="YAY")
    metadata(df2) <- list(a="whee")
    copy <- cbind(df1, df2)
    expect_s4_class(copy, "ParquetDataFrame")
    expect_identical(metadata(copy), list(a="YAY", a="whee"))
})

test_that("extracting duplicate columns produces errors", {
    df <- ParquetDataFrame(mtcars_path, key = list(model = rownames(mtcars)))
    expect_error(df[,c(1,1,2,2,3,4,3,5)])
})
