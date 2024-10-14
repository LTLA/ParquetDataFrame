# Tests the basic functions of a ParquetDataFrame.
# library(testthat); library(ParquetDataFrame); source("setup.R"); source("test-ParquetDataFrame.R")

x <- ParquetDataFrame(infert_path)

test_that("basic methods work for a ParquetDataFrame", {
    expect_identical(ncol(x), ncol(infert_df))
    expect_identical(nrow(x), nrow(infert_df))
    expect_null(rownames(x))
    expect_identical(colnames(x), colnames(infert_df))

    unnamed <- infert_df
    rownames(unnamed) <- NULL
    expect_identical(as.data.frame(x), unnamed)
})

test_that("renaming columns creates a new ParquetDataFrame", {
    copy <- x
    replacements <- sprintf("COL%i", seq_len(ncol(x)))
    colnames(copy) <- replacements
    expect_s4_class(copy, "ParquetDataFrame")
    expect_identical(colnames(copy), replacements)

    copy <- x
    colnames(copy) <- colnames(x)
    expect_s4_class(copy, "ParquetDataFrame")
})

test_that("adding rownames collapses to an ordinary DFrame", {
    copy <- x
    replacements <- sprintf("ROW%i", seq_len(nrow(x)))
    rownames(copy) <- replacements
    expect_s4_class(copy, "DFrame")
    expect_identical(rownames(copy), replacements)

    copy <- x
    rownames(copy) <- NULL
    expect_s4_class(copy, "ParquetDataFrame")
})

test_that("slicing by columns preserves type of a ParquetDataFrame", {
    copy <- x[,1:2]
    expect_s4_class(copy, "ParquetDataFrame")
    expect_identical(colnames(copy), colnames(infert_df)[1:2])
    expect_identical(ncol(copy), 2L)
    unnamed <- infert_df[,1:2]
    rownames(unnamed) <- NULL
    expect_identical(as.data.frame(copy), unnamed)

    cn <- colnames(x)[c(4,2,3)]
    copy <- x[,cn]
    expect_s4_class(copy, "ParquetDataFrame")
    expect_identical(colnames(copy), cn)
    expect_identical(ncol(copy), 3L)

    keep <- startsWith(colnames(x), "d")
    copy <- x[,keep]
    expect_s4_class(copy, "ParquetDataFrame")
    expect_identical(colnames(copy), colnames(infert_df)[keep])
    expect_identical(ncol(copy), sum(keep))

    copy <- x[,5,drop=FALSE]
    expect_s4_class(copy, "ParquetDataFrame")
    expect_identical(colnames(copy), colnames(infert_df)[5])

    # Respects mcols.
    x2 <- x
    mcols(x2) <- DataFrame(whee=seq_len(ncol(x)))
    copy <- x2[,3:1]
    expect_identical(mcols(copy)$whee, 3:1)
})

test_that("extraction of a column yields a ParquetColumnVector", {
    col <- x[,5]
    expect_s4_class(col, "ParquetColumnVector")
    expect_identical(as.vector(col), infert_df[,5])

    nm <- colnames(infert_df)[5]
    col <- x[[nm]]
    expect_s4_class(col, "ParquetColumnVector")
    expect_identical(as.vector(col), infert_df[[nm]])
})

test_that("conditional slicing by rows preserves type of a ParquetDataFrame", {
    i <- x$age > 30
    copy <- x[i,]
    expect_s4_class(copy, "ParquetDataFrame")
    expect_identical(colnames(copy), colnames(infert_df))
    expect_identical(as.vector(copy[[1]]), infert_df[[1]][as.vector(i)])
})

test_that("positional slicing by rows collapses to an ordinary DFrame", {
    i <- sample(nrow(x))
    copy <- x[i,]
    expect_s4_class(copy, "DFrame")
    expect_identical(colnames(copy), colnames(infert_df))
    expect_identical(as.vector(copy[[1]]), infert_df[[1]][i])
})

test_that("head preserves type of a ParquetDataFrame", {
    copy <- head(x, 20)
    expect_s4_class(copy, "ParquetDataFrame")
    expect_identical(colnames(copy), colnames(infert_df))
    expect_identical(as.data.frame(copy), head(infert_df, 20))
})

test_that("tail collapses to an ordinary DFrame", {
    copy <- tail(x, 20)
    expect_s4_class(copy, "DFrame")
    expect_identical(colnames(copy), colnames(infert_df))
    expect_identical(as.vector(copy[[1]]), tail(infert_df[[1]], 20))
})

test_that("subset assignments that collapse to an ordinary DFrame", {
    copy <- x
    copy[1:5,] <- copy[9:13,]
    expect_s4_class(copy, "DFrame")
    expect_s4_class(copy[[1]], "DelayedArray")
    ref <- infert_df
    ref[1:5,] <- ref[9:13,]
    rownames(ref) <- NULL
    expect_identical(as.data.frame(copy), ref)

    copy <- x
    copy[,"foobar"] <- runif(nrow(x))
    expect_s4_class(copy, "DFrame")
    expect_identical(colnames(copy), c(colnames(x), "foobar"))

    copy <- x
    copy$some_random_thing <- runif(nrow(x))
    expect_s4_class(copy, "DFrame")
    expect_identical(colnames(copy), c(colnames(x), "some_random_thing"))
})

test_that("subset assignments that return a ParquetDataFrame", {
    copy <- x
    copy[,1] <- copy[,1]
    expect_s4_class(copy, "ParquetDataFrame")

    copy <- x
    copy[,colnames(x)[2]] <- copy[,colnames(x)[2],drop=FALSE]
    expect_s4_class(copy, "ParquetDataFrame")

    copy <- x
    copy[[3]] <- copy[[3]]
    expect_s4_class(copy, "ParquetDataFrame")

    copy <- x
    copy[[1]] <- copy[[3]]
    expect_s4_class(copy, "ParquetDataFrame")
    expect_s4_class(copy[[1]], "ParquetColumnVector")
    ref <- infert_df
    ref[[1]] <- ref[[3]]
    rownames(ref) <- NULL
    expect_identical(as.data.frame(copy), ref)

    copy <- x
    copy[,c(1,2,3)] <- copy[,c(4,5,6)]
    expect_s4_class(copy, "ParquetDataFrame")
    expect_s4_class(copy[[1]], "ParquetColumnVector")
    ref <- infert_df
    ref[,c(1,2,3)] <- ref[,c(4,5,6)]
    rownames(ref) <- NULL
    expect_identical(as.data.frame(copy), ref)
})

test_that("rbinding collapses to an ordinary DFrame", {
    copy <- rbind(x, x)
    expect_s4_class(copy, "DFrame")
    expect_s4_class(copy[[1]], "DelayedArray")

    ref <- rbind(infert_df, infert_df)
    rownames(ref) <- NULL
    expect_identical(as.data.frame(copy), ref)
})

test_that("cbinding may or may not collapse to an ordinary DFrame", {
    # Same path, we get another PDF.
    copy <- cbind(x, foo=x[["age"]])
    expect_s4_class(copy, "ParquetDataFrame")
    expect_identical(colnames(copy), c(colnames(infert_df), "foo"))

    # Duplicate names causes unique renaming.
    copy <- cbind(x, x)
    expect_s4_class(copy, "ParquetDataFrame")
    expect_identical(colnames(copy), make.unique(rep(colnames(x), 2), sep="_"))

    # Duplicate names causes unique renaming.
    copy <- cbind(x, age=x[["age"]])
    expect_s4_class(copy, "ParquetDataFrame")
    expect_identical(colnames(copy), c(colnames(infert_df), "age_1"))

    # Duplicate names causes unique renaming.
    copy <- cbind(age=x[["age"]], x)
    expect_s4_class(copy, "ParquetDataFrame")
    expect_identical(colnames(copy), make.unique(c("age", colnames(infert_df)), sep="_"))

    # Different DataFrame class causes collapse.
    copy <- cbind(x, infert_df)
    expect_s4_class(copy, "DFrame")
    expect_identical(colnames(copy), rep(colnames(infert_df), 2))

    # Different paths causes collapse.
    tmp <- tempfile()
    file.symlink(infert_path, tmp)
    x2 <- ParquetDataFrame(tmp)
    copy <- cbind(x, x2)
    expect_s4_class(copy, "DFrame")

    # Different paths causes collapse.
    copy <- cbind(x, age=x2[["age"]])
    expect_s4_class(copy, "DFrame")
    expect_identical(colnames(copy), c(colnames(infert_df), "age"))
})

test_that("cbinding carries forward any metadata", {
    x1 <- x
    colnames(x1) <- paste0(colnames(x1), "_1")
    mcols(x1) <- DataFrame(whee="A")

    x2 <- x
    colnames(x2) <- paste0(colnames(x2), "_2")
    mcols(x2) <- DataFrame(whee="B")

    copy <- cbind(x1, x2)
    expect_s4_class(copy, "ParquetDataFrame")
    expect_identical(mcols(copy)$whee, rep(c("A", "B"), each=ncol(x)))

    mcols(x1) <- NULL
    copy <- cbind(x1, x2)
    expect_s4_class(copy, "ParquetDataFrame")
    expect_identical(mcols(copy)$whee, rep(c(NA, "B"), each=ncol(x)))

    metadata(x1) <- list(a="YAY")
    metadata(x2) <- list(a="whee")
    copy <- cbind(x1, x2)
    expect_s4_class(copy, "ParquetDataFrame")
    expect_identical(metadata(copy), list(a="YAY", a="whee"))
})

test_that("as.data.frame works with duplicated columns", {
    duplicates <- c(1,1,2,2,3,4,3,5)
    copy <- x[,duplicates]
    unnamed <- infert_df[,duplicates]
    rownames(unnamed) <- NULL
    expect_identical(as.data.frame(copy), unnamed)
})
