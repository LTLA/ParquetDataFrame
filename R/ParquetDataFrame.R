#' Parquet-backed DataFrame
#'
#' Create a Parquet-backed \linkS4class{DataFrame}, where the data are kept on disk until requested.
#'
#' @param path String specifying a path to a Parquet data directory or file.
#' @param ... Further arguments to be passed to \code{\link[arrow]{open_dataset}}.
#'
#' @return A ParquetDataFrame where each column is a \linkS4class{ParquetColumnVector}.
#'
#' @details
#' The ParquetDataFrame is essentially just a \linkS4class{DataFrame} of \linkS4class{ParquetColumnVector} objects.
#' It is primarily useful for indicating that the in-memory representation is consistent with the underlying Parquet data
#' (e.g., no delayed filter/mutate operations have been applied, no data has been added from other files).
#' Thus, users can specialize code paths for a ParquetDataFrame to operate directly on the underlying Parquet data.
#'
#' In that vein, operations on a ParquetDataFrame may return another ParquetDataFrame if the operation does not introduce inconsistencies with the file-backed data.
#' For example, slicing or combining by column will return a ParquetDataFrame as the contents of the retained columns are unchanged.
#' In other cases, the ParquetDataFrame will collapse to a regular \linkS4class{DFrame} of \linkS4class{ParquetColumnVector} objects before applying the operation;
#' these are still file-backed but lack the guarantee of file consistency.
#'
#' @author Aaron Lun, Patrick Aboyoun
#' @examples
#' # Mocking up a file:
#' tf <- tempfile()
#' on.exit(unlink(tf))
#' arrow::write_parquet(mtcars, tf)
#'
#' # Creating our Parquet-backed data frame:
#' df <- ParquetDataFrame(tf)
#' df
#'
#' # Extraction yields a ParquetColumnVector:
#' df$carb
#'
#' # Some operations preserve the ParquetDataFrame:
#' df[,1:5]
#' combined <- cbind(df, df)
#' class(combined)
#'
#' # ... but most operations collapse to a regular DFrame:
#' df[1:5,]
#' combined2 <- cbind(df, some_new_name=df[,1])
#' class(combined2)
#'
#' @aliases
#' ParquetDataFrame-class
#'
#' show,ParquetDataFrame-method
#' nrow,ParquetDataFrame-method
#' ncol,ParquetDataFrame-method
#' length,ParquetDataFrame-method
#'
#' rownames,ParquetDataFrame-method
#' names,ParquetDataFrame-method
#' rownames<-,ParquetDataFrame-method
#' names<-,ParquetDataFrame-method
#'
#' extractROWS,ParquetDataFrame,ANY-method
#' head,ParquetDataFrame-method
#' tail,ParquetDataFrame-method
#' extractCOLS,ParquetDataFrame-method
#' [[,ParquetDataFrame-method
#'
#' replaceROWS,ParquetDataFrame-method
#' replaceCOLS,ParquetDataFrame-method
#' normalizeSingleBracketReplacementValue,ParquetDataFrame-method
#' [[<-,ParquetDataFrame-method
#'
#' cbind,ParquetDataFrame-method
#' cbind.ParquetDataFrame
#'
#' as.data.frame,ParquetDataFrame-method
#' coerce,ParquetDataFrame,DFrame-method
#'
#' @include query.R
#' @include acquireDataset.R
#' @include ParquetColumnSeed.R
#'
#' @export
#' @importFrom dplyr everything select
ParquetDataFrame <- function(path, ...) {
    dat <- acquireDataset(path, ...)
    query <- select(dat, everything())
    new("ParquetDataFrame", query = query, nrows = nrow(dat))
}

#' @export
#' @importClassesFrom S4Vectors DataFrame
setClass("ParquetDataFrame", contains = "DataFrame", slots = c(query = "arrow_dplyr_query", nrows = "integer"))

#' @export
#' @importFrom S4Vectors classNameForDisplay get_showHeadLines
#' @importFrom S4Vectors makeNakedCharacterMatrixForDisplay
setMethod("show", "ParquetDataFrame", function(object) {
    nr <- nrow(object)
    nc <- ncol(object)
    k <- min(nr, get_showHeadLines())

    df <- as.data.frame(head(object, k))
    rownames(df) <- as.character(seq_len(k))

    mat <- makeNakedCharacterMatrixForDisplay(df)
    mat <- rbind(rep.int("<ParquetColumnVector>", nc), mat)
    if (nr > k) {
        mat <- rbind(mat, rbind("..." = rep.int("...", nc)))
    }

    cat(classNameForDisplay(object), " with ",
        nr, ngettext(nr, " row", " rows"), " and ",
        nc, ngettext(nc, " column", " columns"), "\n",
        sep = "")
    print(mat, quote = FALSE, right = TRUE)

    invisible(NULL)
})

#' @export
setMethod("query", "ParquetDataFrame", function(x) x@query)

#' @export
setMethod("nrow", "ParquetDataFrame", function(x) x@nrows)

#' @export
setMethod("length", "ParquetDataFrame", function(x) length(names(x)))

#' @export
setMethod("rownames", "ParquetDataFrame", function(x) NULL)

#' @export
setMethod("names", "ParquetDataFrame", function(x) names(query(x)))

#' @export
setReplaceMethod("rownames", "ParquetDataFrame", function(x, value) {
    if (!is.null(value)) {
        x <- .collapse_to_df(x)
        rownames(x) <- value
    }
    x
})

#' @export
#' @importFrom dplyr rename
#' @importFrom S4Vectors mcols
#' @importFrom stats setNames
setReplaceMethod("names", "ParquetDataFrame", function(x, value) {
    if (identical(value, names(x))) {
        return(x)
    }
    query <- rename(query(x), !!!setNames(names(x), value))
    mc <- mcols(x)
    if (!is.null(mc)) {
        rownames(mc) <- value
    }
    initialize(x, query = query, elementMetadata = mc)
})

#' @export
#' @importFrom dplyr filter
#' @importFrom S4Vectors extractROWS
setMethod("extractROWS", "ParquetDataFrame", function(x, i) {
    if (missing(i)) {
        x
    } else if (is(i, "ParquetColumnVector") &&
               (seed(i)@type == "logical") &&
               identicalQueryBody(query(x), query(i))) {
        keep <- query(i)$selected_columns[[1L]]
        query <- filter(query(x), !!keep)
        initialize(x, query = query, nrows = nrow(query))
    } else {
        collapsed <- .collapse_to_df(x)
        extractROWS(collapsed, i)
    }
})

#' @export
#' @importFrom S4Vectors head isSingleNumber
setMethod("head", "ParquetDataFrame", function(x, n = 6L, ...) {
    if (!isSingleNumber(n)) {
        stop("'n' must be a single number")
    }
    n <- as.integer(n)
    if (n < 0) {
        n <- max(0L, nrow(x) + n)
    }
    if (n > nrow(x)) {
        x
    } else {
        initialize(x, query = head(query(x), n), nrows = n)
    }
})

#' @export
#' @importFrom S4Vectors isSingleNumber tail
setMethod("tail", "ParquetDataFrame", function(x, n = 6L, ...) {
    x <- .collapse_to_df(x)
    callNextMethod()
})

#' @export
#' @importFrom stats setNames
#' @importFrom S4Vectors extractCOLS mcols normalizeSingleBracketSubscript
setMethod("extractCOLS", "ParquetDataFrame", function(x, i) {
    if (missing(i)) {
        return(x)
    }
    xstub <- setNames(seq_along(x), names(x))
    i <- normalizeSingleBracketSubscript(i, xstub)
    if (anyDuplicated(i)) {
        x <- .collapse_to_df(x)
        extractCOLS(x, i)
    } else {
        query <- query(x)[, i]
        mc <- extractROWS(mcols(x), i)
        initialize(x, query = query, elementMetadata = mc)
    }
})

#' @export
#' @importFrom S4Vectors normalizeDoubleBracketSubscript
setMethod("[[", "ParquetDataFrame", function(x, i, j, ...) {
    if (!missing(j)) {
        stop("list-style indexing of a ParquetDataFrame with non-missing 'j' is not supported")
    }

    if (missing(i) || length(i) != 1L) {
        stop("expected a length-1 'i' for list-style indexing of a ParquetDataFrame")
    }

    i <- normalizeDoubleBracketSubscript(i, x)
    ParquetColumnVector(query(x), column = names(x)[i], length = nrow(x))
})

#' @export
#' @importFrom S4Vectors replaceROWS
setMethod("replaceROWS", "ParquetDataFrame", function(x, i, value) {
    x <- .collapse_to_df(x)
    replaceROWS(x, i, value)
})

#' @export
#' @importFrom S4Vectors normalizeSingleBracketReplacementValue
setMethod("normalizeSingleBracketReplacementValue", "ParquetDataFrame", function(value, x) {
    if (is(value, "ParquetColumnVector")) {
        return(new("ParquetDataFrame", query = query(value), nrows = length(value)))
    }
    callNextMethod()
})

#' @export
#' @importFrom stats setNames
#' @importFrom S4Vectors replaceCOLS normalizeSingleBracketSubscript
setMethod("replaceCOLS", "ParquetDataFrame", function(x, i, value) {
    xstub <- setNames(seq_along(x), names(x))
    i2 <- normalizeSingleBracketSubscript(i, xstub, allow.NAs = TRUE)
    if (!anyNA(i2)) {
        if (is(value, "ParquetDataFrame")) {
            if (identicalQueryBody(query(x), query(value))) {
                x@query$selected_columns[i] <- query(value)$selected_columns
                return(x)
            }
        }
    }

    # In theory, it is tempting to return a ParquetDataFrame; the problem is
    # that assignment will change the mapping of column names to their
    # contents, so it is no longer a pure representation of a ParquetDataFrame.
    x <- .collapse_to_df(x)
    replaceCOLS(x, i, value)
})

#' @export
#' @importFrom S4Vectors normalizeDoubleBracketSubscript
setMethod("[[<-", "ParquetDataFrame", function(x, i, j, ..., value) {
    i2 <- normalizeDoubleBracketSubscript(i, x, allow.nomatch = TRUE)
    if (length(i2) == 1L && !is.na(i2)) {
        if (is(value, "ParquetColumnVector")) {
            if (identicalQueryBody(query(x), query(value))) {
                x@query$selected_columns[i] <- query(value)$selected_columns
                return(x)
            }
        }
    }

    x <- .collapse_to_df(x)
    x[[i]] <- value
    x
})

#' @export
#' @importFrom dplyr rename
#' @importFrom S4Vectors mcols make_zero_col_DFrame combineRows
cbind.ParquetDataFrame <- function(..., deparse.level = 1) {
    preserved <- TRUE
    objects <- list(...)
    xquery <- NULL

    for (i in seq_along(objects)) {
        obj <- objects[[i]]
        if (is(obj, "ParquetColumnVector") || is(obj, "ParquetDataFrame")) {
            if (is(obj, "ParquetColumnVector")) {
                cname <- names(objects)[i]
                if (!is.null(cname)) {
                    obj@seed@query <- rename(query(obj), !!!setNames(names(query(obj)), cname))
                    objects[[i]]@seed@query <- query(obj)
                }
            }

            if (is.null(xquery)) {
                xquery <- query(obj)
            } else if (!identicalQueryBody(query(obj), xquery)) {
                preserved <- FALSE
                break
            }
        } else {
            preserved <- FALSE
            break
        }
    }

    if (!preserved) {
        for (i in seq_along(objects)) {
            obj <- objects[[i]]
            if (is(obj, "ParquetDataFrame")) {
                objects[[i]] <- .collapse_to_df(obj)
            }
        }
        do.call(cbind, objects)
    } else {
        all_selected_columns <- vector("list", length(objects))
        all_mcols <- vector("list", length(objects))
        all_metadata <- vector("list", length(objects))
        has_mcols <- FALSE

        for (i in seq_along(objects)) {
            obj <- objects[[i]]

            md <- list()
            mc <- make_zero_col_DFrame(NCOL(obj))
            if (is(obj, "ParquetDataFrame")) {
                mc <- mcols(obj, use.names = FALSE) %||% mc
                has_mcols <- has_mcols || (ncol(mc) > 0L)
                md <- metadata(obj)
            }

            all_selected_columns[[i]] <- query(obj)$selected_columns
            all_mcols[[i]] <- mc
            all_metadata[[i]] <- md
        }

        cols <- do.call(c, all_selected_columns)
        names(cols) <- make.unique(names(cols), sep = "_")
        xquery$selected_columns <- cols

        if (has_mcols) {
            all_mcols <- do.call(combineRows, all_mcols)
            rownames(all_mcols) <- names(cols)
        } else {
            all_mcols <- NULL
        }

        new("ParquetDataFrame", 
            query = xquery,
            nrows = NROW(objects[[1L]]),
            elementMetadata = all_mcols,
            metadata = do.call(c, all_metadata)
        )
    }
}

#' @export
#' @importFrom S4Vectors bindCOLS
setMethod("cbind", "ParquetDataFrame", cbind.ParquetDataFrame)

#' @importFrom S4Vectors make_zero_col_DFrame mcols mcols<- metadata metadata<-
.collapse_to_df <- function(x) {
    df <- make_zero_col_DFrame(nrow(x))
    for (i in names(x)) {
        df[[i]] <- ParquetColumnVector(query(x), column = i, length = nrow(x))
    }
    mcols(df) <- mcols(x, use.names = FALSE)
    metadata(df) <- metadata(x)
    df
}

#' @export
setMethod("as.data.frame", "ParquetDataFrame", function(x, row.names = NULL, optional = FALSE, ...) {
    as.data.frame(query(x), row.names = row.names, optional = optional, ...)
})

#' @export
#' @importClassesFrom S4Vectors DFrame
setAs("ParquetDataFrame", "DFrame", function(from) .collapse_to_df(from))
