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
#' nrow,ParquetDataFrame-method
#' ncol,ParquetDataFrame-method
#' length,ParquetDataFrame-method
#' path,ParquetDataFrame-method
#'
#' rownames,ParquetDataFrame-method
#' names,ParquetDataFrame-method
#' rownames<-,ParquetDataFrame-method
#' names<-,ParquetDataFrame-method
#'
#' extractROWS,ParquetDataFrame,ANY-method
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
#' @export
ParquetDataFrame <- function(path, ...) {
    tab <- acquireTable(path, ...)
    new("ParquetDataFrame", path=path, columns=colnames(tab), nrows=nrow(tab))
}

#' @export
setClass("ParquetDataFrame", contains="DataFrame", slots=c(path="character", columns="character", nrows="integer"))

#' @export
setMethod("nrow", "ParquetDataFrame", function(x) x@nrows)

#' @export
setMethod("length", "ParquetDataFrame", function(x) length(x@columns))

#' @export
setMethod("path", "ParquetDataFrame", function(object) object@path)

#' @export
setMethod("rownames", "ParquetDataFrame", function(x) NULL)

#' @export
setMethod("names", "ParquetDataFrame", function(x) x@columns)

#' @export
setReplaceMethod("rownames", "ParquetDataFrame", function(x, value) {
    if (!is.null(value)) {
        x <- .collapse_to_df(x)
        rownames(x) <- value
    }
    x
})

#' @export
setReplaceMethod("names", "ParquetDataFrame", function(x, value) {
    if (!identical(value, names(x))) {
        x <- .collapse_to_df(x)
        names(x) <- value
    }
    x
})

#' @export
#' @importFrom S4Vectors extractROWS
setMethod("extractROWS", "ParquetDataFrame", function(x, i) {
    if (!missing(i)) {
        collapsed <- .collapse_to_df(x)
        extractROWS(collapsed, i)
    } else {
        x
    }
})

#' @export
#' @importFrom stats setNames
#' @importFrom S4Vectors extractCOLS normalizeSingleBracketSubscript
setMethod("extractCOLS", "ParquetDataFrame", function(x, i) {
    if (!missing(i)) {
        xstub <- setNames(seq_along(x), names(x))
        i <- normalizeSingleBracketSubscript(i, xstub)
        x@columns <- x@columns[i]
        x@elementMetadata <- extractROWS(x@elementMetadata, i)
    }
    x
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
    ParquetColumnVector(x@path, column=x@columns[i])
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
        return(new("ParquetDataFrame", path=value@seed@path, columns=value@seed@column, nrows=length(value)))
    }
    callNextMethod()
})

#' @export
#' @importFrom stats setNames
#' @importFrom S4Vectors replaceCOLS normalizeSingleBracketSubscript
setMethod("replaceCOLS", "ParquetDataFrame", function(x, i, value) {
    xstub <- setNames(seq_along(x), names(x))
    i2 <- normalizeSingleBracketSubscript(i, xstub, allow.NAs=TRUE)
    if (length(i2) == 1L && !is.na(i2)) {
        if (is(value, "ParquetDataFrame")) {
            if (x@path == value@path && identical(x@columns[i2], value@columns)) {
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
    i2 <- normalizeDoubleBracketSubscript(i, x, allow.nomatch=TRUE)
    if (length(i2) == 1L && !is.na(i2)) {
        if (is(value, "ParquetColumnVector")) {
            if (x@path == value@seed@path && x@columns[i2] == value@seed@column) {
                return(x)
            }
        }
    }

    x <- .collapse_to_df(x)
    x[[i]] <- value
    x
})

#' @export
#' @importFrom S4Vectors mcols make_zero_col_DFrame combineRows
cbind.ParquetDataFrame <- function(..., deparse.level=1) {
    preserved <- TRUE
    all_columns <- character(0)
    objects <- list(...)
    xpath <- NULL

    for (i in seq_along(objects)) {
        obj <- objects[[i]]
        if (is(obj, "ParquetDataFrame")) {
            if (is.null(xpath)) {
                xpath <- obj@path
            } else if (obj@path != xpath) {
                preserved <- FALSE
                break
            } 
            all_columns <- c(all_columns, obj@columns)

        } else if (is(obj, "ParquetColumnVector")) {
            if (is.null(xpath)) {
                xpath <- obj@seed@path
            } else if (obj@seed@path != xpath || !identical(names(objects)[i], obj@seed@column)) {
                preserved <- FALSE
                break
            } 
            all_columns <- c(all_columns, obj@seed@column)

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
        all_mcols <- list()
        has_mcols <- FALSE
        all_metadata <- list()

        for (i in seq_along(objects)) {
            obj <- objects[[i]]

            mc <- NULL
            md <- list()
            if (is(obj, "DataFrame")) {
                mc <- mcols(obj, use.names=FALSE)
                md <- metadata(obj)
                if (is.null(mc)) {
                    mc <- make_zero_col_DFrame(length(obj))
                } else {
                    has_mcols <- TRUE
                }
            } else {
                mc <- make_zero_col_DFrame(1)
            }

            all_mcols[[i]] <- mc
            all_metadata[[i]] <- md
        }

        if (has_mcols) {
            all_mcols <- do.call(combineRows, all_mcols)
        } else {
            all_mcols <- NULL
        }

        new("ParquetDataFrame", 
            path=xpath,
            columns=all_columns,
            nrows=NROW(objects[[1]]),
            elementMetadata=all_mcols,
            metadata=do.call(c, all_metadata)
        )
    }
}

#' @export
#' @importFrom S4Vectors bindCOLS
setMethod("cbind", "ParquetDataFrame", cbind.ParquetDataFrame)

#' @importFrom S4Vectors make_zero_col_DFrame mcols mcols<- metadata metadata<-
.collapse_to_df <- function(x) {
    df <- make_zero_col_DFrame(x@nrows)
    for (i in seq_along(x@columns)) {
        df[[as.character(i)]] <- ParquetColumnVector(x@path, column=x@columns[i])
    }
    colnames(df) <- x@columns
    mcols(df) <- mcols(x, use.names=FALSE)
    metadata(df) <- metadata(x)
    df
}

#' @export
setMethod("as.data.frame", "ParquetDataFrame", function(x, row.names = NULL, optional = FALSE, ...) {
    tab <- acquireTable(x@path)

    ucol <- unique(x@columns)
    is.same <- identical(x@columns, ucol)
    tab <- tab[,ucol]

    output <- as.data.frame(tab, row.names=row.names, optional=optional, ...)
    output <- output[,match(x@columns,colnames(output)),drop=FALSE]

    output
})

#' @export
setAs("ParquetDataFrame", "DFrame", function(from) .collapse_to_df(from))
