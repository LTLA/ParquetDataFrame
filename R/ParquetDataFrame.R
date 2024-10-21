#' Parquet-backed DataFrame
#'
#' Create a Parquet-backed \linkS4class{DataFrame}, where the data are kept on disk until requested.
#'
#' @param data Either a string containing the path to the Parquet data,
#' or an \code{arrow_dplyr_query} object.
#' @param key Either a character vector or a named list of character vectors
#' containing the names of the columns in the Parquet data that specify
#' the primary key of the array.
#' @param ... Further arguments to be passed to \code{\link[arrow]{open_dataset}}.
#'
#' @return A ParquetDataFrame where each column is a \linkS4class{ParquetColumn}.
#'
#' @details
#' The ParquetDataFrame is essentially just a \linkS4class{DataFrame} of \linkS4class{ParquetColumn} objects.
#' It is primarily useful for indicating that the in-memory representation is consistent with the underlying Parquet data
#' (e.g., no delayed filter/mutate operations have been applied, no data has been added from other files).
#' Thus, users can specialize code paths for a ParquetDataFrame to operate directly on the underlying Parquet data.
#'
#' In that vein, operations on a ParquetDataFrame may return another ParquetDataFrame if the operation does not introduce inconsistencies with the file-backed data.
#' For example, slicing or combining by column will return a ParquetDataFrame as the contents of the retained columns are unchanged.
#' In other cases, the ParquetDataFrame will collapse to a regular \linkS4class{DFrame} of \linkS4class{ParquetColumn} objects before applying the operation;
#' these are still file-backed but lack the guarantee of file consistency.
#'
#' @author Aaron Lun, Patrick Aboyoun
#' @examples
#' # Mocking up a file:
#' tf <- tempfile()
#' on.exit(unlink(tf))
#' arrow::write_parquet(cbind(model = rownames(mtcars), mtcars), tf)
#'
#' # Creating our Parquet-backed data frame:
#' df <- ParquetDataFrame(tf, key = "model")
#' df
#'
#' # Extraction yields a ParquetColumn:
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
#' makeNakedCharacterMatrixForDisplay,ParquetDataFrame-method
#'
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
#' @include arrow_query.R
#' @include acquireDataset.R
#' @include ParquetColumn.R
#'
#' @name ParquetDataFrame
NULL

#' @export
#' @importFrom dplyr everything select
#' @importFrom S4Vectors isSingleString
#' @importFrom stats setNames
ParquetDataFrame <- function(data, key, ...) {
    if (inherits(data, "arrow_dplyr_query")) {
        query <- data
    } else {
        query <- select(acquireDataset(data, ...), everything())
    }

    if (isSingleString(key)) {
        rownames <- pull(distinct(select(query, as.name(!!key))), as_vector = TRUE)
        names(rownames) <- rownames
        key <- setNames(list(rownames), key)
    }

    if (is.list(key)) {
        key <- CharacterList(key)
    }

    if (is(key, "CharacterList") && (length(key) == 1L) && is.null(names(key[[1L]]))) {
        names(key[[1L]]) <- key[[1L]]
    }

    new("ParquetDataFrame", query = query, key = key)
}

#' @export
#' @importClassesFrom BiocGenerics OutOfMemoryObject
#' @importClassesFrom IRanges CharacterList
#' @importClassesFrom S4Vectors DataFrame
setClass("ParquetDataFrame", contains = c("DataFrame", "OutOfMemoryObject"), slots = c(query = "arrow_dplyr_query", key = "CharacterList"))

#' @importFrom S4Vectors setValidity2
setValidity2("ParquetDataFrame", function(x) {
    if ((length(x@key) != 1L) || is.null(names(x@key[[1L]])) ||
         is.null(names(x@key)) || !(names(x@key) %in% names(x@query))) {
        return("'key' slot must be a named list containing a single named character vector")
    }
    TRUE
})

#' @export
#' @importFrom S4Vectors makeNakedCharacterMatrixForDisplay
setMethod("makeNakedCharacterMatrixForDisplay", "ParquetDataFrame", function(x) {
    callNextMethod(as.data.frame(x))
})

#' @export
setMethod("arrow_query", "ParquetDataFrame", function(x) x@query)

#' @export
setMethod("nrow", "ParquetDataFrame", function(x) length(x@key[[1L]]))

#' @export
setMethod("length", "ParquetDataFrame", function(x) length(names(x)))

#' @export
setMethod("rownames", "ParquetDataFrame", function(x) names(x@key[[1L]]))

#' @export
setMethod("names", "ParquetDataFrame", function(x) setdiff(names(x@query), names(x@key)))

#' @export
setReplaceMethod("rownames", "ParquetDataFrame", function(x, value) {
    names(x@key[[1L]]) <- value
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
    query <- rename(x@query, !!!setNames(names(x), value))
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
        return(x)
    }

    key <- x@key
    if (is.atomic(i)) {
        if (is.character(i) && !all(i %in% x@key[[1L]])) {
            stop("'i' contains row names not present in the Parquet key")
        }
        key[[1L]] <- key[[1L]][i]
    } else if (is(i, "ParquetColumn") &&
               (i@data@seed@type == "logical") &&
               .identicalQueryBody(x@query, i@data@seed@query) &&
               identical(x@key, i@data@seed@key)) {
        keep <- i@data@seed@query$selected_columns[i@data@seed@value]
        query <- filter(x@query, !!keep)
        rownames <- pull(distinct(select(query, as.name(!!names(x@key)))), as_vector = TRUE)
        key[[1L]] <- key[[1L]][match(rownames, key[[1L]])]
    } else {
        stop("unsupported 'i' for subsetting a ParquetDataFrame")
    }

    initialize(x, key = key)
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
        key <- x@key
        key[[1L]] <- head(x@key[[1L]], n)
        initialize(x, key = key)
    }
})

#' @export
#' @importFrom S4Vectors isSingleNumber tail
setMethod("tail", "ParquetDataFrame", function(x, n = 6L, ...) {
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
        key <- x@key
        key[[1L]] <- tail(x@key[[1L]], n)
        initialize(x, key = key)
    }
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
        x <- .collapse_to_dframe(x)
        extractCOLS(x, i)
    } else {
        query <- x@query[, c(names(x@key), names(x)[i])]
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
    new("ParquetColumn", data = ParquetArray(x@query, key = x@key, value = names(x)[i]))
})

#' @export
#' @importFrom S4Vectors replaceROWS
setMethod("replaceROWS", "ParquetDataFrame", function(x, i, value) {
    x <- .collapse_to_dframe(x)
    callGeneric(x, i, value)
})

#' @export
#' @importFrom S4Vectors normalizeSingleBracketReplacementValue
setMethod("normalizeSingleBracketReplacementValue", "ParquetDataFrame", function(value, x) {
    if (is(value, "ParquetColumn")) {
        return(new("ParquetDataFrame", query = value@data@seed@query, key = value@data@seed@key))
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
            if (.identicalQueryBody(x@query, value@query) && identical(x@key, value@key)) {
                x@query$selected_columns[names(x)[i2]] <- value@query$selected_columns[names(value)]
                return(x)
            }
        }
    }

    # In theory, it is tempting to return a ParquetDataFrame; the problem is
    # that assignment will change the mapping of column names to their
    # contents, so it is no longer a pure representation of a ParquetDataFrame.
    x <- .collapse_to_dframe(x)
    replaceCOLS(x, i, value)
})

#' @export
#' @importFrom S4Vectors normalizeDoubleBracketSubscript
setMethod("[[<-", "ParquetDataFrame", function(x, i, j, ..., value) {
    i2 <- normalizeDoubleBracketSubscript(i, x, allow.nomatch = TRUE)
    if (length(i2) == 1L && !is.na(i2)) {
        if (is(value, "ParquetColumn")) {
            if (.identicalQueryBody(x@query, value@data@seed@query) &&
                identical(x@key, value@data@seed@key)) {
                x@query$selected_columns[names(x)[i2]] <- value@data@seed@query$selected_columns[value@data@seed@value]
                return(x)
            }
        }
    }

    x <- .collapse_to_dframe(x)
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
    xkey <- NULL

    for (i in seq_along(objects)) {
        obj <- objects[[i]]
        if (is(obj, "ParquetColumn")) {
            cname <- names(objects)[i]
            if (!is.null(cname)) {
                seed <- obj@data@seed
                query <- rename(obj@data@seed@query, !!!setNames(obj@data@seed@value, cname))
                obj@data@seed <- initialize(seed, query = query, value = cname)
                objects[[i]] <- obj
            }
            if (is.null(xquery)) {
                xquery <- obj@data@seed@query
                xkey <- obj@data@seed@key
            } else if (!.identicalQueryBody(obj@data@seed@query, xquery) || !identical(obj@data@seed@key, xkey)) {
                preserved <- FALSE
                break
            }
        } else if (is(obj, "ParquetDataFrame")) {
            if (is.null(xquery)) {
                xquery <- obj@query
                xkey <- obj@key
            } else if (!.identicalQueryBody(obj@query, xquery) || !identical(obj@key, xkey)) {
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
                objects[[i]] <- .collapse_to_dframe(obj)
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
            if (is(obj, "ParquetColumn")) {
                sc <- obj@data@seed@query$selected_columns[obj@data@seed@value]
            } else if (is(obj, "ParquetDataFrame")) {
                sc <- obj@query$selected_columns[names(obj)]
                mc <- mcols(obj, use.names = FALSE) %||% mc
                has_mcols <- has_mcols || (ncol(mc) > 0L)
                md <- metadata(obj)
            }

            all_selected_columns[[i]] <- sc
            all_mcols[[i]] <- mc
            all_metadata[[i]] <- md
        }

        all_selected_columns <- c(xquery$selected_columns[names(xkey)],
                                  all_selected_columns)
        cols <- do.call(c, all_selected_columns)
        names(cols) <- make.unique(names(cols), sep = "_")
        xquery$selected_columns <- cols

        if (has_mcols) {
            all_mcols <- do.call(combineRows, all_mcols)
            rownames(all_mcols) <- setdiff(names(cols), names(xkey))
        } else {
            all_mcols <- NULL
        }

        new("ParquetDataFrame", query = xquery, key = xkey,
            elementMetadata = all_mcols, metadata = do.call(c, all_metadata))
    }
}

#' @export
#' @importFrom S4Vectors bindCOLS
setMethod("cbind", "ParquetDataFrame", cbind.ParquetDataFrame)

#' @export
#' @importFrom BiocGenerics as.data.frame
setMethod("as.data.frame", "ParquetDataFrame", function(x, row.names = NULL, optional = FALSE, ...) {
    df <- .executeQuery(x@query, key = x@key)
    rownames(df) <- df[[names(x@key)]]
    df <- df[x@key[[1L]], names(x), drop = FALSE]
    rownames(df) <- names(x@key[[1L]])[match(rownames(df), x@key[[1L]])]
    df
})

#' @importFrom S4Vectors make_zero_col_DFrame mcols mcols<- metadata metadata<-
.collapse_to_dframe <- function(x) {
    df <- make_zero_col_DFrame(nrow(x))
    for (j in names(x)) {
        df[[j]] <- x[[j]]
    }
    rownames(df) <- rownames(x)
    mcols(df) <- mcols(x, use.names = FALSE)
    metadata(df) <- metadata(x)
    df
}

#' @export
#' @importClassesFrom S4Vectors DFrame
setAs("ParquetDataFrame", "DFrame", function(from) .collapse_to_dframe(from))
