#' ParquetFactTable objects
#'
#' @description
#' ParquetFactTable is a low-level helper class for representing a
#' pointer to a Parquet fact table.
#'
#' @param data Either a string containing the path to the Parquet data,
#' or an \code{arrow_dplyr_query} object.
#' @param key Either a character vector or a named list of character vectors
#' containing the names of the columns in the Parquet data that specify
#' the primary key of the array.
#' @param fact Either a character vector containing the names of the columns
#' in the Parquet data that specify the facts or a named character vector where
#' the names specify the column names and the values specify the column type;
#' one of \code{"logical"}, \code{"integer"}, \code{"double"}, or
#' \code{"character"}.
#' @param ... Further arguments to be passed to
#' \code{\link[arrow]{open_dataset}}.
#'
#' @author Patrick Aboyoun
#'
#' @examples
#' # Create a data.frame from the Titanic data
#' df <- do.call(expand.grid, c(dimnames(Titanic), stringsAsFactors = FALSE))
#' df$fate <- as.integer(Titanic[as.matrix(df)])
#'
#' # Write data to a parquet file
#' tf <- tempfile()
#' on.exit(unlink(tf))
#' arrow::write_parquet(df, tf)
#'
#' tbl <- ParquetFactTable(tf, key = c("Class", "Sex", "Age", "Survived"), fact = "fate")
#'
#' @aliases
#' ParquetFactTable-class
#' [,ParquetFactTable,ANY,ANY,ANY-method
#' all.equal.ParquetFactTable
#' as.data.frame,ParquetFactTable-method
#' bindCOLS,ParquetFactTable-method
#' colnames,ParquetFactTable-method
#' colnames<-,ParquetFactTable-method
#' ncol,ParquetFactTable-method
#' nrow,ParquetFactTable-method
#' rownames,ParquetFactTable-method
#'
#' @include arrow_query.R
#' @include acquireDataset.R
#' @include keynames.R
#'
#' @name ParquetFactTable
NULL

setOldClass("arrow_dplyr_query")

#' @export
#' @importClassesFrom BiocGenerics OutOfMemoryObject
#' @importClassesFrom IRanges CharacterList
#' @importClassesFrom S4Vectors RectangularData
setClass("ParquetFactTable", contains = c("RectangularData", "OutOfMemoryObject"),
    slots = c(query = "arrow_dplyr_query", key = "CharacterList", fact = "character"))

setValidity2("ParquetFactTable", function(x) {
    msg <- NULL
    if (is.null(names(x@key))) {
        msg <- c(msg, "'key' slot must be a named CharacterList")
    }
    if (!all(names(x@key) %in% names(x@query))) {
        msg <- c(msg, "all names in 'key' slot must match columns in 'query'")
    }
    if (is.null(names(x@fact))) {
        msg <- c(msg, "'fact' slot must be a named character vector")
    }
    if (!all(names(x@fact) %in% names(x@query))) {
        msg <- c(msg, "all names in 'fact' slot must match columns in 'query'")
    }
    if (length(intersect(names(x@key), names(x@fact)))) {
        msg <- c(msg, "names in 'key' and 'fact' slots must be unique")
    }
    if (is.null(msg)) {
        TRUE
    } else {
        msg
    }
})

#' @export
setMethod("nkey", "ParquetFactTable", function(x) length(x@key))

#' @export
setMethod("nrow", "ParquetFactTable", function(x) prod(lengths(x@key, use.names = FALSE)))

#' @export
setMethod("ncol", "ParquetFactTable", function(x) length(x@fact))

#' @export
setMethod("keynames", "ParquetFactTable", function(x, ...) names(x@key))

#' @export
#' @importFrom BiocGenerics rownames
setMethod("rownames", "ParquetFactTable", function(x, do.NULL = TRUE, prefix = "row") {
    do.call(paste, c(do.call(expand.grid, as.list(x@key)), list(sep = "|")))
})

#' @export
#' @importFrom BiocGenerics colnames
setMethod("colnames", "ParquetFactTable", function(x, do.NULL = TRUE, prefix = "col") names(x@fact))

#' @export
#' @importFrom BiocGenerics colnames<-
#' @importFrom dplyr rename
#' @importFrom stats setNames
setReplaceMethod("colnames", "ParquetFactTable", function(x, value) {
    query <- x@query
    fact <- x@fact
    orig <- names(fact)
    query <- rename(query, !!!setNames(orig, value))
    value <- setNames(value, orig)
    names(fact) <- value[names(fact)]
    initialize(x, query = query, fact = fact)
})

#' @export
setMethod("[", "ParquetFactTable", function(x, i, j, ..., drop = TRUE) {
    query <- x@query
    fact <- x@fact
    if (!missing(j)) {
        query$selected_columns <- c(query$selected_columns[names(x@key)], query$selected_columns[j])
        fact <- fact[j]
    }

    key <- x@key
    if (!missing(i)) {
        if (!is.list(i) || is.null(names(i))) {
            stop("'i' must be a named list")
        }
        for (k in intersect(names(key), names(i))) {
            sub <- i[[k]]
            if (is.atomic(sub)) {
                key[[k]] <- key[[k]][sub]
            } else {
                stop("invalid value for '", k, "' in 'i'")
            }
        }
    }

    initialize(x, query = query, key = key, fact = fact)
})

#' @export
#' @importFrom S4Vectors bindCOLS
setMethod("bindCOLS", "ParquetFactTable",
function(x, objects = list(), use.names = TRUE, ignore.mcols = FALSE, check = TRUE) {
    query <- x@query
    selected_columns <- query$selected_columns
    fact <- x@fact

    for (i in seq_along(objects)) {
        obj <- objects[[i]]
        if (!is(obj, "ParquetFactTable")) {
            stop("all objects must be of class 'ParquetFactTable'")
        }
        if (!isTRUE(all.equal(x, obj))) {
            stop("all objects must share a compatible 'ParquetFactTable' structure")
        }
        newname <- names(objects)[i]
        if (!is.null(newname) && nzchar(newname)) {
            if (ncol(obj) > 1L) {
                colnames(obj) <- paste(newname, colnames(obj), sep = "_")
            }
            colnames(obj) <- newname
        }
        selected_columns <- c(selected_columns, obj@query$selected_columns[colnames(obj)])
        fact <- c(fact, obj@fact)
    }
    names(selected_columns) <- make.unique(names(selected_columns), sep = "_")
    names(fact) <- make.unique(names(fact), sep = "_")
    query$selected_columns <- selected_columns
    initialize(x, query = query, fact = fact)
})

#' @exportS3Method base::all.equal
all.equal.ParquetFactTable <- function(target, current, check.fact = FALSE, ...) {
    if (!is(current, "ParquetFactTable")) {
        return("current is not a ParquetFactTable")
    }
    if (!identical(target@query$.data, current@query$.data)) {
        return("query data mismatch")
    }
    if (check.fact) {
        i <- setdiff(names(unclass(target@query)), ".data")
    } else {
        i <- setdiff(names(unclass(target@query)), c(".data", "selected_columns"))
    }
    target <- c(unclass(target@query)[i], list(key = as.list(target@key), fact = list(target@fact)[check.fact]))
    current <- c(unclass(current@query)[i], list(key = as.list(current@key), fact = list(current@fact)[check.fact]))
    callGeneric(target, current, ...)
}

#' @export
#' @importFrom BiocGenerics as.data.frame
#' @importFrom dplyr filter
setMethod("as.data.frame", "ParquetFactTable", function(x, row.names = NULL, optional = FALSE, ...) {
    query <- x@query
    key <- x@key
    fact <- x@fact

    for (i in names(key)) {
        query <- filter(query, as.character(!!as.name(i)) %in% key[[i]])
    }

    # Allow for 1 extra row to check for duplicate keys
    length <- prod(lengths(key, use.names = FALSE)) + 1L
    query <- head(query, n = length)

    df <- as.data.frame(query)[, c(names(key), names(fact))]
    if (anyDuplicated(df[, names(key)])) {
        stop("duplicate keys found in Parquet data")
    }

    df
})

#' @importFrom dplyr pull slice_head
.getColumnType <- function(column_query) {
    DelayedArray::type(pull(slice_head(column_query, n = 0L), as_vector = TRUE))
}

#' @export
#' @importFrom dplyr distinct everything mutate pull select
#' @rdname ParquetFactTable
ParquetFactTable <- function(data, key, fact, ...) {
    if (inherits(data, "arrow_dplyr_query")) {
        query <- data
    } else {
        query <- select(acquireDataset(data, ...), everything())
    }

    if (is.character(key)) {
        key <- sapply(key, function(x) pull(distinct(select(query, as.name(!!x))), as_vector = TRUE), simplify = FALSE)
    }
    if (is.list(key)) {
        key <- CharacterList(key)
    }
    if (!is(key, "CharacterList") || is.null(names(key))) {
        stop("'key' must be a character vector or a named list of character vectors")
    }

    if (!is.character(fact)) {
        stop("'fact' must be a character vector")
    }
    if (is.null(names(fact))) {
        fact <- setNames(rep.int(NA_character_, length(fact)), fact)
    }

    for (i in names(fact)) {
        if (is.na(fact[[i]])) {
            column_query <- select(query, !!as.name(i))
            fact[[i]] <- .getColumnType(column_query)
        } else {
            cast <- switch(fact[[i]], logical = "as.logical", integer = "as.integer", double = "as.double", character = "as.character",
                           stop("'type' must be one of 'logical', 'integer', 'double', or 'character'"))
            query <- do.call(mutate, c(list(query), setNames(list(call(cast, as.name(i))), i)))
        }
    }

    cols <- c(lapply(names(key), as.name), lapply(names(fact), as.name))
    query <- do.call(select, c(list(query), cols))

    new("ParquetFactTable", query = query, key = key, fact = fact)
}
