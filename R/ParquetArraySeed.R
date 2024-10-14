#' ParquetArraySeed objects
#'
#' @description
#' ParquetArraySeed is a low-level helper class for representing a
#' pointer to a Parquet dataset.
#'
#' Note that a ParquetArraySeed object is not intended to be used directly.
#' Most end users will typically create and manipulate a higher-level
#' \link{ParquetArray} object instead. See \code{?\link{ParquetArray}} for
#' more information.
#'
#' @param data Either a string containing the path to the Parquet data,
#' or an existing ParquetArraySeed object.
#' @param key Either a character vector or a named list of character vectors
#' containing the names of the columns in the Parquet data that specify
#' the primary key of the array.
#' @param value String containing the name of the column in the Parquet data
#' that specifies the value of the array.
#' @param type String specifying the type of the Parquet data values;
#' one of \code{"logical"}, \code{"integer"}, \code{"double"}, or
#' \code{"character"}. If \code{NULL}, this is determined by inspecting
#' the data.
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
#' pqaseed <- ParquetArraySeed(tf, key = c("Class", "Sex", "Age", "Survived"), value = "fate")
#'
#' @aliases
#' ParquetArraySeed-class
#' [,ParquetArraySeed,ANY,ANY,ANY-method
#' aperm,ParquetArraySeed-method
#' DelayedArray,ParquetArraySeed-method
#' dim,ParquetArraySeed-method
#' dimnames,ParquetArraySeed-method
#' extract_array,ParquetArraySeed-method
#' t,ParquetArraySeed-method
#' type,ParquetArraySeed-method
#'
#' @seealso
#' \code{\link{ParquetArray}},
#' \code{\link[S4Arrays]{Array}}
#'
#' @include arrow_query.R
#' @include acquireDataset.R
#'
#' @name ParquetArraySeed
NULL

#' @export
#' @importClassesFrom BiocGenerics OutOfMemoryObject
#' @importClassesFrom IRanges CharacterList
#' @importClassesFrom S4Arrays Array
setClass("ParquetArraySeed", contains = c("Array", "OutOfMemoryObject"),
    slots = c(query = "arrow_dplyr_query", key = "CharacterList", value = "character", type = "character", drop = "logical")
)

#' @importFrom S4Vectors isSingleString isTRUEorFALSE setValidity2
setValidity2("ParquetArraySeed", function(x) {
    if (is.null(names(x@key))) {
        return("'key' slot must be a named list of character vectors")
    }
    if (!isSingleString(x@value)) {
        return("'value' slot must be a string")
    }
    if (!isSingleString(x@type) || !(x@type %in% c("logical", "integer", "double", "character"))) {
        return("'type' slot must be one of 'logical', 'integer', 'double', or 'character'")
    }
    if (!isTRUEorFALSE(x@drop)) {
        return("'drop' slot must be TRUE or FALSE")
    }
    TRUE
})

#' @export
setMethod("arrow_query", "ParquetArraySeed", function(x) x@query)

#' @export
setMethod("dim", "ParquetArraySeed", function(x) {
    ans <- lengths(x@key, use.names = FALSE)
    if (x@drop) {
        keep <- ans != 1L
        if (!any(keep)) {
            ans <- 1L
        } else {
            ans <- ans[keep]
        }
    }
    ans
})

#' @export
setMethod("dimnames", "ParquetArraySeed", function(x) {
    ans <- as.list(x@key)
    if (x@drop) {
        keep <- lengths(ans, use.names = FALSE) != 1L
        if (!any(keep)) {
            ans <- NULL
        } else {
            ans <- ans[keep]
        }
    }
    ans
})

#' @export
#' @importFrom DelayedArray type
setMethod("type", "ParquetArraySeed", function(x) x@type)

#' @export
#' @importFrom BiocGenerics aperm
setMethod("aperm", "ParquetArraySeed", function(a, perm, ...) {
    k <- length(a@key)
    if ((length(perm) != k) || !setequal(perm, seq_len(k))) {
        stop("'perm' must be a permutation of 1:", k)
    }
    initialize(a, key = a@key[perm])
})

#' @export
#' @importFrom BiocGenerics t
setMethod("t", "ParquetArraySeed", function(x) {
    if (length(x@key) != 2L) {
        stop("'t()' is only defined for 2-dimensional Parquet arrays")
    }
    aperm(x, perm = 2:1)
})

.subset_ParquetArraySeed <- function(x, Nindex, drop) {
    key <- x@key
    ndim <- length(key)
    nsubscript <- length(Nindex)
    if (nsubscript == 0L)
        return(x)  # no-op
    if (nsubscript != ndim) {
        stop("incorrect number of subscripts")
    }
    for (d in seq_len(ndim)) {
        indices <- Nindex[[d]]
        if (is.character(indices)) {
            key[[d]] <- indices
        } else if (is.numeric(indices)) {
            dnames <- key[[d]]
            if (any(indices > length(dnames))) {
                stop("subscript out of bounds")
            }
            key[[d]] <- dnames[indices]
        } else if (!is.null(indices)) {
            stop("subscripts must be character, numeric, or NULL")
        }
    }
    initialize(x, key = key, drop = drop)
}

#' @export
#' @importFrom S4Vectors isTRUEorFALSE
setMethod("[", "ParquetArraySeed", function(x, i, j, ..., drop = TRUE) {
    Nindex <- S4Arrays:::extract_Nindex_from_syscall(sys.call(), parent.frame())
    .subset_ParquetArraySeed(x, Nindex = Nindex, drop = drop)
})

#' @export
#' @importFrom DelayedArray extract_array
#' @importFrom dplyr filter
setMethod("extract_array", "ParquetArraySeed", function(x, index) {
    # Process index argument
    if (!is.list(index)) {
        stop("'index' must be a list")
    } else if (all(vapply(index, is.null, logical(1L)))) {
        index <- as.list(x@key)
    } else if (length(index) == length(x@key)) {
        names(index) <- names(x@key)
    }

    # Initialize output array
    fill <- switch(x@type, logical = FALSE, integer = 0L, double = 0, character = "")
    output <- array(fill, dim = lengths(index, use.names = FALSE))
    if (min(dim(output)) == 0L) {
        return(output)
    }

    # Extract query
    query <- arrow_query(x)

    # Add key filters
    for (i in seq_along(index)) {
        idx <- index[[i]]
        if (is.null(idx)) {
            index[[i]] <- x@key[[i]]
        } else if (is.numeric(idx)) {
            index[[i]] <- x@key[[i]][idx]
        }
        query <- filter(query, !!as.name(names(x@key)[i]) %in% index[[i]])
    }

    # Add defualt value filter
    query <- switch(x@type,
                    logical = filter(query, !(!!as.name(x@value))),
                    integer = filter(query, !!as.name(x@value) != 0L),
                    double = filter(query, !!as.name(x@value) != 0),
                    character = filter(query, !!as.name(x@value) != ""))

    # Execute query
    df <- as.data.frame(query)
    key <- df[, names(x@key)]
    if (anyDuplicated(key)) {
        stop("duplicate keys found in Parquet data")
    }

    # Fill output array
    dimnames(output) <- index
    output[as.matrix(key)] <- df[[x@value]]
    if (x@drop) {
        output <- drop(output)
    }

    output
})

#' @export
#' @importFrom DelayedArray DelayedArray
setMethod("DelayedArray", "ParquetArraySeed", function(seed) ParquetArray(seed))

#' @export
#' @importFrom dplyr distinct mutate pull select
#' @importFrom IRanges CharacterList
#' @importClassesFrom IRanges CharacterList
#' @importFrom stats setNames
#' @rdname ParquetArraySeed
ParquetArraySeed <- function(data, key, value, type = NULL, ...) {
    if (inherits(data, "arrow_dplyr_query")) {
        dat <- data
    } else {
        dat <- acquireDataset(data, ...)
    }

    if (is.character(key)) {
        key <- sapply(key, function(x) pull(distinct(select(dat, as.name(!!x))), as_vector = TRUE), simplify = FALSE)
    }

    if (is.list(key)) {
        key <- CharacterList(key)
    }

    if (!is(key, "CharacterList") || is.null(names(key))) {
        stop("'key' must be a character vector or a named list of character vectors")
    }

    cols <- c(lapply(names(key), as.name), list(as.name(value)))
    query <- do.call(select, c(list(dat), cols))
    if (is.null(type)) {
        type <- .getColumnType(query)
    } else {
        cast <- switch(type, logical = "as.logical", integer = "as.integer", double = "as.double", character = "as.character",
                       stop("'type' must be one of 'logical', 'integer', 'double', or 'character'"))
        query <- do.call(mutate,c(list(query), setNames(list(call(cast, as.name(value))), value)))
    }
    new("ParquetArraySeed", query = query, key = key, value = value, type = type, drop = FALSE)
}
