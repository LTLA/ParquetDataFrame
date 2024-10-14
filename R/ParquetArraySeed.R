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
#' @param dimensions Either a character vector or a named list of character
#' vectors containing the names of the columns in the Parquet data that specify
#' the dimensions of the array.
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
#' pqaseed <- ParquetArraySeed(tf, dimensions = c("Class", "Sex", "Age", "Survived"), value = "fate")
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
    slots = c(query = "arrow_dplyr_query", dimensions = "CharacterList", value = "character", type = "character")
)

#' @importFrom S4Vectors isSingleString setValidity2
setValidity2("ParquetArraySeed", function(x) {
    if (is.null(names(x@dimensions))) {
        return("'dimensions' slot must be a named list of character vectors")
    }
    if (!isSingleString(x@value)) {
        return("'value' slot must be a string")
    }
    if (!isSingleString(x@type) || !(x@type %in% c("logical", "integer", "double", "character"))) {
        return("'type' slot must be one of 'logical', 'integer', 'double', or 'character'")
    }
    TRUE
})

#' @export
setMethod("arrow_query", "ParquetArraySeed", function(x) x@query)

#' @export
setMethod("dim", "ParquetArraySeed", function(x) unname(lengths(x@dimensions)))

#' @export
setMethod("dimnames", "ParquetArraySeed", function(x) unname(as.list(x@dimensions)))

#' @export
#' @importFrom DelayedArray type
setMethod("type", "ParquetArraySeed", function(x) x@type)

#' @export
#' @importFrom BiocGenerics aperm
setMethod("aperm", "ParquetArraySeed", function(a, perm, ...) {
    k <- length(a@dimensions)
    if ((length(perm) != k) || !setequal(perm, seq_len(k))) {
        stop("'perm' must be a permutation of 1:", k)
    }
    initialize(a, dimensions = a@dimensions[perm])
})

#' @export
#' @importFrom BiocGenerics t
setMethod("t", "ParquetArraySeed", function(x) {
    if (length(x@dimensions) != 2L) {
        stop("'t()' is only defined for 2-dimensional Parquet arrays")
    }
    aperm(x, perm = 2:1)
})

.subset_ParquetArraySeed <- function(x, Nindex) {
    ndim <- length(x@dimensions)
    nsubscript <- length(Nindex)
    if (nsubscript == 0L)
        return(x)  # no-op
    if (nsubscript != ndim) {
        stop("incorrect number of subscripts")
    }
    for (d in seq_len(ndim)) {
        indices <- Nindex[[d]]
        if (is.character(indices)) {
            x@dimensions[[d]] <- indices
        } else if (is.numeric(indices)) {
            dnames <- x@dimensions[[d]]
            if (any(indices > length(dnames))) {
                stop("subscript out of bounds")
            }
            x@dimensions[[d]] <- dnames[indices]
        } else if (!is.null(indices)) {
            stop("subscripts must be character, numeric, or NULL")
        }
    }
    x
}

#' @export
#' @importFrom S4Vectors isTRUEorFALSE
setMethod("[", "ParquetArraySeed", function(x, i, j, ..., drop = TRUE) {
    Nindex <- S4Arrays:::extract_Nindex_from_syscall(sys.call(), parent.frame())
    .subset_ParquetArraySeed(x, Nindex)
})

#' @export
#' @importFrom DelayedArray extract_array
#' @importFrom dplyr filter
setMethod("extract_array", "ParquetArraySeed", function(x, index) {
    query <- arrow_query(x)

    # Add dimname filters
    for (i in seq_along(index)) {
        if (is.null(index[[i]])) {
            index[[i]] <- x@dimensions[[i]]
        } else {
            index[[i]] <- x@dimensions[[i]][index[[i]]]
        }
        query <- filter(query, !!as.name(names(x@dimensions)[i]) %in% index[[i]])
    }

    # Add defualt value filter
    query <- switch(x@type,
                    logical = filter(query, !(!!as.name(x@value))),
                    integer = filter(query, !!as.name(x@value) != 0L),
                    double = filter(query, !!as.name(x@value) != 0),
                    character = filter(query, !!as.name(x@value) != ""))

    # Create output array
    fill <- switch(x@type, logical = FALSE, integer = 0L, double = 0, character = "")
    output <- array(fill, dim = lengths(index), dimnames = index)
    if (min(dim(output)) > 0L) {
        df <- as.data.frame(query)
        output[as.matrix(df[, names(x@dimensions)])] <- df[[x@value]]
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
ParquetArraySeed <- function(data, dimensions, value, type = NULL, ...) {
    if (inherits(data, "arrow_dplyr_query")) {
        dat <- data
    } else {
        dat <- acquireDataset(data, ...)
    }

    if (is.character(dimensions)) {
        dimensions <- sapply(dimensions, function(x) pull(distinct(select(dat, as.name(!!x))), as_vector = TRUE), simplify = FALSE)
    }

    if (is.list(dimensions)) {
        dimensions <- CharacterList(dimensions)
    }

    if (!is(dimensions, "CharacterList") || is.null(names(dimensions))) {
        stop("'dimensions' must be a character vector or a named list of character vectors")
    }

    cols <- c(lapply(names(dimensions), as.name), list(as.name(value)))
    query <- do.call(select, c(list(dat), cols))
    if (is.null(type)) {
        type <- .getColumnType(query)
    } else {
        cast <- switch(type, logical = "as.logical", integer = "as.integer", double = "as.double", character = "as.character",
                       stop("'type' must be one of 'logical', 'integer', 'double', or 'character'"))
        query <- do.call(mutate,c(list(query), setNames(list(call(cast, as.name(value))), value)))
    }
    new("ParquetArraySeed", query = query, dimensions = dimensions, value = value, type = type)
}
