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
#' or an \code{arrow_dplyr_query} object.
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
#' Arith,ParquetArraySeed,ParquetArraySeed-method
#' Arith,ParquetArraySeed,numeric-method
#' Arith,numeric,ParquetArraySeed-method
#' Compare,ParquetArraySeed,ParquetArraySeed-method
#' Compare,ParquetArraySeed,vector-method
#' Compare,vector,ParquetArraySeed-method
#' Logic,ParquetArraySeed,ParquetArraySeed-method
#' Math,ParquetArraySeed-method
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
#' @import methods
#' @importClassesFrom BiocGenerics OutOfMemoryObject
#' @importClassesFrom IRanges CharacterList
#' @importClassesFrom S4Arrays Array
setClass("ParquetArraySeed", contains = c("Array", "OutOfMemoryObject"),
    slots = c(query = "arrow_dplyr_query", key = "CharacterList", value = "character", type = "character", drop = "logical")
)

#' @importFrom S4Vectors isSingleString isTRUEorFALSE setValidity2
setValidity2("ParquetArraySeed", function(x) {
    if (is.null(names(x@key)) || !all(names(x@key) %in% names(x@query))) {
        return("'key' slot must be a list of character vectors with names that match the query")
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

.compatibleSeeds <- function(x, y) {
    body <-  c(".data", "filtered_rows", "group_by_vars", "drop_empty_groups", "arrange_vars", "arrange_desc")
    x_query <- x@query
    y_query <- y@query

    identical(x@key, y@key) &&
    identical(x@drop, y@drop) &&
    inherits(x_query, "arrow_dplyr_query") &&
    inherits(y_query, "arrow_dplyr_query") &&
    identical(unclass(x_query)[body], unclass(y_query)[body])
}

#' @importFrom dplyr mutate select
.Arith.ParquetArraySeed <- function(.Generic, query, key, drop, v1, v2) {
    query <- switch(.Generic,
                    "+" = mutate(query, .x. = `+`(!!v1, !!v2)),
                    "-" = mutate(query, .x. = `-`(!!v1, !!v2)),
                    "*" = mutate(query, .x. = `*`(!!v1, !!v2)),
                    "/" = mutate(query, .x. = `/`(!!v1, !!v2)),
                    "^" = mutate(query, .x. = `^`(!!v1, !!v2)),
                    "%%" = mutate(query, .x. = `%%`(!!v1, !!v2)),
                    "%/%" = mutate(query, .x. = `%/%`(!!v1, !!v2)))
    query <- select(query, c(names(key), ".x."))
    type <- .getColumnType(query)
    new("ParquetArraySeed", query = query, key = key, value = ".x.", type = type, drop = drop)
}

#' @export
setMethod("Arith", c(e1 = "ParquetArraySeed", e2 = "ParquetArraySeed"), function(e1, e2) {
    if (!.compatibleSeeds(e1, e2)) {
        stop("can only perform arithmetic operations with compatible objects")
    }
    query <- e1@query
    query$selected_columns <- c(query$selected_columns, e2@query$selected_columns[e2@value])
    names(query$selected_columns) <- make.unique(names(query$selected_columns), sep = "_")
    v1 <- as.name(e1@value)
    v2 <- as.name(names(query$selected_columns)[length(query$selected_columns)])
    .Arith.ParquetArraySeed(.Generic, query = query, key = e1@key, drop = e1@drop, v1 = v1, v2 = v2)
})

#' @export
setMethod("Arith", c(e1 = "ParquetArraySeed", e2 = "numeric"), function(e1, e2) {
    if (length(e2) != 1L) {
        stop("can only perform arithmetic operations with a scalar value")
    }
    query <- e1@query
    v1 <- as.name(e1@value)
    .Arith.ParquetArraySeed(.Generic, query = query, key = e1@key, drop = e1@drop, v1 = v1, v2 = e2)
})

#' @export
setMethod("Arith", c(e1 = "numeric", e2 = "ParquetArraySeed"), function(e1, e2) {
    if (length(e1) != 1L) {
        stop("can only perform arithmetic operations with a scalar value")
    }
    query <- e2@query
    v2 <- as.name(e2@value)
    .Arith.ParquetArraySeed(.Generic, query = query, key = e2@key, drop = e2@drop, v1 = e1, v2 = v2)
})

#' @importFrom dplyr mutate select
.Compare.ParquetArraySeed <- function(.Generic, query, key, drop, v1, v2) {
    query <- switch(.Generic,
                    "==" = mutate(query, .x. = `==`(!!v1, !!v2)),
                    ">" = mutate(query, .x. = `>`(!!v1, !!v2)),
                    "<" = mutate(query, .x. = `<`(!!v1, !!v2)),
                    "!=" = mutate(query, .x. = `!=`(!!v1, !!v2)),
                    "<=" = mutate(query, .x. = `<=`(!!v1, !!v2)),
                    ">=" = mutate(query, .x. = `>=`(!!v1, !!v2)))
    query <- select(query, c(names(key), ".x."))
    new("ParquetArraySeed", query = query, key = key, value = ".x.", type = "logical", drop = drop)
}

#' @export
setMethod("Compare", c(e1 = "ParquetArraySeed", e2 = "ParquetArraySeed"), function(e1, e2) {
    if (!.compatibleSeeds(e1, e2)) {
        stop("can only perform comparison operations with compatible objects")
    }
    query <- e1@query
    query$selected_columns <- c(query$selected_columns, e2@query$selected_columns[e2@value])
    names(query$selected_columns) <- make.unique(names(query$selected_columns), sep = "_")
    v1 <- as.name(e1@value)
    v2 <- as.name(names(query$selected_columns)[length(query$selected_columns)])
    .Compare.ParquetArraySeed(.Generic, query = query, key = e1@key, drop = e1@drop, v1 = v1, v2 = v2)
})

#' @export
setMethod("Compare", c(e1 = "ParquetArraySeed", e2 = "vector"), function(e1, e2) {
    if (length(e2) != 1L) {
        stop("can only perform comparison operations with a scalar value")
    }
    query <- e1@query
    v1 <- as.name(e1@value)
    .Compare.ParquetArraySeed(.Generic, query = query, key = e1@key, drop = e1@drop, v1 = v1, v2 = e2)
})

#' @export
setMethod("Compare", c(e1 = "vector", e2 = "ParquetArraySeed"), function(e1, e2) {
    if (length(e1) != 1L) {
        stop("can only perform comparison operations with a scalar value")
    }
    query <- e2@query
    v2 <- as.name(e2@value)
    .Compare.ParquetArraySeed(.Generic, query = query, key = e2@key, drop = e2@drop, v1 = e1, v2 = v2)
})

#' @importFrom dplyr mutate select
.Logic.ParquetArraySeed <- function(.Generic, query, key, drop, v1, v2) {
    query <- switch(.Generic,
                    "&" = mutate(query, .x. = `&`(!!v1, !!v2)),
                    "|" = mutate(query, .x. = `|`(!!v1, !!v2)))
    query <- select(query, c(names(key), ".x."))
    new("ParquetArraySeed", query = query, key = key, value = ".x.", type = "logical", drop = drop)
}

#' @export
setMethod("Logic", c(e1 = "ParquetArraySeed", e2 = "ParquetArraySeed"), function(e1, e2) {
    if (!.compatibleSeeds(e1, e2)) {
        stop("can only perform logical operations with compatible objects")
    }
    query <- e1@query
    query$selected_columns <- c(query$selected_columns, e2@query$selected_columns[e2@value])
    names(query$selected_columns) <- make.unique(names(query$selected_columns), sep = "_")
    v1 <- as.name(e1@value)
    v2 <- as.name(names(query$selected_columns)[length(query$selected_columns)])
    .Logic.ParquetArraySeed(.Generic, query = query, key = e1@key, drop = e1@drop, v1 = v1, v2 = v2)
})

#' @export
#' @importFrom dplyr mutate select
setMethod("Math", "ParquetArraySeed", function(x) {
    query <- x@query
    name <- as.name(x@value)
    query <-
      switch(.Generic,
             abs = mutate(query, .x. = abs(!!name)),
             sign = mutate(query, .x. = sign(!!name)),
             sqrt = mutate(query, .x. = sqrt(!!name)),
             ceiling = mutate(query, .x. = ceiling(!!name)),
             floor = mutate(query, .x. = floor(!!name)),
             trunc = mutate(query, .x. = trunc(!!name)),
             log = mutate(query, .x. = log(!!name)),
             log10 = mutate(query, .x. = log10(!!name)),
             log2 = mutate(query, .x. = log2(!!name)),
             log1p = mutate(query, .x. = log1p(!!name)),
             acos = mutate(query, .x. = acos(!!name)),
             asin = mutate(query, .x. = asin(!!name)),
             exp = mutate(query, .x. = exp(!!name)),
             cos = mutate(query, .x. = cos(!!name)),
             sin = mutate(query, .x. = sin(!!name)),
             tan = mutate(query, .x. = tan(!!name)),
             stop("unsupported Math operator: ", .Generic))
    query <- select(query, c(names(x@key), ".x."))
    type <- .getColumnType(query)
    initialize(x, query = query, value = ".x.", type = type)
})

#' @export
#' @importFrom DelayedArray extract_array
setMethod("extract_array", "ParquetArraySeed", function(x, index) {
    # Process index argument
    if (!is.list(index)) {
        stop("'index' must be a list")
    } else if (all(vapply(index, is.null, logical(1L)))) {
        index <- as.list(x@key)
    } else {
        # Add names to index list
        if (length(index) == length(x@key)) {
            names(index) <- names(x@key)
        } else if (length(index) == length(dimnames(x))) {
            names(index) <- names(dimnames(x))
        }

        if (!is.null(names(index))) {
            # Replace NULL and integer values with strings
            for (i in names(index)) {
                idx <- index[[i]]
                if (is.null(idx)) {
                    index[[i]] <- x@key[[i]]
                } else {
                    index[[i]] <- x@key[[i]][idx]
                }
            }

            # Add dropped dimensions if data are present
            if ((length(index) < length(x@key)) && all(lengths(index, use.names = FALSE) > 0L)) {
                key <- as.list(x@key)
                key[names(index)] <- index
                index <- key
            }
        }
    }

    # Initialize output array
    fill <- switch(x@type, logical = FALSE, integer = 0L, double = 0, character = "")
    output <- array(fill, dim = lengths(index, use.names = FALSE))
    if (min(dim(output)) == 0L) {
        return(output)
    }
    dimnames(output) <- index

    # Fill output array
    df <- .executeQuery(x@query, key = index)
    keycols <- df[, names(x@key)]
    output[as.matrix(keycols)] <- df[[x@value]]
    if (x@drop) {
        output <- as.array(drop(output))
    }

    output
})

#' @export
#' @importFrom DelayedArray DelayedArray
setMethod("DelayedArray", "ParquetArraySeed", function(seed) ParquetArray(seed))

#' @export
#' @importFrom dplyr distinct everything mutate pull select
#' @importFrom IRanges CharacterList
#' @importClassesFrom IRanges CharacterList
#' @importFrom stats setNames
#' @rdname ParquetArraySeed
ParquetArraySeed <- function(data, key, value, type = NULL, ...) {
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

    cols <- c(lapply(names(key), as.name), list(as.name(value)))
    query <- do.call(select, c(list(query), cols))
    if (is.null(type)) {
        type <- .getColumnType(query)
    } else {
        cast <- switch(type, logical = "as.logical", integer = "as.integer", double = "as.double", character = "as.character",
                       stop("'type' must be one of 'logical', 'integer', 'double', or 'character'"))
        query <- do.call(mutate,c(list(query), setNames(list(call(cast, as.name(value))), value)))
    }
    new("ParquetArraySeed", query = query, key = key, value = value, type = type, drop = FALSE)
}
