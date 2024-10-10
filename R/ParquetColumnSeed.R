#' Column of a Parquet file
#'
#' Represent a column of a Parquet file as a 1-dimensional \linkS4class{DelayedArray}.
#' This allows us to use Parquet data inside \linkS4class{DataFrame}s without loading them into memory.
#'
#' @param path String specifying a path to a Parquet data directory or file.
#' @param column String containing the name of the column in the data.
#' @param length Integer containing the number of rows.
#' If \code{NULL}, this is determined by inspecting the data.
#' This should only be supplied for efficiency purposes, to avoid a file look-up on construction.
#' @param type String specifying the type of the data.
#' If \code{NULL}, this is determined by inspecting the data.
#' Users may specify this to avoid a look-up, or to coerce the output into a different type.
#' @param x Either a string containing the path to the Parquet data (to be used as \code{path}),
#' or an existing ParquetColumnSeed object.
#' @param ... Further arguments to be passed to \code{\link[arrow]{open_dataset}}.
#'
#' @return For \code{ParquetColumnSeed}, a ParquetColumnSeed is returned, obviously.
#'
#' For \code{ParquetColumnVector}, a ParquetColumnVector is returned.
#'
#' @author Aaron Lun, Patrick Aboyoun
#'
#' @examples
#' # Mocking up a file:
#' tf <- tempfile()
#' on.exit(unlink(tf))
#' arrow::write_parquet(mtcars, tf)
#'
#' # Creating a vector:
#' ParquetColumnVector(tf, column="gear")
#'
#' # This happily lives inside DataFrames:
#' collected <- list()
#' for (x in colnames(mtcars)) {
#'     collected[[x]] <- ParquetColumnVector(tf, column=x)
#' }
#' DataFrame(collected)
#'
#' @aliases
#' ParquetColumnSeed-class
#' dim,ParquetColumnSeed-method
#' Arith,ParquetColumnSeed,ParquetColumnSeed-method
#' Arith,ParquetColumnSeed,numeric-method
#' Arith,numeric,ParquetColumnSeed-method
#' Compare,ParquetColumnSeed,ParquetColumnSeed-method
#' Compare,ParquetColumnSeed,vector-method
#' Compare,vector,ParquetColumnSeed-method
#' Logic,ParquetColumnSeed,ParquetColumnSeed-method
#' Math,ParquetColumnSeed-method
#' type,ParquetColumnSeed-method
#' DelayedArray,ParquetColumnSeed-method
#' extract_array,ParquetColumnSeed-method
#' ParquetColumnVector-class
#' Ops,ParquetColumnVector,ParquetColumnVector-method
#' Ops,ParquetColumnVector,numeric-method
#' Ops,numeric,ParquetColumnVector-method
#' Math,ParquetColumnVector-method
#'
#' @include query.R
#' @include acquireDataset.R
#'
#' @name ParquetColumnSeed
NULL

#' @export
#' @import methods
setClass("ParquetColumnSeed", slots = c(query = "arrow_dplyr_query", length = "integer", type = "character"))

#' @export
setMethod("query", "ParquetColumnSeed", function(x) x@query)

#' @export
setMethod("dim", "ParquetColumnSeed", function(x) x@length)

#' @importFrom dplyr mutate
.Arith.ParquetColumnSeed <- function(.Generic, query, v1, v2, length) {
    query <- switch(.Generic,
                    "+" = mutate(query, x = `+`(!!v1, !!v2)),
                    "-" = mutate(query, x = `-`(!!v1, !!v2)),
                    "*" = mutate(query, x = `*`(!!v1, !!v2)),
                    "/" = mutate(query, x = `/`(!!v1, !!v2)),
                    "^" = mutate(query, x = `^`(!!v1, !!v2)),
                    "%%" = mutate(query, x = `%%`(!!v1, !!v2)),
                    "%/%" = mutate(query, x = `%/%`(!!v1, !!v2)))
    query <- select(query, "x")
    type <- DelayedArray::type(pull(slice_head(query, n = 0L), as_vector = TRUE))
    new("ParquetColumnSeed", query = query, type = type, length = length)
}

#' @export
setMethod("Arith", c(e1 = "ParquetColumnSeed", e2 = "ParquetColumnSeed"), function(e1, e2) {
    df <- cbind.ParquetDataFrame(x = ParquetColumnVector(e1), y = ParquetColumnVector(e2))
    query <- query(df)
    .Arith.ParquetColumnSeed(.Generic, query = query, v1 = as.name("x"), v2 = as.name("y"),
                             length = e1@length)
})

#' @export
setMethod("Arith", c(e1 = "ParquetColumnSeed", e2 = "numeric"), function(e1, e2) {
    if (length(e2) != 1) {
        stop("can only perform arithmetic operations with a scalar value")
    }
    query <- query(e1)
    name <- as.name(names(query))
    .Arith.ParquetColumnSeed(.Generic, query = query, v1 = name, v2 = e2, length = e1@length)
})

#' @export
setMethod("Arith", c(e1 = "numeric", e2 = "ParquetColumnSeed"), function(e1, e2) {
    if (length(e1) != 1) {
        stop("can only perform arithmetic operations with a scalar value")
    }
    query <- query(e2)
    name <- as.name(names(query))
    .Arith.ParquetColumnSeed(.Generic, query = query, v1 = e1, v2 = name, length = e2@length)
})

#' @importFrom dplyr mutate
.Compare.ParquetColumnSeed <- function(.Generic, query, v1, v2, length) {
    query <- switch(.Generic,
                    "==" = mutate(query, x = `==`(!!v1, !!v2)),
                    ">" = mutate(query, x = `>`(!!v1, !!v2)),
                    "<" = mutate(query, x = `<`(!!v1, !!v2)),
                    "!=" = mutate(query, x = `!=`(!!v1, !!v2)),
                    "<=" = mutate(query, x = `<=`(!!v1, !!v2)),
                    ">=" = mutate(query, x = `>=`(!!v1, !!v2)))
    query <- select(query, "x")
    new("ParquetColumnSeed", query = query, type = "logical", length = length)
}

#' @export
setMethod("Compare", c(e1 = "ParquetColumnSeed", e2 = "ParquetColumnSeed"), function(e1, e2) {
    df <- cbind.ParquetDataFrame(x = ParquetColumnVector(e1), y = ParquetColumnVector(e2))
    query <- query(df)
    .Compare.ParquetColumnSeed(.Generic, query = query, v1 = as.name("x"), v2 = as.name("y"),
                               length = e1@length)
})

#' @export
setMethod("Compare", c(e1 = "ParquetColumnSeed", e2 = "vector"), function(e1, e2) {
    if (length(e2) != 1) {
        stop("can only perform comparison operations with a scalar value")
    }
    query <- query(e1)
    name <- as.name(names(query))
    .Compare.ParquetColumnSeed(.Generic, query = query, v1 = name, v2 = e2, length = e1@length)
})

#' @export
setMethod("Compare", c(e1 = "vector", e2 = "ParquetColumnSeed"), function(e1, e2) {
    if (length(e1) != 1) {
        stop("can only perform comparison operations with a scalar value")
    }
    query <- query(e2)
    name <- as.name(names(query))
    .Compare.ParquetColumnSeed(.Generic, query = query, v1 = e1, v2 = name, length = e2@length)
})

#' @importFrom dplyr mutate
.Logic.ParquetColumnSeed <- function(.Generic, query, v1, v2, length) {
    query <- switch(.Generic,
                    "&" = mutate(query, x = `&`(!!v1, !!v2)),
                    "|" = mutate(query, x = `|`(!!v1, !!v2)))
    query <- select(query, "x")
    new("ParquetColumnSeed", query = query, type = "logical", length = length)
}

#' @export
setMethod("Logic", c(e1 = "ParquetColumnSeed", e2 = "ParquetColumnSeed"), function(e1, e2) {
    df <- cbind.ParquetDataFrame(x = ParquetColumnVector(e1), y = ParquetColumnVector(e2))
    query <- query(df)
    .Logic.ParquetColumnSeed(.Generic, query = query, v1 = as.name("x"), v2 = as.name("y"),
                             length = e1@length)
})

#' @export
#' @importFrom dplyr mutate
setMethod("Math", "ParquetColumnSeed", function(x) {
    query <- query(x)
    name <- as.name(names(query))
    query <-
      switch(.Generic,
             abs = mutate(query, x = abs(!!name)),
             sign = mutate(query, x = sign(!!name)),
             sqrt = mutate(query, x = sqrt(!!name)),
             ceiling = mutate(query, x = ceiling(!!name)),
             floor = mutate(query, x = floor(!!name)),
             trunc = mutate(query, x = trunc(!!name)),
             log = mutate(query, x = log(!!name)),
             log10 = mutate(query, x = log10(!!name)),
             log2 = mutate(query, x = log2(!!name)),
             log1p = mutate(query, x = log1p(!!name)),
             acos = mutate(query, x = acos(!!name)),
             asin = mutate(query, x = asin(!!name)),
             exp = mutate(query, x = exp(!!name)),
             cos = mutate(query, x = cos(!!name)),
             sin = mutate(query, x = sin(!!name)),
             tan = mutate(query, x = tan(!!name)),
             stop("unsupported Math operator: ", .Generic))
    query <- select(query, x)
    type <- DelayedArray::type(pull(slice_head(query, n = 0L), as_vector = TRUE))
    initialize(x, query = query, type = type)
})

#' @export
#' @importFrom DelayedArray type
setMethod("type", "ParquetColumnSeed", function(x) x@type)

#' @export
#' @importFrom DelayedArray DelayedArray
setMethod("DelayedArray", "ParquetColumnSeed", function(seed) ParquetColumnVector(seed))

#' @export
#' @importFrom DelayedArray extract_array
#' @importFrom dplyr pull
setMethod("extract_array", "ParquetColumnSeed", function(x, index) {
    slice <- index[[1]]
    if (is.null(slice)) {
        output <- pull(query(x), as_vector = TRUE)
    } else if (length(slice) == 0) {
        output <- logical()
    } else {
        original <- slice
        modified <- FALSE

        if (anyDuplicated(slice)) {
            slice <- unique(slice)
            modified <- TRUE
        }

        if (is.unsorted(slice)) {
            slice <- sort(slice)
            modified <- TRUE
        }

        output <- pull(query(x), as_vector = FALSE)$Take(slice - 1L)$as_vector()
        if (modified) {
            m <- match(original, slice)
            output <- output[m]
        }
    }

    if (!is(output, x@type)) {
        output <- as(output, x@type)
    }
    array(output)
})

#' @export
#' @rdname ParquetColumnSeed
#' @importFrom DelayedArray type
#' @importFrom dplyr select slice_head
ParquetColumnSeed <- function(path, column, type = NULL, length = NULL, ...) {
    if (inherits(path, "arrow_dplyr_query")) {
        dat <- path
    } else {
        dat <- acquireDataset(path, ...)
    }
    query <- select(dat, !!column)
    if (is.null(type)){
        type <- DelayedArray::type(pull(slice_head(query, n = 0L), as_vector = TRUE))
    }
    if (is.null(length)) {
        length <- nrow(dat)
    } 
    new("ParquetColumnSeed", query = query, length = length, type = type)
}

#' @export
setClass("ParquetColumnVector", contains = "DelayedArray", slots = c(seed = "ParquetColumnSeed"))

#' @export
#' @importFrom DelayedArray seed
setMethod("query", "ParquetColumnVector", function(x) query(seed(x)))

#' @export
setMethod("Ops", c(e1 = "ParquetColumnVector", e2 = "ParquetColumnVector"), function(e1, e2) {
    seed <- try(callGeneric(seed(e1), seed(e2)), silent = TRUE)
    if (inherits(seed, "try-error")) {
        callNextMethod()
    } else {
        new("ParquetColumnVector", seed = seed)
    }
})

#' @export
setMethod("Ops", c(e1 = "ParquetColumnVector", e2 = "numeric"), function(e1, e2) {
    seed <- try(callGeneric(seed(e1), e2), silent = TRUE)
    if (inherits(seed, "try-error")) {
        callNextMethod()
    } else {
        new("ParquetColumnVector", seed = seed)
    }
})

#' @export
setMethod("Ops", c(e1 = "numeric", e2 = "ParquetColumnVector"), function(e1, e2) {
    seed <- try(callGeneric(e1, seed(e2)), silent = TRUE)
    if (inherits(seed, "try-error")) {
        callNextMethod()
    } else {
        new("ParquetColumnVector", seed = seed)
    }
})

#' @export
setMethod("Math", "ParquetColumnVector", function(x) initialize(x, seed = callGeneric(seed(x))))

#' @export
#' @rdname ParquetColumnSeed
ParquetColumnVector <- function(x, column, type = NULL, length = NULL, ...) {
    if (!is(x, "ParquetColumnSeed")) {
        x <- ParquetColumnSeed(x, column = column, type = type, length = length, ...)
    }
    new("ParquetColumnVector", seed = x)
}
