#' Parquet datasets as DelayedMatrix objects
#'
#' @description
#' The ParquetMatrix class is a \link[DelayedArray]{DelayedMatrix} subclass
#' for representing and operating on a Parquet dataset.
#'
#' All the operations available for \link[DelayedArray]{DelayedMatrix}
#' objects work on ParquetMatrix objects.
#'
#' @param data Either a string containing the path to the Parquet data,
#' or an existing ParquetArraySeed object.
#' @param row Either a character vector or a named list of character vectors
#' containing the names of the columns in the Parquet data that specify the
#' rows of the matrix.
#' @param col Either a character vector or a named list of character vectors
#' containing the names of the columns in the Parquet data that specify the
#' columns of the matrix.
#' @param key Either a character vector or a named list of character vectors
#' containing the names of the columns in the Parquet data that specify the
#' rows and columns of the matrix.
#' @param value String containing the name of the column in the Parquet data
#' that specifies the value of the matrix.
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
#' # Create a data.frame from a matrix
#' df <- data.frame(
#'   rowname = rep(rownames(state.x77), times = ncol(state.x77)),
#'   colname = rep(colnames(state.x77), each = nrow(state.x77)),
#'   value = as.vector(state.x77)
#' )
#'
#' # Write data to a parquet file
#' tf <- tempfile()
#' on.exit(unlink(tf))
#' arrow::write_parquet(df, tf)
#'
#' pqmat <- ParquetMatrix(tf, row = "rowname", col = "colname", value = "value")
#'
#' @aliases
#' ParquetMatrix-class
#' [,ParquetMatrix,ANY,ANY,ANY-method
#'
#' @include ParquetArraySeed.R
#' @include ParquetArray.R
#'
#' @name ParquetMatrix
NULL

#' @export
#' @importClassesFrom DelayedArray DelayedMatrix
setClass("ParquetMatrix", contains = c("ParquetArray", "DelayedMatrix"))

setValidity2("ParquetMatrix", function(x) {
    if (length(seed(x)@key) != 2L) {
        return("'key' seed slot must be a two element named list of character vectors")
    }
    TRUE
})

#' @export
setMethod("[", "ParquetMatrix", function(x, i, j, ..., drop = TRUE) {
    Nindex <- S4Arrays:::extract_Nindex_from_syscall(sys.call(), parent.frame())
    seed <- .subset_ParquetArraySeed(seed(x), Nindex = Nindex, drop = drop)
    if (length(dim(seed)) == 1L) {
        ParquetArray(seed)
    } else {
        initialize(x, seed = seed)
    }
})

setMethod("[", "ParquetArray", function(x, i, j, ..., drop = TRUE) {
    Nindex <- S4Arrays:::extract_Nindex_from_syscall(sys.call(), parent.frame())
    initialize(x, seed = .subset_ParquetArraySeed(seed(x), Nindex = Nindex, drop = drop))
})

#' @export
#' @importFrom S4Vectors isSingleString
#' @rdname ParquetMatrix
ParquetMatrix <- function(data, row, col, value, key = c(row, col), type = NULL, ...) {
    if (!is(data, "ParquetArraySeed")) {
        if (length(key) != 2L) {
            stop("'key' must contain exactly 2 elements: rows and columns")
        }
        data <- ParquetArraySeed(data, key = key, value = value, type = type, ...)
    }
    new("ParquetMatrix", seed = data)
}
