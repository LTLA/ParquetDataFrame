#' Column of a Parquet file
#'
#' Represent a column of a Parquet file as a 1-dimensional \linkS4class{DelayedArray}.
#' This allows us to use Parquet data inside \linkS4class{DataFrame}s without loading them into memory.
#'
#' @param path String containing a path to a Parquet file.
#' @param column String containing the name of the column inside the file.
#' @param length Integer containing the number of rows.
#' If \code{NULL}, this is determined by inspecting the file.
#' This should only be supplied for efficiency purposes, to avoid a file look-up on construction.
#' @param type String specifying the type of the data.
#' If \code{NULL}, this is determined by inspecting the file.
#' Users may specify this to avoid a look-up, or to coerce the output into a different type.
#' @param x Either a string containing the path to a Parquet file (to be used as \code{path}),
#' or an existing ParquetColumnSeed object.
#' @param ... Further arguments to be passed to the \code{ParquetColumnSeed} constructor.
#'
#' @return For \code{ParquetColumnSeed}, a ParquetColumnSeed is returned, obviously.
#' 
#' For \code{ParquetColumnVector}, a ParquetColumnVector is returned.
#'
#' @author Aaron Lun
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
#' type,ParquetColumnSeed-method
#' path,ParquetColumnSeed-method
#' extract_array,ParquetColumnSeed-method
#' ParquetColumnVector-class
#' DelayedArray,ParquetColumnSeed-method
#'
#' @name ParquetColumnSeed
NULL

#' @export
#' @import methods
setClass("ParquetColumnSeed", slots=c(path="character", column="character", length="integer", type="character"))

#' @export
setMethod("dim", "ParquetColumnSeed", function(x) x@length)

#' @export
#' @importFrom DelayedArray type
setMethod("type", "ParquetColumnSeed", function(x) x@type)

#' @export
#' @importFrom BiocGenerics path
setMethod("path", "ParquetColumnSeed", function(object) object@path)

#' @export
#' @importFrom DelayedArray extract_array
setMethod("extract_array", "ParquetColumnSeed", function(x, index) {
    tab <- acquireDataset(x@path)
    slice <- index[[1]]

    if (is.null(slice)) {
        output <- tab[[x@column]]$as_vector()
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

        output <- tab[[x@column]]$Take(slice - 1L)$as_vector()
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
ParquetColumnSeed <- function(path, column, type=NULL, length=NULL) {
    if (is.null(type) || is.null(length)) {
        tab <- acquireDataset(path)
        col <- tab[[column]]
        if (is.null(type)){ 
            type <- DelayedArray::type(col$Slice(0,0)$as_vector())
        }
        if (is.null(length)) {
            length <- nrow(tab)
        }
    } 
    new("ParquetColumnSeed", path=path, column=column, length=length, type=type)
}

#' @export
setClass("ParquetColumnVector", contains="DelayedArray", slots=c(seed="ParquetColumnSeed"))

#' @export
#' @importFrom DelayedArray DelayedArray
setMethod("DelayedArray", "ParquetColumnSeed", function(seed) ParquetColumnVector(seed))

#' @export
#' @rdname ParquetColumnSeed
ParquetColumnVector <- function(x, ...) {
    if (!is(x, "ParquetColumnSeed")) {
        x <- ParquetColumnSeed(x, ...)
    }
    new("ParquetColumnVector", seed=x)
}
