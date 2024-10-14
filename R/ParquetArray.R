#' Parquet datasets as DelayedArray objects
#'
#' @description
#' The ParquetArray class is a \link[DelayedArray]{DelayedArray} subclass
#' for representing and operating on a Parquet dataset.
#'
#' All the operations available for \link[DelayedArray]{DelayedArray}
#' objects work on ParquetArray objects.
#'
#' @inheritParams ParquetArraySeed
#'
#' @author Patrick Aboyoun
#'
#' @examples
#' # Create a data.frame from the Titanic data
#' df <- do.call(expand.grid, c(dimnames(Titanic), stringsAsFactors = FALSE))
#' df$fate <- Titanic[as.matrix(df)]
#'
#' # Write data to a parquet file
#' tf <- tempfile()
#' on.exit(unlink(tf))
#' arrow::write_parquet(df, tf)
#'
#' pqarray <- ParquetArray(tf, key = c("Class", "Sex", "Age", "Survived"), value = "fate")
#'
#' @aliases
#' ParquetArray-class
#' [,ParquetArray,ANY,ANY,ANY-method
#' aperm,ParquetArray-method
#' t,ParquetArray-method
#'
#' @seealso
#' \code{\link{ParquetArraySeed}},
#' \code{\link[DelayedArray]{DelayedArray}}
#'
#' @include ParquetArraySeed.R
#'
#' @name ParquetArray
NULL

#' @export
#' @importClassesFrom DelayedArray DelayedArray
setClass("ParquetArray", contains = "DelayedArray", slots = c(seed = "ParquetArraySeed"))

#' @export
#' @importFrom DelayedArray seed
setMethod("arrow_query", "ParquetArray", function(x) arrow_query(seed(x)))

#' @export
setMethod("[", "ParquetArray", function(x, i, j, ..., drop = TRUE) {
    Nindex <- S4Arrays:::extract_Nindex_from_syscall(sys.call(), parent.frame())
    initialize(x, seed = .subset_ParquetArraySeed(seed(x), Nindex = Nindex, drop = drop))
})

#' @export
#' @importFrom BiocGenerics aperm
setMethod("aperm", "ParquetArray", function(a, perm, ...) {
    initialize(a, seed = aperm(seed(a), perm = perm, ...))
})

#' @export
#' @importFrom BiocGenerics t
setMethod("t", "ParquetArray", function(x) {
    initialize(x, seed = t(seed(x)))
})

#' @export
#' @rdname ParquetArray
ParquetArray <- function(data, key, value, type = NULL, ...) {
    if (!is(data, "ParquetArraySeed")) {
        data <- ParquetArraySeed(data, key = key, value = value, type = type, ...)
    }
    new("ParquetArray", seed = data)
}
