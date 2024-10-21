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
#' Arith,ParquetArray,ParquetArray-method
#' Arith,ParquetArray,numeric-method
#' Arith,numeric,ParquetArray-method
#' Compare,ParquetArray,ParquetArray-method
#' Compare,ParquetArray,vector-method
#' Compare,vector,ParquetArray-method
#' Logic,ParquetArray,ParquetArray-method
#' Math,ParquetArray-method
#'
#' @seealso
#' \code{\link{ParquetArraySeed}},
#' \code{\link[DelayedArray]{DelayedArray}}
#'
#' @include arrow_query.R
#' @include ParquetArraySeed.R
#'
#' @name ParquetArray
NULL

#' @export
#' @importClassesFrom DelayedArray DelayedArray
setClass("ParquetArray", contains = "DelayedArray", slots = c(seed = "ParquetArraySeed"))

#' @export
#' @importFrom DelayedArray seed
setMethod("arrow_query", "ParquetArray", function(x) x@seed@query)

#' @export
setMethod("[", "ParquetArray", function(x, i, j, ..., drop = TRUE) {
    Nindex <- S4Arrays:::extract_Nindex_from_syscall(sys.call(), parent.frame())
    initialize(x, seed = .subset_ParquetArraySeed(x@seed, Nindex = Nindex, drop = drop))
})

#' @export
#' @importFrom BiocGenerics aperm
setMethod("aperm", "ParquetArray", function(a, perm, ...) {
    initialize(a, seed = aperm(a@seed, perm = perm, ...))
})

#' @export
#' @importFrom BiocGenerics t
setMethod("t", "ParquetArray", function(x) {
    initialize(x, seed = t(x@seed))
})

#' @export
setMethod("Arith", c(e1 = "ParquetArray", e2 = "ParquetArray"), function(e1, e2) {
    initialize(e1, seed = callGeneric(e1@seed, e2@seed))
})

#' @export
setMethod("Arith", c(e1 = "ParquetArray", e2 = "numeric"), function(e1, e2) {
    initialize(e1, seed = callGeneric(e1@seed, e2))
})

#' @export
setMethod("Arith", c(e1 = "numeric", e2 = "ParquetArray"), function(e1, e2) {
    initialize(e2, seed = callGeneric(e1, e2@seed))
})

#' @export
setMethod("Compare", c(e1 = "ParquetArray", e2 = "ParquetArray"), function(e1, e2) {
    initialize(e1, seed = callGeneric(e1@seed, e2@seed))
})

#' @export
setMethod("Compare", c(e1 = "ParquetArray", e2 = "vector"), function(e1, e2) {
    initialize(e1, seed = callGeneric(e1@seed, e2))
})

#' @export
setMethod("Compare", c(e1 = "vector", e2 = "ParquetArray"), function(e1, e2) {
    initialize(e2, seed = callGeneric(e1, e2@seed))
})

#' @export
setMethod("Logic", c(e1 = "ParquetArray", e2 = "ParquetArray"), function(e1, e2) {
    initialize(e1, seed = callGeneric(e1@seed, e2@seed))
})

#' @export
setMethod("Math", "ParquetArray", function(x) {
    initialize(x, seed = callGeneric(x@seed))
})

#' @export
#' @rdname ParquetArray
ParquetArray <- function(data, key, value, type = NULL, ...) {
    if (!is(data, "ParquetArraySeed")) {
        data <- ParquetArraySeed(data, key = key, value = value, type = type, ...)
    }
    new("ParquetArray", seed = data)
}
