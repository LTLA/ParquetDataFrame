#' Query Accessor
#'
#' Get or set the query value contained in an object.
#'
#' @param x An object to get or set the query value.
#' @param ... Additional arguments, for use in specific methods.
#'
#' @author Patrick Aboyoun
#'
#' @examples
#' arrow_query
#' showMethods("arrow_query")
#'
#' @aliases
#' arrow_query
#'
#' arrow_query,ParquetColumnSeed-method
#' arrow_query,ParquetColumnVector-method
#' arrow_query,ParquetDataFrame-method
#'
#' @export
setGeneric("arrow_query", function(x, ...) standardGeneric("arrow_query"))

setOldClass("arrow_dplyr_query")

.identicalQueryBody <- function(x, y) {
    body <-  c(".data", "filtered_rows", "group_by_vars", "drop_empty_groups", "arrange_vars", "arrange_desc")
    inherits(x, "arrow_dplyr_query") &&
    inherits(y, "arrow_dplyr_query") &&
    identical(unclass(x)[body], unclass(y)[body])
}
