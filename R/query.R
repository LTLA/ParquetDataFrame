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
#' query
#' showMethods("query")
#'
#' @aliases
#' query
#'
#' query,ParquetColumnSeed-method
#' query,ParquetColumnVector-method
#' query,ParquetDataFrame-method
#'
#' @export
setGeneric("query", function(x, ...) standardGeneric("query"))

setOldClass("arrow_dplyr_query")

identicalQueryBody <- function(x, y) {
    body <-  c(".data", "filtered_rows", "group_by_vars", "drop_empty_groups", "arrange_vars", "arrange_desc")
    inherits(x, "arrow_dplyr_query") &&
    inherits(y, "arrow_dplyr_query") &&
    identical(unclass(x)[body], unclass(y)[body])
}
