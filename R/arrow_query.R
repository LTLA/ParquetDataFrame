#' Arrow Query Accessor
#'
#' Get the arrow query value contained in an object.
#'
#' @param x An object to get the arrow query value.
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
#' arrow_query,ParquetArray-method
#' arrow_query,ParquetArraySeed-method
#' arrow_query,ParquetColumn-method
#' arrow_query,ParquetDataFrame-method
#' arrow_query,ParquetFactTable-method
#' arrow_query,ParquetMatrix-method
#'
#' @export
setGeneric("arrow_query", function(x, ...) standardGeneric("arrow_query"))

setOldClass("arrow_dplyr_query")

#' @importFrom dplyr pull slice_head
.getColumnType <- function(column_query) {
    DelayedArray::type(pull(slice_head(column_query, n = 0L), as_vector = TRUE))
}

.identicalQueryBody <- function(x, y) {
    body <-  c(".data", "filtered_rows", "group_by_vars", "drop_empty_groups", "arrange_vars", "arrange_desc")
    inherits(x, "arrow_dplyr_query") &&
    inherits(y, "arrow_dplyr_query") &&
    identical(unclass(x)[body], unclass(y)[body])
}

#' @importFrom dplyr filter
.executeQuery <- function(query, key) {
    for (i in names(key)) {
        query <- filter(query, !!as.name(i) %in% key[[i]])
    }

    # Allow for 1 extra row to check for duplicate keys
    length <- prod(lengths(key, use.names = FALSE)) + 1L
    query <- head(query, n = length)

    df <- as.data.frame(query)
    if (anyDuplicated(df[, names(key)])) {
        stop("duplicate keys found in Parquet data")
    }

    df
}
