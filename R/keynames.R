#' Key Names and Key Count
#'
#' Get the key names or key count of an object.
#'
#' @param x An object to get the key names or key count.
#' @param ... Additional arguments, for use in specific methods.
#'
#' @author Patrick Aboyoun
#'
#' @examples
#' keynames
#' showMethods("keynames")
#'
#' nkey
#' showMethods("nkey")
#'
#' @aliases
#' keynames
#' nkey
#'
#' keynames,ParquetFactTable-method
#' nkey,ParquetFactTable-method
#'
#' @export
setGeneric("keynames", function(x, ...) standardGeneric("keynames"))

#' @export
setGeneric("nkey", function(x) standardGeneric("nkey"))
