#' Key Names and Key Count
#'
#' Get the key names, key dimension names, or key count of an object.
#'
#' @param x An object to get the key related information.
#' @param value A character vector of key dimension names.
#'
#' @author Patrick Aboyoun
#'
#' @examples
#' keynames
#' showMethods("keynames")
#'
#' keydimnames
#' showMethods("keydimnames")
#'
#' nkey
#' showMethods("nkey")
#'
#' @aliases
#' keynames
#' keydimnames
#' keydimnames<-
#' nkey
#'
#' keynames,ParquetFactTable-method
#' keydimnames,ParquetFactTable-method
#' keydimnames<-,ParquetFactTable-method
#' nkey,ParquetFactTable-method
#'
#' @name keynames
NULL

#'
#' @export
#' @rdname keynames
setGeneric("keynames", function(x) standardGeneric("keynames"))

#' @export
#' @rdname keynames
setGeneric("keydimnames", function(x, value) standardGeneric("keydimnames"))

#' @export
#' @rdname keynames
setGeneric("keydimnames<-", function(x, value) standardGeneric("keydimnames<-"))

#' @export
#' @rdname keynames
setGeneric("nkey", function(x) standardGeneric("nkey"))
