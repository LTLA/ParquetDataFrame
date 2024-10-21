#' ParquetColumn objects
#'
#' @author Patrick Aboyoun
#'
#' @include arrow_query.R
#' @include ParquetArray.R
#'
#' @aliases
#' ParquetColumn-class
#' as.vector,ParquetColumn-method
#' extractROWS,ParquetColumn,ANY-method
#' head,ParquetColumn-method
#' length,ParquetColumn-method
#' names,ParquetColumn-method
#' show,ParquetColumn-method
#' showAsCell,ParquetColumn-method
#' tail,ParquetColumn-method
#' Ops,ParquetColumn,ParquetColumn-method
#' Ops,ParquetColumn,vector-method
#' Ops,vector,ParquetColumn-method
#' Math,ParquetColumn-method
#'
#' @name ParquetColumn
NULL

#' @export
#' @importClassesFrom S4Vectors Vector
setClass("ParquetColumn", contains = "Vector", slots = c(data = "ParquetArray"))

#' @export
#' @importFrom S4Vectors classNameForDisplay
#' @importFrom utils capture.output
setMethod("show", "ParquetColumn", function(object) {
    text <- capture.output(object@data)
    text[1L] <- sub(classNameForDisplay(object@data), classNameForDisplay(object), text[1L])
    cat(text, sep = "\n")
})

#' @export
#' @importFrom S4Vectors showAsCell
setMethod("showAsCell", "ParquetColumn", function(object) {
    callGeneric(as.vector(object@data))
})

#' @export
setMethod("arrow_query", "ParquetColumn", function(x) x@data@seed@query)

#' @export
setMethod("length", "ParquetColumn", function(x) length(x@data))

#' @export
setMethod("names", "ParquetColumn", function(x) names(names(x@data)))

#' @export
setMethod("extractROWS", "ParquetColumn", function(x, i) {
    if (is(i, "ParquetColumn")) {
        i <- i@data
    }
    initialize(x, data = callGeneric(x@data, i = i))
})

#' @export
#' @importFrom S4Vectors head isSingleNumber
setMethod("head", "ParquetColumn", function(x, n = 6L, ...) {
    if (!isSingleNumber(n)) {
        stop("'n' must be a single number")
    }
    n <- as.integer(n)
    if (n < 0) {
        n <- max(0L, length(x) + n)
    }
    if (n > length(x)) {
        x
    } else {
        x@data@seed@key[[1L]] <- head(x@data@seed@key[[1L]], n)
        x
    }
})

#' @export
#' @importFrom S4Vectors isSingleNumber tail
setMethod("tail", "ParquetColumn", function(x, n = 6L, ...) {
    if (!isSingleNumber(n)) {
        stop("'n' must be a single number")
    }
    n <- as.integer(n)
    if (n < 0) {
        n <- max(0L, length(x) + n)
    }
    if (n > length(x)) {
        x
    } else {
        x@data@seed@key[[1L]] <- tail(x@data@seed@key[[1L]], n)
        x
    }
})

#' @export
setMethod("Ops", c(e1 = "ParquetColumn", e2 = "ParquetColumn"), function(e1, e2) {
    initialize(e1, data = callGeneric(e1@data, e2@data))
})

#' @export
setMethod("Ops", c(e1 = "ParquetColumn", e2 = "vector"), function(e1, e2) {
    initialize(e1, data = callGeneric(e1@data, e2))
})

#' @export
setMethod("Ops", c(e1 = "vector", e2 = "ParquetColumn"), function(e1, e2) {
    initialize(e1, data = callGeneric(e1, e2@data))
})

#' @export
setMethod("Math", "ParquetColumn", function(x) {
    initialize(x, data = callGeneric(x@data))
})

#' @export
#' @importFrom BiocGenerics as.vector
setMethod("as.vector", "ParquetColumn", function(x, mode = "any") {
    vec <- c(as.array(x@data))
    if (mode != "any") {
        storage.mode(vec) <- mode
    }
    vec
})
