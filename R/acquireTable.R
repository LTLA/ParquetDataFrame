persistent <- new.env()
persistent$handles <- list()

#' Acquire the Arrow Table 
#'
#' Acquire a (possibly cached) Arrow Table representing a Parquet file given its path.
#' 
#' @param path String containing a path to a Parquet file.
#'
#' @return 
#' For \code{acquireTable}, an Arrow Table identical to that returned by \code{\link{read_parquet}} with \code{as_data_frame=FALSE}.
#'
#' For \code{releaseTable}, any existing Table for the \code{path} is cleared from cache, and \code{NULL} is invisibly returned.
#' If \code{path=NULL}, all cached Tables are removed.
#'
#' @author Aaron Lun
#'
#' @details
#' \code{acquireTable} will cache the Table object in the current R session to avoid repeated initialization.
#' This improves efficiency for repeated calls, e.g., when creating a \linkS4class{DataFrame} with multiple columns from the same Parquet file.
#' The cached Table for any given \code{path} can be deleted by calling \code{releaseTable} for the same \code{path}.
#'
#' @examples
#' # Mocking up a file:
#' tf <- tempfile()
#' on.exit(unlink(tf))
#' arrow::write_parquet(mtcars, tf)
#'
#' acquireTable(tf)
#' acquireTable(tf) # just re-uses the cache
#' releaseTable(tf) # clears the cache
#' @export
#' @importFrom arrow read_parquet
#' @importFrom utils tail
acquireTable <- function(path) {
    # Here we set up an LRU cache for the Parquet handles. 
    # This avoids the initialization time when querying lots of columns.
    nhandles <- length(persistent$handles)

    i <- which(names(persistent$handles) == path)
    if (length(i)) {
        output <- persistent$handles[[i]]
        if (i < nhandles) {
            persistent$handles <- persistent$handles[c(seq_len(i-1L), seq(i+1L, nhandles), i)] # moving to the back
        }
        return(output)
    }

    # Pulled this value out of my ass.
    limit <- 100
    if (nhandles >= limit) {
        persistent$handles <- tail(persistent$handles, limit - 1L)
    }

    output <- read_parquet(path, as_data_frame=FALSE)
    persistent$handles[[path]] <- output
    output
}

#' @export
#' @rdname acquireTable
releaseTable <- function(path) {
    if (is.null(path)) {
        persistent$handles <- list()
    } else {
        i <- which(names(persistent$handles) == path)
        if (length(i)) {
            persistent$handles <- persistent$handles[-i]
        }
    }
    invisible(NULL)
}
