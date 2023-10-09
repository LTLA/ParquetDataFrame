persistent <- new.env()
persistent$handles <- list()

#' Acquire the Parquet file handle
#'
#' Acquire a (possibly cached) handle to a Parquet file, given its path.
#' 
#' @param path String containing a path to a Parquet file.
#'
#' @return 
#' For \code{acquireHandle}, an Arrow Table identical to that returned by \code{\link{read_parquet}} with \code{as_data_frame=FALSE}.
#'
#' For \code{releaseHandle}, any existing Table for the \code{path} is cleared from cache, and \code{NULL} is invisibly returned.
#' If \code{path=NULL}, all cached handles are removed.
#'
#' @author Aaron Lun
#'
#' @details
#' \code{acquireHandle} will cache the file handle in the current R session to avoid repeated initialization.
#' This improves efficiency for repeated calls, e.g., when creating a \linkS4class{DataFrame} with multiple columns from the same Parquet file.
#' The cached handle for any given \code{path} can be removed from the cache by calling \code{releaseHandle}.
#'
#' @examples
#' # Mocking up a file:
#' tf <- tempfile()
#' on.exit(unlink(tf))
#' arrow::write_parquet(mtcars, tf)
#'
#' acquireHandle(tf)
#' acquireHandle(tf) # just re-uses the cache
#' releaseHandle(tf) # clears the cache
#' @export
#' @importFrom arrow read_parquet
#' @importFrom utils tail
acquireHandle <- function(path) {
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
#' @rdname acquireHandle
releaseHandle <- function(path) {
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
