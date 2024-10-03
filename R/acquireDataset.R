persistent <- new.env()
persistent$handles <- list()

#' Acquire the Arrow Dataset
#'
#' Acquire a (possibly cached) Arrow Dataset representing a Parquet file given its path.
#' 
#' @param path String containing a path to a Parquet file.
#'
#' @return 
#' For \code{acquireDataset}, an Arrow Dataset identical to that returned by \code{\link{read_parquet}} with \code{as_data_frame=FALSE}.
#'
#' For \code{releaseDataset}, any existing Dataset for the \code{path} is cleared from cache, and \code{NULL} is invisibly returned.
#' If \code{path=NULL}, all cached Datasets are removed.
#'
#' @author Aaron Lun
#'
#' @details
#' \code{acquireDataset} will cache the Dataset object in the current R session to avoid repeated initialization.
#' This improves efficiency for repeated calls, e.g., when creating a \linkS4class{DataFrame} with multiple columns from the same Parquet file.
#' The cached Dataset for any given \code{path} can be deleted by calling \code{releaseDataset} for the same \code{path}.
#'
#' @examples
#' # Mocking up a file:
#' tf <- tempfile()
#' on.exit(unlink(tf))
#' arrow::write_parquet(mtcars, tf)
#'
#' acquireDataset(tf)
#' acquireDataset(tf) # just re-uses the cache
#' releaseDataset(tf) # clears the cache
#' @export
#' @importFrom arrow read_parquet
#' @importFrom utils tail
acquireDataset <- function(path) {
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
#' @rdname acquireDataset
releaseDataset <- function(path) {
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
