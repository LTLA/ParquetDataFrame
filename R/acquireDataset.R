persistent <- new.env()
persistent$handles <- list()

setOldClass("arrow_dplyr_query")

identicalQueryBody <- function(x, y) {
    body <-  c(".data", "filtered_rows", "group_by_vars", "drop_empty_groups", "arrange_vars", "arrange_desc")
    inherits(x, "arrow_dplyr_query") &&
    inherits(y, "arrow_dplyr_query") &&
    identical(unclass(x)[body], unclass(y)[body])
}

#' Acquire the Arrow Dataset
#'
#' Acquire a (possibly cached) Arrow Dataset created from Parquet data.
#'
#' @param path String specifying a path to a Parquet data directory or file.
#' @param ... Further arguments to be passed to \code{\link[arrow]{open_dataset}}.
#'
#' @return
#' For \code{acquireDataset}, an Arrow Dataset identical to that returned by \code{as_arrow_table(\link[arrow]{open_dataset})}.
#'
#' For \code{releaseDataset}, any existing Dataset for the \code{path} is cleared from cache, and \code{NULL} is invisibly returned.
#' If \code{path=NULL}, all cached Datasets are removed.
#'
#' @author Aaron Lun, Patrick Aboyoun
#'
#' @details
#' \code{acquireDataset} will cache the Dataset object in the current R session to avoid repeated initialization.
#' This improves efficiency for repeated calls, e.g., when creating a \linkS4class{DataFrame} with multiple columns from the same Parquet data path.
#' The cached Dataset for any given \code{path} can be deleted by calling \code{releaseDataset} for the same \code{path}.
#'
#' @examples
#' # Mocking up a parquet file:
#' tf <- tempfile()
#' on.exit(unlink(tf))
#' arrow::write_parquet(mtcars, tf)
#'
#' acquireDataset(tf)
#' acquireDataset(tf) # just re-uses the cache
#' releaseDataset(tf) # clears the cache
#'
#' # Mocking up a parquet data diretory:
#' td <- tempfile()
#' on.exit(unlink(td), add = TRUE)
#' arrow::write_dataset(mtcars, td, format = "parquet", partitioning = "cyl")
#'
#' acquireDataset(td)
#' acquireDataset(td) # just re-uses the cache
#' releaseDataset(td) # clears the cache
#' @export
#' @importFrom arrow as_arrow_table
#' @importFrom arrow open_dataset
#' @importFrom S4Vectors isSingleString
#' @importFrom utils tail
acquireDataset <- function(path, ...) {
    if (!(isSingleString(path) && nzchar(path))) {
        stop("'path' must be a single non-empty string")
    }

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

    output <- open_dataset(path, format = "parquet", ...)
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
