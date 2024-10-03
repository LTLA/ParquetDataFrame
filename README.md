# Bioconductor-compatible Parquet objects

This package implements Bioconductor-friendly bindings to Parquet files so that they can be used inside standard objects like `DataFrame`s and `SummarizedExperiment`s.
Usage is pretty simple:

```r
tf <- tempfile()
arrow::write_parquet(mtcars, tf)

library(ParquetDataFrame)
df <- ParquetDataFrame(tf)
df
## ParquetDataFrame with 32 rows and 11 columns
##                       mpg                   cyl                  disp
##     <ParquetColumnVector> <ParquetColumnVector> <ParquetColumnVector>
## 1                      21                     6                   160
## 2                      21                     6                   160
## 3                    22.8                     4                   108
## 4                    21.4                     6                   258
## 5                    18.7                     8                   360
## ...                   ...                   ...                   ...
## 28                   30.4                     4                  95.1
## 29                   15.8                     8                   351
## 30                   19.7                     6                   145
## 31                     15                     8                   301
## 32                   21.4                     4                   121
##                        hp                  drat                    wt
##     <ParquetColumnVector> <ParquetColumnVector> <ParquetColumnVector>
## 1                     110                   3.9                  2.62
## 2                     110                   3.9                 2.875
## 3                      93                  3.85                  2.32
## 4                     110                  3.08                 3.215
## 5                     175                  3.15                  3.44
## ...                   ...                   ...                   ...
## 28                    113                  3.77                 1.513
## 29                    264                  4.22                  3.17
## 30                    175                  3.62                  2.77
## 31                    335                  3.54                  3.57
## 32                    109                  4.11                  2.78
##                      qsec                    vs                    am
##     <ParquetColumnVector> <ParquetColumnVector> <ParquetColumnVector>
## 1                   16.46                     0                     1
## 2                   17.02                     0                     1
## 3                   18.61                     1                     1
## 4                   19.44                     1                     0
## 5                   17.02                     0                     0
## ...                   ...                   ...                   ...
## 28                   16.9                     1                     1
## 29                   14.5                     0                     1
## 30                   15.5                     0                     1
## 31                   14.6                     0                     1
## 32                   18.6                     1                     1
##                      gear                  carb
##     <ParquetColumnVector> <ParquetColumnVector>
## 1                       4                     4
## 2                       4                     4
## 3                       4                     1
## 4                       3                     1
## 5                       3                     2
## ...                   ...                   ...
## 28                      5                     2
## 29                      5                     4
## 30                      5                     6
## 31                      5                     8
## 32                      4                     2
```

This produces a file-backed `ParquetDataFrame`, consisting of file-backed `ParquetColumnVector` objects.
These can be realized into memory via the usual `as.vector()` method.

```r
class(df$mpg)
## [1] "ParquetColumnVector"
## attr(,"package")
## [1] "ParquetDataFrame"

as.vector(df$mpg)
##  [1] 21.0 21.0 22.8 21.4 18.7 18.1 14.3 24.4 22.8 19.2 17.8 16.4 17.3 15.2 10.4
## [16] 10.4 14.7 32.4 30.4 33.9 21.5 15.5 15.2 13.3 19.2 27.3 26.0 30.4 15.8 19.7
## [31] 15.0 21.4
```

We can now create a `SummarizedExperiment` consisting of a `ParquetDataFrame`, let's say in the `colData`:

```r
library(SummarizedExperiment)
se <- SummarizedExperiment(
    list(stuff=matrix(runif(nrow(df) * 10), nrow=10)), # mocking up an assay
    colData=df
)
class(colData(se, withDimnames=FALSE))
## [1] "ParquetDataFrame"
## attr(,"package")
## [1] "ParquetDataFrame"
```

This behaves properly when we operate on the parent structure, even if those operations introduce inconsistencies from the Parquet file.
In such cases, the `ParquetDataFrame` will collapse to a `DFrame` of `ParquetColumnVector`s, still file-backed but possibly with delayed operations.

```r
se$random_junk <- runif(ncol(se))
se <- se[,se$random_junk < 0.5]
## DataFrame with 17 rows and 12 columns
##                mpg            cyl           disp             hp           drat
##     <DelayedArray> <DelayedArray> <DelayedArray> <DelayedArray> <DelayedArray>
## 1               21              6            160            110            3.9
## 2               21              6            160            110            3.9
## 3             22.8              4            108             93           3.85
## 4             21.4              6            258            110           3.08
## 5             18.7              8            360            175           3.15
## ...            ...            ...            ...            ...            ...
## 13            21.5              4          120.1             97            3.7
## 14            15.5              8            318            150           2.76
## 15            19.2              8            400            175           3.08
## 16            30.4              4           95.1            113           3.77
## 17            21.4              4            121            109           4.11
##                 wt           qsec             vs             am           gear
##     <DelayedArray> <DelayedArray> <DelayedArray> <DelayedArray> <DelayedArray>
## 1             2.62          16.46              0              1              4
## 2            2.875          17.02              0              1              4
## 3             2.32          18.61              1              1              4
## 4            3.215          19.44              1              0              3
## 5             3.44          17.02              0              0              3
## ...            ...            ...            ...            ...            ...
## 13           2.465          20.01              1              0              3
## 14            3.52          16.87              0              0              3
## 15           3.845          17.05              0              0              3
## 16           1.513           16.9              1              1              5
## 17            2.78           18.6              1              1              4
##               carb random_junk
##     <DelayedArray>   <numeric>
## 1                4    0.115055
## 2                4    0.238806
## 3                1    0.434618
## 4                1    0.146558
## 5                2    0.494950
## ...            ...         ...
## 13               1   0.4308120
## 14               2   0.1875856
## 15               2   0.3112936
## 16               2   0.0976866
## 17               2   0.4853946
```

Advanced users can also retrieve the Arrow Dataset for direct queries against the Parquet file:

```r
acquireDataset(path(df))
## Table
## 32 rows x 11 columns
## $mpg <double>
## $cyl <double>
## $disp <double>
## $hp <double>
## $drat <double>
## $wt <double>
## $qsec <double>
## $vs <double>
## $am <double>
## $gear <double>
## $carb <double>
## 
## See $metadata for additional Schema metadata
```
