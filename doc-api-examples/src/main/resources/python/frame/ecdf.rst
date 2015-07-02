Consider the following sample data set in *frame* with actual data labels
specified in the *labels* column and the predicted labels in the
*predictions* column:

>>> import intelanalytics as ia
>>> import pandas as p
>>> f = ia.Frame(ia.Pandas(p.DataFrame([1, 3, 1, 0]), [('numbers', ia.int32)]))
  [==Job Progress...]

>>> f.take(5)
[[1], [3], [1], [0]]

#[==Job Progress...]
#    >>> result = f.ecdf('numbers')
#    >>> result.inspect()
#
#      b:int32   b_ECDF:float64
#    /--------------------------/
#       1             0.2
#       2             0.5
#       3             0.8
#       4             1.0

