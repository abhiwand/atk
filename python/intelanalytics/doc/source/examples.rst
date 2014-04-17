
Examples
========

>>> from intelanalytics import *

>>> # (using LittleFrame for these examples)
>>> from intelanalytics.little.frame import LittleFrame
>>> BigFrame = LittleFrame

>>> from intelanalytics.core.sources import SimpleDataSource
>>> f = BigFrame(SimpleDataSource([('i', int32), ('s', int32)],
...                                rows=[(1, 'one'),
...                                      (3, 'three'),
...                                      (None, 'seven'),
...                                      (-2, 'minus two'),
...                                      (-5, 'minus five'),
...                                      (None, None),
...                                      (8, None),
...                                      (10, 'ten')]))
>>> f
  i:int32     s:int32
        1         one
        3       three
     None       seven
       -2   minus two
       -5  minus five
     None        None
        8        None
       10         ten

>>> f.inspect(3)
  i:int32 s:int32
        1     one
        3   three
     None   seven

>>> f.take(2)
[(1, 'one'), (3, 'three')]

>>> f.dropna(any)

>>> f.inspect()
  i:int32     s:int32
        1         one
        3       three
       -2   minus two
       -5  minus five
       10         ten

>>> f.drop(lambda row: row['i'] < 0)
>>> f.inspect()
  i:int32 s:int32
        1     one
        3   three
       10     ten

>>> f.filter(lambda row: row.s.startswith('t'))

>>> f.inspect()
  i:int32 s:int32
        3   three
       10     ten
