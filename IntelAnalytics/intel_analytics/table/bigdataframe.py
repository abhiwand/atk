"""
BigDataFrame
"""

from intel_analytics.graph.titan.graph import TitanGraphBuilderFactory

class BigDataFrameException(Exception):
    pass

class BigDataFrame(object):
    """
    BigDataFrame

    Proxy for large 2D container to work with table data at scale
    """


    def __init__(self, table):
        """
        (internal constructor)
        Parameters
        ----------
        table : Table
        graphbuilders: A factory of graph builder libraries to construct graphs at scale from HBase tables to Titan graph database
        """
        #if not isinstance(table, Table):
        #    raise Exception("bad table given to Constructor")
        if table is None:
            raise Exception("BigDataFrame Constructor requires non-None table")

        self._table = table
        #this holds the original table that we imported the data, will be used for understanding which features are derived or not
        self.origin_table_name=self._table.table_name
        self.lineage=[]
        self.lineage.append(self._table.table_name)
        self._graphbuilder_factory_class = TitanGraphBuilderFactory

    def __str__(self):
        buf = 'BigDataFrame{ '
        for key in self.__dict__:
            if not key.startswith('_'):
                buf+='%s:%s ' % (key, self.__dict__[key])
        buf += '}'
        return buf

    def get_table_name(self):
        return self._table.table_name

    def get_schema(self):
        """
        Returns the list of column names/types
        """
        return self._table.get_schema()

    #----------------------------------------------------------------------
    # TODO - Save/Restore/Export
    #----------------------------------------------------------------------

    def to_csv(filename, include_header=True, include_schema=True):
        """
        Serialize the current state of the table to a CSV file

        Parameters
        ----------
        filename : String
            name of file
        include_header : Boolean
            whether to write the header as the first line
        include_schema : Boolean
            whether also write an schema file with same name
        """
        # We'll create an HDFS folder and shard into files
        # We'll provide a separate, explicit "Download" mechanism to go from
        # HDFS (or S3) to client
        raise BigDataFrameException("Not implemented")

    def to_json(file, include_schema=True):
        """
        Serialize the current state of the table to an XML file

        Parameters
        ----------
        file : String
            name of snapshot file
        include_schema : Boolean
            whether also write an schema file with same name
        """
        #TODO: embed the schema in same file, similar to header?
        raise BigDataFrameException("Not implemented")


    def to_xml(file, include_schema=True):
        """
        Serialize the current state of the table to an XML file

        Parameters
        ----------
        file : String
            name of snapshot file
        include_schema : Boolean
            whether also write an schema file with same name
        """
        #TODO: use a JSON schema (or XML XSD or DTD?) --embed in the same
        #      file, similar to header?
        raise BigDataFrameException("Not implemented")


    #----------------------------------------------------------------------
    # Merging / Joining
    #----------------------------------------------------------------------
    def append(self, other, ignore_index=False, verify_integrity=False):
        """
        (pandas)
        Append columns of other to end of this frame's columns and index,
        returning a new object.  Columns not in this frame are added as new
        columns.

        Parameters
        ----------
        other : DataFrame or list of Series/dict-like objects
        ignore_index : boolean, default False
            If True do not use the index labels. Useful for gluing together
            record arrays
        verify_integrity : boolean, default False
            If True, raise Exception on creating index with duplicates

        Notes
        -----
        If a list of dict is passed and the keys are all contained in the
        DataFrame's index, the order of the columns in the resulting DataFrame
        will be unchanged

        Returns
        -------
        appended : DataFrame

        """
        raise BigDataFrameException("Not implemented")

    def join(self, other, on=None, how='left', lsuffix='', rsuffix='',
             sort=False):
        """
        (pandas)
        Join columns with other DataFrame either on index or on a key
        column. Efficiently Join multiple DataFrame objects by index at once by
        passing a list.

        Parameters
        ----------
        other : DataFrame, Series with name field set, or list of DataFrame
            Index should be similar to one of the columns in this one. If a
            Series is passed, its name attribute must be set, and that will be
            used as the column name in the resulting joined DataFrame
        on : column name, tuple/list of column names, or array-like
            Column(s) to use for joining, otherwise join on index. If multiples
            columns given, the passed DataFrame must have a MultiIndex. Can
            pass an array as the join key if not already contained in the
            calling DataFrame. Like an Excel VLOOKUP operation
        how : {'left', 'right', 'outer', 'inner'}
            How to handle indexes of the two objects. Default: 'left'
            for joining on index, None otherwise
            * left: use calling frame's index
            * right: use input frame's index
            * outer: form union of indexes
            * inner: use intersection of indexes
        lsuffix : string
            Suffix to use from left frame's overlapping columns
        rsuffix : string
            Suffix to use from right frame's overlapping columns
        sort : boolean, default False
            Order result DataFrame lexicographically by the join key. If False,
            preserves the index order of the calling (left) DataFrame

        Notes
        -----
        on, lsuffix, and rsuffix options are not supported when passing a list
        of DataFrame objects

        Returns
        -------
        joined : DataFrame
        """
        raise BigDataFrameException("Not implemented")

    def merge(self, right, how='inner', on=None, left_on=None, right_on=None,
              left_index=False, right_index=False, sort=False,
              suffixes=('_x', '_y'), copy=True):
        """
        (pandas)
        Merge DataFrame objects by performing a database-style join operation by
        columns or indexes.

        If joining columns on columns, the DataFrame indexes *will be
        ignored*. Otherwise if joining indexes on indexes or indexes on a
        column or columns, the index will be passed on.

        Parameters
        ----------%s
        right : DataFrame
        how : {'left', 'right', 'outer', 'inner'}, default 'inner'
            * left: use only keys from left frame (SQL: left outer join)
            * right: use only keys from right frame (SQL: right outer join)
            * outer: use union of keys from both frames (SQL: full outer join)
            * inner: use intersection of keys from both frames (SQL: inner join)
        on : label or list
            Field names to join on. Must be found in both DataFrames. If on is
            None and not merging on indexes, then it merges on the intersection
            of the columns by default.
        left_on : label or list, or array-like
            Field names to join on in left DataFrame. Can be a vector or list of
            vectors of the length of the DataFrame to use a particular vector as
            the join key instead of columns
        right_on : label or list, or array-like
            Field names to join on in right DataFrame or vector/list of vectors
            per left_on docs
        left_index : boolean, default False
            Use the index from the left DataFrame as the join key(s). If it is a
            MultiIndex, the number of keys in the other DataFrame (either the
            index or a number of columns) must match the number of levels
        right_index : boolean, default False
            Use the index from the right DataFrame as the join key. Same
            caveats as left_index
        sort : boolean, default False
            Sort the join keys lexicographically in the result DataFrame
        suffixes : 2-length sequence (tuple, list, ...)
            Suffix to apply to overlapping column names in the left and right
            side, respectively
        copy : boolean, default True
            If False, do not copy data unnecessarily

        Examples
        --------

        >>> A              >>> B
            lkey value         rkey value
        0   foo  1         0   foo  5
        1   bar  2         1   bar  6
        2   baz  3         2   qux  7
        3   foo  4         3   bar  8

        >>> merge(A, B, left_on='lkey', right_on='rkey', how='outer')
           lkey  value_x  rkey  value_y
        0  bar   2        bar   6
        1  bar   2        bar   8
        2  baz   3        NaN   NaN
        3  foo   1        foo   5
        4  foo   4        foo   5
        5  NaN   NaN      qux   7

        Returns
        -------
        merged : DataFrame
        """
        raise BigDataFrameException("Not implemented")

    #----------------------------------------------------------------------
    # Canned Transforms
    #  String Manipulation
    #  Math Functions
    #  Aggregate and Eval
    #  Date/Time
    #  Time Series, Windows, Categorical variables
    #----------------------------------------------------------------------
    # TODO - column crossing function

    # TODO - 0.5 add the methods we use from Pig HERE:


    #----------------------------------------------------------------------
    # Apply User-Defined Function (canned and UDF)
    #----------------------------------------------------------------------


    def transform(self, column_name, new_column_name, transformation, keep_source_column=False, transformation_args=None):

        """
        Applies a built-in transformation function to the given column

        Parameters
        ----------
        column_name : String
            source column for the function
        new_column_name : String
            name for the new column that will be created as a result of applying the transformation
        transformation : enumeration
            transformation to apply
        keep_source_column: boolean
            whether to keep the given column in the output of the transformation
        transformation_args: list
            the arguments for the transformation to apply
        """
        self._table.transform(column_name, new_column_name, transformation, keep_source_column, transformation_args)
        self.lineage.append(self._table.table_name)

    #TODO - how to pass UDFs through to mapreduce
    def apply(self, column_name, func, output_type):

        """
        Applies a user-defined function (UDF) to the given column

        Parameters
        ----------
        column_name : String
            src column for the function
        func : function
            function to apply
        output_type: DataType
            data type of the output


        (pandas)
    #def apply(self, func, axis=0, broadcast=False, raw=False, args=(), **kwds):
        Applies function along input axis of DataFrame. Objects passed to
        functions are Series objects having index either the DataFrame's index
        (axis=0) or the columns (axis=1). Return type depends on whether passed
        function aggregates

        Parameters
        ----------
        func : function
            Function to apply to each column
        axis : {0, 1}
            0 : apply function to each column
            1 : apply function to each row
        broadcast : bool, default False
            For aggregation functions, return object of same size with values
            propagated
        raw : boolean, default False
            If False, convert each row or column into a Series. If raw=True the
            passed function will receive ndarray objects instead. If you are
            just applying a NumPy reduction function this will achieve much
            better performance
        args : tuple
            Positional arguments to pass to function in addition to the
            array/series
        Additional keyword arguments will be passed as keywords to the function

        Examples
        --------
        >>> df.apply(numpy.sqrt) # returns DataFrame
        >>> df.apply(numpy.sum, axis=0) # equiv to df.sum(0)
        >>> df.apply(numpy.sum, axis=1) # equiv to df.sum(1)

        See also
        --------
        DataFrame.applymap: For elementwise operations

        Returns
        -------
        applied : Series or DataFrame
        """
        raise BigDataFrameException("Not implemented")

    def applymap(self, column_name, func, output_type):
        """
        Applies a user-defined function (UDF) to each cell in the given column

        Parameters
        ----------
        column_name : String
            source column for the function
        func : function
            function to apply
        output_type: DataType
            data type of the output

        (pandas frame.py)
    #def applymap(self, func):
        Apply a function to a DataFrame that is intended to operate
        elementwise, i.e. like doing map(func, series) for each series in the
        DataFrame

        Parameters
        ----------
        func : function
            Python function, returns a single value from a single value

        Returns
        -------
        applied : DataFrame
        """
        raise BigDataFrameException("Not implemented")


    # TODO: groupby needs more study
    def groupby(self, by=None, axis=0, level=None, as_index=True, sort=True,
                group_keys=True, squeeze=False):
        """
        (from pandas/core/generic.py, NDFrame)

        Group series using mapper (dict or key function, apply given function
        to group, return result as series) or by a series of columns

        Parameters
        ----------
        by : mapping function / list of functions, dict, Series, or tuple /
            list of column names.
            Called on each element of the object index to determine the groups.
            If a dict or Series is passed, the Series or dict VALUES will be
            used to determine the groups
        axis : int, default 0
        level : int, level name, or sequence of such, default None
            If the axis is a MultiIndex (hierarchical), group by a particular
            level or levels
        as_index : boolean, default True
            For aggregated output, return object with group labels as the
            index. Only relevant for DataFrame input. as_index=False is
            effectively "SQL-style" grouped output
        sort : boolean, default True
            Sort group keys. Get better performance by turning this off
        group_keys : boolean, default True
            When calling apply, add group keys to index to identify pieces
        squeeze : boolean, default False
            reduce the dimensionaility of the return type if possible, otherwise
            return a consistent type

        Examples
        --------
        # DataFrame result
        >>> data.groupby(func, axis=0).mean()

        # DataFrame result
        >>> data.groupby(['col1', 'col2'])['col3'].mean()

        # DataFrame with hierarchical index
        >>> data.groupby(['col1', 'col2']).mean()

        Returns
        -------
        GroupBy object
        (some context for future functions or another BDF)
        """
        raise BigDataFrameException("Not implemented")



    #----------------------------------------------------------------------
    # Indexing, Iteration
    #----------------------------------------------------------------------

    # def insert(self, loc, column, value, allow_duplicates=False):
    # Probably don't need --the transform functions will produce new columns
    # and optionally keeps or overwrites the src column
    # 2 options for new column:
    #     1) in-place transformation --> add new column to current table
    #     2) create a deep copy of table with new column included
    # Also, do I keep the src column?
    # these options do not require an explicity insert command

    # Hold off doing the BigColumn slicing, >0.5
    #def __getitem__(self, column_name):
    #    """
    #    Accesses a particular column
    #
    #    Parameters
    #    ----------
    #    column_name : String
    #        name of the column
    #
    #    Returns
    #    -------
    #    column : BigColumn
    #        proxy to the selected column
    #    """
    #    print "Not implemented"


    def head(self, n=10):
        """
        Provides string representation of the first n lines of the table

        Parameters
        ----------
        n : int
            number of rows

        Returns
        -------
        head : String
        """
        # for IPython, consider dumping 2D array (NDarray) for pretty-print
        self._table.head(n)


        # How do I manually create a row? (not doing)

        # Not doing __setitem__ for 0.5
        # instead we'll do:
        #
        #    bdf['x'].apply(sq, 'sqx')
        #
        #def __setitem__(self, column_name, value):
        #"""
        #sets/insert the column name
        #"""
        ## bdf['sqx'] = bdf['x'].apply(sq)
        #print "Not implemented"

        #  Index-based lookup doesn't fit the HBase model unless we provide
        #  consequetive row key, which we shouldn't do anyway because of perf

        #def get_value(self, index, col):
        #"""
        #Quickly retrieve single value at passed column and index  (row key)
    #
    #Parameters
    #----------
    #index : row label
    #col : column label
    #
    #Returns
    #-------
    #value : scalar value
    #"""
    #print "Not implemented"

    #def set_value(self, index, col, value):
    #"""
    #Put single value at passed column and index
    #
    #Parameters
    #----------
    #index : row label
    #col : column label
    #value : scalar value
    #
    #Returns
    #-------
    #frame : DataFrame
    #If label pair is contained, will be reference to calling DataFrame,
    #otherwise a new object
    #"""
    #print "Not implemented"


    #----------------------------------------------------------------------
    # Cleaning
    #----------------------------------------------------------------------

    def drop(self, column_name, func):
        """
        Drops rows which meet given criteria

        Parameters
        ----------
        column_name : String
            name of column for the function
        func : function
            filter function evaluated at each cell in the given column; if
            result is true, row is dropped
        """
        raise BigDataFrameException("Not implemented")



    def dropna(self, how='any', column_name=None):
    #         def dropna(self, how='any', thresh=None, subset=None):
        """
        Drops all rows which have NA values

        Parameters
        ----------
        how : { 'any', 'all' }
            any : if any column has an NA value, drop row
            all : if all the columns have an NA value, drop row
        """
        # Currently we don't support threshold or subset so leave them out for the 0.5 release
        #         thresh : int
        #             require that many non-NA values, else drop row
        #         subset : array-like
        #             considers only the given columns in the check, None means all
        self._table.dropna(how, column_name)
        self.lineage.append(self._table.table_name)


    def fillna(self, column_name, value):
        """
        Fills in the NA with given value

        Parameters
        ----------
        column_name : String
            name of column for the function
        value : Imputation
            the fill value
        """
        self._table.fillna(column_name, value)
        self.lineage.append(self._table.table_name)

    def impute(self, column_name, how):
        """
        Fills in NA values with imputation

        Parameters
        ----------
        column_name : String
            name of column for the function
        how : Imputation
            the imputation operation
        """
        # Imputation will be an enumeration of supported operations, like
        # Imputation.AVG or something
        self._table.impute(column_name, how)
        self.lineage.append(self._table.table_name)


        # just use our String/Math Manip functions for now
        #def replace(self, column_name, to_replace):
        #"""
        #Parameters
        #----------
        #column_name : String
        #    name of column for replace
        #how : Imputation
        #    the imputation operation
        #"""
        #print "Not implemented"

    def get_graphbuilder(self, graph_type):
        """
        Returns a graphbuilder for this BigDataFrame

        Parameters
        ----------
        graph_type : GraphTypes.*
            Class indicating the type of graph, like GraphTypes.Property
            or GraphTypes.Bipartite
        """
        return self._graphbuilder_factory_class.\
            get_graphbuilder(graph_type, self.table)


