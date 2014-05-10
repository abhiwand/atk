from intelanalytics.core.types import supported_types


class Row(object):

    def __init__(self, schema, data=None):
        """
        Expects schema to be a OrderedDict
        """
        self.schema = schema
        self.data = [] if data is None else data  # data is an array of strings right now

    def __getattr__(self, name):
        if name != "_schema" and name in self.schema.keys():
            return self[name]
        return super(Row, self).__getattribute__(name)

    def __getitem__(self, key):
        try:
            if isinstance(key, slice):
                raise TypeError("Index slicing a row is not supported")
            if isinstance(key, list):
                return [self._get_cell_value(k) for k in key]
            return self._get_cell_value(key)
        except KeyError:
            raise KeyError("Column name " + str(key) + " not present.")

    def _is_cell_empty(self, column):
        # todo - flesh this out according to the schema (what is considered 'empty' for each dtype?)
        #t = self.schema[column]
        return self._get_cell_value(column) is None

    def is_empty(self, choice, subset=None):
        """
        Determines if the row has empty cells, according to column data type

        Parameters
        ----------
        choice : any, all, str
            Indicates the cell candidates.  any means if any cell in the row is empty, return True.
            all means only if all the cells in the row are empty will the method return True.
            A single column name be also be used, and the answer depends only on that column

        subset : list of str, optional
            The column names of the cells to consider.  If specified, choice must be any or all

        Returns
        -------
        result : bool
            Whether the row, cell, or cell set is empty

        >>> row = Row()

        Examples
        --------
        >>> row.is_empty('a')
        >>> row.is_empty(any)
        >>> row.is_empty(all)
        >>> row.is_empty(any, ['a', 'b'])
        >>> row.is_empty(all, ['a', 'b'])
        """
        if isinstance(choice, basestring):
            return self._is_cell_empty(choice)
        elif choice is any or choice is all:
            subset = self.schema.keys() if subset is None else subset
            return choice(map(self._is_cell_empty, subset))
        else:
            raise ValueError("Bad choice; must be any, all, or a column name")

    def _get_cell_value(self, key):
        # converts the string into the proper data type
        index = self.schema.keys().index(key)  # could improve speed here...
        dtype = self.schema[key]
        return supported_types.cast(self.data[index], dtype)

