from collections import OrderedDict


class Row(OrderedDict):

    def __init__(self, frame, *args, **kwargs):
        self.frame = frame
        super(Row).__init__(*args, **kwargs)

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

        Examples
        --------
        >>> row.is_empty('a')
        >>> row.is_empty(any)
        >>> row.is_empty(all)
        >>> row.is_empty(any, ['a', 'b'])
        >>> row.is_empty(all, ['a', 'b'])
        """
        # todo - implement
        raise NotImplementedError()
