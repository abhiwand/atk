import abc

class FrameBuilder(object):
    """
   Abstract class for the various table builders to inherit
   """
    __metaclass__ = abc.ABCMeta

    def __init__(self, table):
        self._table = table

    @abc.abstractmethod
    def build_from_csv(self, file, schema=None, skip_header=False):
        """
        Reads CSV (comma-separated-value) file and loads into a table

        Parameters
        ----------
        file : string
            path to file
        schema : string
            TODO:

        TODO: others parameters for the csv parser

        Returns
        -------
        frame : BigDataFrame
        """
        pass
    @abc.abstractmethod
    def build_from_json(self, file, schema=None):
        """
        Reads JSON (www.json.org) file and loads into a table

        Parameters
        ----------
        file : string
            path to file
        schema : string
            TODO:

        TODO: others parameters for the parser

        Returns
        -------
        frame : BigDataFrame
        """
        pass
    @abc.abstractmethod
    def build_from_xml(self, file, schema=None):
        """
        Reads XML file and loads into a table

        Parameters
        ----------
        file : string
            path to file
        schema : string
            TODO:

        TODO: others parameters for the parser

        Returns
        -------
        frame : BigDataFrame
        """
        pass




