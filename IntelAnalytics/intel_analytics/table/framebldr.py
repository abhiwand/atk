import abc

class FrameBuilder(object):
    """
   Abstract class for the various table builders to inherit.
   """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def build_from_csv(self, file, schema=None, skip_header=False):
        """
        Reads CSV (comma-separated-value) file and loads into a table.

        Parameters
        ----------
        file : string
            The path to the file.
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
        Reads a JSON (www.json.org) file and loads it into a table.

        Parameters
        ----------
        file : string
            The path to the file.
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
        Reads an XML file and loads it into a table.

        Parameters
        ----------
        file : string
            The path to the file.
        schema : string
            TODO:

        TODO: others parameters for the parser

        Returns
        -------
        frame : BigDataFrame
        """
        pass




