import abc

class FrameBuilder(object):
    """
   Abstract class for the various table builders to inherit
   """
    __metaclass__ = abc.ABCMeta

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
    def build_from_json(self, file):
        """
        Reads JSON (www.json.org) file and loads into a table

        Parameters
        ----------
        file : string
            path to file

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




