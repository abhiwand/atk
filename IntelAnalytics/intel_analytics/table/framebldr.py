import abc
from intel_analytics.config import global_config, dynamic_import


__all__ = ['get_frame_builder',
           'get_frame',
           ]


class FrameBuilderFactory(object):
    """
    Abstract class for the various frame builder factories (i.e. one for Hbase)
    """
    __metaclass__ = abc.ABCMeta

    #todo: implement when builder discrimination is required
    def __init__(self):
        pass

    @abc.abstractmethod
    def get_frame_builder(self):
        raise Exception("Not overridden")

    @abc.abstractmethod
    def get_frame(self, frame_name):
        raise Exception("Not overridden")


class FrameBuilder(object):
    """
   Abstract class for the various table builders to inherit
   """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def build_empty(self, name):
        pass

    @abc.abstractmethod
    def build_from_csv(self, filename, schema=None, skip_header=False):
        """
        Reads CSV (comma-separated-value) file and loads into a table

        Parameters
        ----------
        filename : string
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
    def build_from_json(self, filename, schema=None):
        """
        Reads JSON (www.json.org) file and loads into a table

        Parameters
        ----------
        filename : string
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
    def build_from_xml(self, filename, schema=None):
        """
        Reads XML file and loads into a table

        Parameters
        ----------
        filename : string
            path to file
        schema : string
            TODO:

        TODO: others parameters for the parser

        Returns
        -------
        frame : BigDataFrame
        """
        pass


def get_frame_builder():
    """
    Returns a frame_builder with which to create BigDataFrame objects
    """
    factory_class = _get_frame_builder_factory_class()
    return factory_class.get_frame_builder()


def get_frame(frame_name):
    """
    Returns a previously created frame
    """
    factory_class = _get_frame_builder_factory_class()
    return factory_class.get_graph(frame_name)


# dynamically and lazily load the correct frame_builder factory,
# according to config
_frame_builder_factory = None


def _get_frame_builder_factory_class():
    global _frame_builder_factory
    if _frame_builder_factory is None:
        frame_builder_factory_class = dynamic_import(
            global_config['py_frame_builder_factory_class'])
        _frame_builder_factory = frame_builder_factory_class.get_instance()
    return _frame_builder_factory
