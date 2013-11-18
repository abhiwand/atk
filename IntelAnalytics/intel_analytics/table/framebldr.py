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
        pass
    @abc.abstractmethod
    def build_from_json(self, file, schema=None):
        pass
    @abc.abstractmethod
    def build_from_xml(self, file, schema=None):
        pass


class HBaseFrameBuilder(FrameBuilder):
    def __init__(self, table):
        super(HBase2TitanBipartiteGraphBuilder, self).__init__(table)


    def build_from_csv(self, file, schema=None, skip_header=False):
        pass
    def build_from_json(self, file, schema=None):
        pass
    def build_from_xml(self, file, schema=None):
        pass


