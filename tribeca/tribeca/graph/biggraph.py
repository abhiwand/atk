
#-------------------------------------------------------------------------
# Graphs
#-------------------------------------------------------------------------

class BigGraph(object):
    pass

import abc
class GraphBuilder(object):
    """
    Abstract class for the various graph builders to inherit
    (not to be confused by the Tribeca "GraphBuilder" product or component)
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def build(self):
        pass
