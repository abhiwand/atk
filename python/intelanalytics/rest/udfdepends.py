import os
import sys
import intelanalytics.rest.spark

class Udf(object):
    """
    An object for containing  and managing all the UDF dependencies.

    """
    @staticmethod
    def install(dependencies):
        """
        installs the specified files on the cluster
        :param dependencies: the file dependencies to be serialized to the cluster
        :return: nothing
        """
        if dependencies is not None:
            intelanalytics.rest.spark.UdfDependencies = dependencies
        else:
            raise ValueError ("The dependencies list to be installed on the cluster cannot be empty")

    @staticmethod
    def uninstall():
        """
        removes the specified files from the cluster
        :return:
        """
        raise NotImplementedError

    @staticmethod
    def list():
        """
        lists all the user files on the cluster
        :return:
        """
        raise NotImplementedError


