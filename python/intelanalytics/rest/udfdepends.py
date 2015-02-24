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

        Notes
        -----
        The files to install can be:
        either local python scripts without any packaging structure
        e.g.: ia.Udf.install(['my_script.py']) where my_script.py is in the same local folder
        or
        absolute path to a valid python package/module which includes the intended python file and all its
        dependencies.
        e,g: ia.Udf.install(['testcases/auto_tests/my_script.py'] where it is essential to install the whole 'testcases'
        module on the worker nodes as 'my_script.py' has other dependencies in the 'testcases' module. There are certain
        pitfalls associated with this approach.
        In this case all the folders, subfolders and files within 'testcases' directory get zipped, serialized and
        copied over to each of the worker nodes every time that the user calls a function that uses UDFs.

        So, to keep the overhead low, it is strongly advised that the users make a separate 'utils' folder and keep all
        their python libraries in that folder and simply install that folder to the workers.
        Also, when you do not need the dependencies for future function calls, you can prevent them from getting copied
        over every time by doing ia.Udf.install([]), to empty out the install list.

        :param dependencies: the file dependencies to be serialized to the cluster
        :return: nothing
        """
        if dependencies is not None:
            intelanalytics.rest.spark.UdfDependencies = dependencies
        else:
            raise ValueError ("The dependencies list to be installed on the cluster cannot be empty")

    @staticmethod
    def list():
        """
        lists all the user files getting copied to the cluster
        :return: list of files to be copied to the cluster
        """
        return intelanalytics.rest.spark.UdfDependencies