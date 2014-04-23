##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2014 Intel Corporation All Rights Reserved.
#
# The source code contained or described herein and all documents related to
# the source code (Material) are owned by Intel Corporation or its suppliers
# or licensors. Title to the Material remains with Intel Corporation or its
# suppliers and licensors. The Material may contain trade secrets and
# proprietary and confidential information of Intel Corporation and its
# suppliers and licensors, and is protected by worldwide copyright and trade
# secret laws and treaty provisions. No part of the Material may be used,
# copied, reproduced, modified, published, uploaded, posted, transmitted,
# distributed, or disclosed in any way without Intel's prior express written
# permission.
#
# No license under any patent, copyright, trade secret or other intellectual
# property right is granted to or conferred upon you by disclosure or
# delivery of the Materials, either expressly, by implication, inducement,
# estoppel or otherwise. Any license under such intellectual property rights
# must be express and approved by Intel in writing.
##############################################################################

from cloud.serialization.cloudpickle import CloudPickler
import pkg_resources
import types


class IAPickle(CloudPickler):

    def __init__(self, file, protocol=None, min_size_to_save=0):
        CloudPickler.__init__(self, file, protocol, min_size_to_save)
        self.imports_required = set()

    def save_inst(self, obj):
        CloudPickler.save_inst(self, obj)
        self.imports_required.add(obj.__class__.__module__)

    CloudPickler.dispatch[types.InstanceType] = save_inst

    def __extract_module_names(self, module_set):
        modules = set()
        for module in module_set:
            modules.add(module.__name__.split('.')[0])
        return modules

    def get_dependent_modules(self):
        module_dependency = []
        final_set = self.imports_required.union(self.__extract_module_names(self.modules))
        for i in final_set:
            try:
                module_dependency.append("%s %s" % (i, pkg_resources.get_distribution(i).version))
            except:
                module_dependency.append(i)
        return module_dependency
