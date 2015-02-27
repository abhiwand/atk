##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2015 Intel Corporation All Rights Reserved.
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

import intelanalytics as ia
ia.connect()

for name in ia.get_frame_names():
    print 'deleting frame: %s' %name
    ia.drop_frames(name)


employees_frame = ia.Frame(ia.CsvFile("employees.csv", schema = [('Employee', str), ('Manager', str), ('Title', str), ('Years', ia.int64)], skip_header_lines=1), 'employees_frame')

employees_frame.inspect()

#A bipartite graph
#Notice that this is a funny example since managers are also employees!
#Preseuambly Steve the manager and Steve the employee are the same person

#Option 1

graph = ia.Graph()
graph.define_vertex_type('Employee')
graph.define_edge_type('worksunder', 'Employee', 'Employee', directed=False)
graph.vertices['Employee'].add_vertices(employees_frame, 'Manager', [])
graph.vertices['Employee'].add_vertices(employees_frame, 'Employee', ['Title'])
graph.edges['worksunder'].add_edges(employees_frame, 'Employee', 'Manager', ['Years'])

graph.vertex_count
graph.edge_count
graph.vertices['Employee'].inspect(9)
graph.edges['worksunder'].inspect(20)

#Option 2

ia.drop_graphs(graph)
graph = ia.Graph()
graph.define_vertex_type('Employee')
graph.define_edge_type('worksunder', 'Employee', 'Employee', directed=False)
graph.vertices['Employee'].add_vertices(employees_frame, 'Employee', ['Title'])
graph.edges['worksunder'].add_edges(employees_frame, 'Employee', 'Manager', ['Years'], create_missing_vertices = True)

graph.vertex_count
graph.edge_count
graph.vertices['Employee'].inspect(9)
graph.edges['worksunder'].inspect(20)
