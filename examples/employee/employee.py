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

graph.vertex_count()
graph.edge_count()
graph.vertices['Employee'].inspect(9)
graph.edges['worksunder'].inspect(20)

#Option 2

ia.drop_graphs(graph)
graph = ia.Graph()
graph.define_vertex_type('Employee')
graph.define_edge_type('worksunder', 'Employee', 'Employee', directed=False)
graph.vertices['Employee'].add_vertices(employees_frame, 'Employee', ['Title'])
graph.edges['worksunder'].add_edges(employees_frame, 'Employee', 'Manager', ['Years'], create_missing_vertices = True)

graph.vertex_count()
graph.edge_count()
graph.vertices['Employee'].inspect(9)
graph.edges['worksunder'].inspect(20)