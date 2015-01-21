
#=====================
#Process Flow Examples
#=====================

import intelanalytics as ia
ia.connect()
ia.errors.show_details = True


for name in ia.get_frame_names():
    print 'deleting frame: %s' %name
    ia.drop_frames(name)


for name in ia.get_graph_names():
    print 'deleting graph: %s' %name
    ia.drop_graphs(name)


print ia.valid_data_types


#=====================
#Importing Data
#=====================

schema_ab = [('a', ia.int32), ('b', str)]
my_csv = ia.CsvFile('datasets/small_songs.csv', schema_ab)

#schema_2 = [('column_a', str), ('', ia.ignore), ('more_data', str)] #we do not support the ignore type.

my_csv = ia.CsvFile('datasets/small_songs.csv', schema_ab)
csv1 = ia.CsvFile(file_name="data.txt", schema=schema_ab)
csv2 = ia.CsvFile(file_name="more_data.txt", schema=schema_ab)
#csv3 = ia.CsvFile("different_data.txt", schema=[('x', ia.float32), ('', ia.ignore), ('y', ia.int64)])

raw_csv_data_file = "datasets/my_data.csv"
column_schema_list = [("x", ia.float64), ("y", ia.float64), ("z", str)]
csv4 = ia.CsvFile(raw_csv_data_file, column_schema_list, delimiter='|', skip_header_lines=2)


#=====================
#BigFrame
#=====================

f = ia.Frame()

my_frame = ia.Frame(csv4, 'myframe')

f2 = my_frame.copy()
f2.name = "copy_of_myframe"

f3 = my_frame.copy(['x', 'z'])
f3.name = "copy_of_myframe2"

f4 = my_frame.copy(['x', 'z'], where = (lambda row: "TRUE" in row.z))
f4.name = "copy_of_myframe_true"



small_songs = ia.Frame(my_csv, name = "small_songs")
small_songs.inspect()
small_songs.get_error_frame().inspect()


#Append

objects1 = ia.Frame(ia.CsvFile("datasets/objects1.csv", schema=[('Object', str), ('Count', ia.int64)], skip_header_lines=1), 'objects1')
objects2 = ia.Frame(ia.CsvFile("datasets/objects2.csv", schema=[('Thing', str)], skip_header_lines=1), 'objects2')

objects1.inspect()
objects2.inspect()

objects1.append(objects2)
objects1.inspect()


#=====================
#Inspect the Data
#=====================

objects1.row_count

len(objects1.schema)

objects1

print objects1.inspect(2)

subset_of_objects1 = objects1.take(3, offset=2)
print subset_of_objects1


animals = ia.Frame(ia.CsvFile("datasets/animals.csv", schema=[('User', ia.int32), ('animals', str), ('Int1', ia.int64), ('Int2', ia.int64), ('Float1', ia.float64), ('Float2', ia.float64)], skip_header_lines=1), 'animals')
animals.inspect()
freq = animals.top_k('animals', animals.row_count)
freq.inspect(freq.row_count)

from pprint import *
summary = {}
for col in ['Int1', 'Int2', 'Float1', 'Float2']:
    summary[col] = animals.column_summary_statistics(col)
    pprint(summary[col])



#=====================
#Data Cleaning
#=====================

def clean_animals(row):
    if 'basset hound' in row.animals:
        return 'dog'
    elif 'ginea pig' in row.animals:
        return 'guinea pig'
    else:
        return row.animals

animals.add_columns(clean_animals, ('animals_cleaned', str))

animals.drop_columns('animals')

animals.rename_columns({'animals_cleaned' : 'animals'})

#=====================
#Drop Rows
#=====================

animals.drop_rows(lambda row: row['Int2'] < 0)

#=====================
#Drop Duplicates
#=====================

animals.drop_duplicates(['User', 'animals'])

animals.inspect(animals.row_count)


#=====================
#Transform the Data
#=====================

animals.add_columns(lambda row: row.Int1*row.Int2, ('Int1xInt2', ia.float64))

animals.add_columns(lambda row: 1, ('all_ones', ia.int64))

animals.add_columns(lambda row: row.Float1 + row.Float2, ('Float1PlusFloat2', ia.float64))
summary['Float1PlusFloat2'] = animals.column_summary_statistics('Float1PlusFloat2')
#pprint(summary['Float1PlusFloat2'])

def piecewise_linear_transformation(row):
    x = row.Float1PlusFloat2
    if x is None:
        return None
    elif x < 50:
        m, c =0.0046, 0.4168
    elif 50 <= x < 81:
        m, c =0.0071, 0.3429
    elif 81 <= x:
        m, c =0.0032, 0.4025
    else:
        return None
    return m * x + c

animals.add_columns(piecewise_linear_transformation, ('PWL', ia.float64))

animals.add_columns(lambda row: [abs(row.Int1), abs(row.Int2)], [('Abs_Int1', ia.int64), ('Abs_Int2', ia.int64)])


#=====================
#Group by (and aggregate)
#=====================

grouped_animals = animals.group_by('animals', {'Int1': [ia.agg.avg, ia.agg.sum, ia.agg.stdev], 'Int2': [ia.agg.avg, ia.agg.sum]})
grouped_animals.inspect()

grouped_animals2 = animals.group_by(['animals', 'Int1'], {'Float1': [ia.agg.avg, ia.agg.stdev, ia.agg.var, ia.agg.min, ia.agg.max], 'Int2': [ia.agg.count, ia.agg.count_distinct]})

grouped_animals2 = animals.group_by(['animals', 'Int1'], ia.agg.count)



#=====================
#Join
#=====================

my_frame = ia.Frame(ia.CsvFile("datasets/my_frame.csv", schema=[('a', str), ('b', str), ('c', str)]), 'my_frame')
your_frame = ia.Frame(ia.CsvFile("datasets/your_frame.csv", schema=[('b', str), ('c', str), ('d', str)]), 'your_frame')

our_inner_frame = my_frame.join(your_frame, 'b', how='inner')

our_outer_frame = my_frame.join(your_frame, 'b', how='outer')

our_left_frame = my_frame.join(your_frame, 'b', how='left')

our_right_frame = my_frame.join(your_frame, left_on = 'b', right_on = 'd', how = 'right')

our_outer_frame.inspect()

def add_categories(row):
    if row.a is None:
        return 'No Category'
    elif 'alligator' in row.a:
        return 'animal,reptile'
    elif 'mirror' in row.a:
        return 'object'
    elif 'apple' in row.a:
        return 'plant,fruit,red'
    else:
        return 'No Category'


our_outer_frame.add_columns(add_categories, ('category', str))
flattened_frame = our_outer_frame.flatten_column('category')
flattened_frame.inspect()


#=====================
#TitanGraph
#=====================

employees_frame = ia.Frame(ia.CsvFile("datasets/employees.csv", schema = [('Employee', str), ('Manager', str), ('Title', str), ('Years', ia.int64)], skip_header_lines=1), 'employees_frame')

employees_frame.inspect()

employees_vertices = ia.VertexRule("Employee_Name", employees_frame['Employee'], {'Title':employees_frame.Title})

managers_vertices = ia.VertexRule("Manager_Name", employees_frame.Manager)

reports = ia.EdgeRule("worksunder", employees_vertices, managers_vertices, {'Years': employees_frame.Years}, bidirectional = True)

my_graph = ia.TitanGraph([employees_vertices, managers_vertices, reports], "employee_graph")

my_graph.query.gremlin("g.V[0..8]")

my_graph.query.gremlin("g.E[0..16]")


#=====================
#Graph
#=====================

#A bipartite graph 
#Notice that this is a funny example since managers are also employees!
#Preseuambly Steve the manager and Steve the employee are the same person

graph = ia.Graph('parquet_graph1')
graph.define_vertex_type('Employee')
graph.define_vertex_type('Manager')
graph.define_edge_type('worksunder', 'Employee', 'Manager', directed=False)

graph.vertices['Employee'].add_vertices(employees_frame, 'Employee')
graph.vertices['Manager'].add_vertices(employees_frame, 'Manager')
graph.edges['worksunder'].add_edges(employees_frame, 'Employee_Name', 'Manager_Name', ['Years'])

graph.vertex_count()
graph.edge_count()
graph.vertices['Employee'].inspect(8)

#A unipartite graph
#Here, we fix the previous oddity by specifying that managers are also employees
#But we don't have a title for Rob (maybe he's the CEO since he doesn't report to anyone)
#Notice that this also builds the graph incorrecty! This is because the manager vertex rule
#overwrites the employee vertex rule. Both Rob and David are missing title, but we do
#know David's title

graph2 = ia.Graph('parquet_graph2')
graph2.define_vertex_type('Employee')
graph2.define_edge_type('worksunder', 'Employee', 'Employee', directed=False)
graph2.vertices['Employee'].add_vertices(employees_frame, 'Employee', ['Title'])
graph2.vertices['Employee'].add_vertices(employees_frame, 'Manager', [])
graph2.edges['worksunder'].add_edges(employees_frame, 'Employee', 'Manager', ['Years'])

graph2.vertex_count()
graph2.edge_count()
graph2.vertices['Employee'].inspect(9)

#A unipartite graph con't
#There are two ways to fix the above example 1) we do the manager rule first so it doesn't
#overwrite the employee rule or 2) we omit the manager rule entirely, which will create 
#dangling edges, then specify that missing vertices should be added in. 

#Option 1

ia.drop_graphs(graph2)
graph2 = ia.Graph('parquet_graph3')
graph2.define_vertex_type('Employee')
graph2.define_edge_type('worksunder', 'Employee', 'Employee', directed=False)
graph2.vertices['Employee'].add_vertices(employees_frame, 'Manager', [])
graph2.vertices['Employee'].add_vertices(employees_frame, 'Employee', ['Title'])
graph2.edges['worksunder'].add_edges(employees_frame, 'Employee', 'Manager', ['Years'])

graph2.vertex_count()
graph2.edge_count()
graph2.vertices['Employee'].inspect(9)

#Option 2

ia.drop_graphs(graph2)
graph2 = ia.Graph()
graph2.define_vertex_type('Employee')
graph2.define_edge_type('worksunder', 'Employee', 'Employee', directed=False)
graph2.vertices['Employee'].add_vertices(employees_frame, 'Employee', ['Title'])
graph2.edges['worksunder'].add_edges(employees_frame, 'Employee', 'Manager', ['Years'], create_missing_vertices = True)

graph2.vertex_count()
graph2.edge_count()
graph2.vertices['Employee'].inspect(9)

