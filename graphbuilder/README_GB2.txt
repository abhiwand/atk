Graphbuilder 2 : the graph building boogaloo.
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

demo apps (by name of jar file) and their command lines

name: CreateLinkGraph

what it does: reads in wikipedia data set and creates a link graph in the
legacy output format of separate text files for edges and vertices

arguments:
-i <input file path>
-o <output file path>

name: CreateWordCountGraph

what it does: reads in wikipedia data set and creates a page/word incidence
graph with the edge annoted by word frequency

arguments:
-i <input file path>
-o <output file path>

name: hbaseToLegacyOutput

what it does:  reads an hbase table (hbase must be started and running) 
               extracts a graph based on user provided parsing rules
               sends to legacy output of separate text files for edges and
               vertices

arguments:
-t <table name> : name of hbase table used as input

-o <output name> : path for output file

-v <vertex parsing expression>
          example: -v "cf:name=cf:age,cf:dept"
-e <edge parsing expression> 
           example:  -e "cf:name,cf:dept,worksAt"


