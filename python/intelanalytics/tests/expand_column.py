

import intelanalytics as ia

def expand_column(frame, column_name):

    s = frame.take(1, columns=[column_name])[0]
    count = len(s.split(','))

    schema = [((column_name + "_%d" % i), ia.float64) for i in xrange(count)]
    col_name = column_name

    def ac(row):
       return  [row[col_name].split(',')]

    frame.add_columns(ac, schema)

