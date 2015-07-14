
import sys
import os
pycli = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
print pycli
sys.path.insert(0, pycli)

def print_frame():
    pass


import taprootanalytics as ta
from taprootanalytics.core.iatypes import is_type_float, is_type_unicode
from taprootanalytics.rest.frame import FrameSchema

spaces_between_cols = 2  # const

def get_validated_none_or_int_gt_zero(name, value):
    if value is not None:
        if not isinstance(value, int):
            raise TypeError('%s argument must be an integer, got %s' % (name, type(value)))
        if value <= 0:
            raise ValueError('%s argument must be an integer > 0, got %s' % (name, value))
    return value

def _get_col_sizes(rows, row_index, schema, wrap):
    #headers =["%s:%s" % (name, data_type) for name, data_type in FrameSchema.from_types_to_strings(schema)]
    headers =["%s" % name for name, data_type in schema]
    sizes = [len(h) for h in headers]
    #sizes = [len("%s:%s" % (name, data_type)) for name, data_type in FrameSchema.from_types_to_strings(schema)]
    for r in xrange(wrap):
        if r+row_index < len(rows):
            row = rows[r+row_index]
            for c in xrange(len(sizes)):
                entry = unicode(row[c])
                lines = entry.splitlines()
                max = 0
                for line in lines:
                    length = len(line)
                    if length > max:
                        max = length
                #s = len(unicode(row[c]))
                if max > sizes[c]:
                    sizes[c] = max
    return sizes


def _get_num_cols(schema, width, start_col_index, col_sizes):
    # column_clump
    num_cols = 0
    line_length = -spaces_between_cols
    while line_length < width and start_col_index + num_cols < len(schema):
        candidate = col_sizes[start_col_index + num_cols] + spaces_between_cols
        if (line_length + candidate) > width:
            if num_cols == 0:
                num_cols = 1
            break
        num_cols += 1

    return num_cols


def _get_row_clump_count(row_count, wrap):
    return row_count / wrap + (1 if row_count % wrap else 0)


def _get_padded_string_value(value, size):
    return value + ' ' * (size - len(value))


class FrameInspection(object):
    """
    class used specifically for inspect, where the __repr__ is king
    """

    def __init__(self, schema, rows, wrap=None, truncate=None, round=None, width=80, margin=None):
        self.schema = schema
        self.rows = rows
        self.wrap=wrap
        self.truncate = get_validated_none_or_int_gt_zero("truncate", truncate)
        self.round = get_validated_none_or_int_gt_zero("round_floats", round)
        self.width = get_validated_none_or_int_gt_zero("width", width)
        self.margin = get_validated_none_or_int_gt_zero("margin", margin)
        self.value_formatters = [self.get_value_formatter(data_type) for name, data_type in schema]

    def __repr__(self):
        if self.wrap is None or self.wrap == 'stripes':
            return self._stripes()
        return self._wrap()


    # def _column_clump(self, row_count, column_index, col_sizes, start_row_index):
    #     lines_list = []
    #     print "column_index=%s" % column_index
    #     num_cols = _get_num_cols(self.schema, self.width, column_index, col_sizes)
    #     if num_cols == 0:
    #         raise RuntimeError("Internal booboo, num_cols == 0")
    #     #slice_a, slice_b = column_index, column_index + num_cols
    #     #print "slice=[%s:%s]" % (slice_a, slice_b)
    #     header_line = '  '.join([self.format_schema_entry(name, data_type, col_sizes[column_index+i]) for i, (name, data_type) in enumerate(self.schema[column_index:column_index+num_cols])])
    #     thick_line = "=" * len(header_line)
    #     lines_list.extend(["", header_line, thick_line])
    #     # print rows
    #     for r in xrange(start_row_index, start_row_index + self.wrap):
    #         if r < row_count:
    #             lines_list.append('  '.join([self.format_value_entry(data, col_sizes[column_index+i]) for i, data in enumerate(self.rows[r][column_index:column_index+num_cols])]))
    #         else:
    #             break
    #     #print "num_cols = %s" % num_cols
    #     column_index += num_cols
    #     return num_cols, lines_list, num_cols
    #     #print "----------------------------------------------------------"

    @staticmethod
    def _get_row_index_str(index):
        return "[%s]  " % index

    @staticmethod
    def _get_bonus_lines(bonus, col_sizes, pad_str):
        # bonus is a list of tuples of the form (relative column index, [lines])
        # col_sizes is an array of the col_sizes for the 'current' clump (hence
        #   the 'relative column index' in the bonus list --these indices match
        #
        bonus_lines = []  # list of new, full-fledged extra lines that come from the bonus

        def there_are_tuples_in(x):
            return bool(len(x))

        while there_are_tuples_in(bonus):
            bonus_index = 0
            new_line_columns = [pad_str]
            for size_index in xrange(len(col_sizes)):
                if bonus_index < len(bonus) and size_index == bonus[bonus_index][0]:
                    index, lines = bonus[bonus_index]  # the 'tuple'
                    entry = lines.pop(0)
                    new_line_columns.append(_get_padded_string_value(entry, col_sizes[size_index]))
                    if not len(lines):
                        del bonus[bonus_index]  # remove empty tuple, which also naturally moves index to the next tuple
                    else:
                        bonus_index += 1  # move on to the next tuple
                else:
                    new_line_columns.append(" " * (col_sizes[size_index] + spaces_between_cols))

            if len(bonus) != bonus_index:  # sanity check for the while loop
                raise RuntimeError("Infinite loop in _get_bonus_lines")

            bonus_lines.append(''.join(new_line_columns))

        return bonus_lines

    def _wrap(self):
        """print rows in clumps style"""
        row_count = len(self.rows)
        row_clump_count = _get_row_clump_count(row_count, self.wrap)
        lines_list = []
        bonus = []
        for row_clump_index in xrange(row_clump_count):
            start_row_index = row_clump_index * self.wrap
            stop_row_index = start_row_index + self.wrap
            if stop_row_index > row_count:
                stop_row_index = row_count
            col_sizes = _get_col_sizes(self.rows, start_row_index, self.schema, self.wrap)
            column_index = 0
            while column_index < len(self.schema):
                num_cols = _get_num_cols(self.schema, self.width, column_index, col_sizes)
                if num_cols == 0:
                    raise RuntimeError("Internal error, num_cols == 0")  # sanity check on algo
                header_line = self._get_row_index_str('#' * len(str(stop_row_index))) + '  '.join([self.format_schema_entry(name, data_type, col_sizes[column_index+i]) for i, (name, data_type) in enumerate(self.schema[column_index:column_index+num_cols])])
                thick_line = "=" * len(header_line)
                lines_list.extend(["", header_line, thick_line])
                for r in xrange(start_row_index, stop_row_index):
                    lines_list.append(self._get_row_index_str(r) + '  '.join([self.format_value_entry(data, col_sizes[column_index+i], i, bonus) for i, data in enumerate(self.rows[r][column_index:column_index+num_cols])]))
                    if bonus:
                        lines_list.extend(self._get_bonus_lines(bonus, col_sizes[column_index:column_index+num_cols], pad_str=' ' *len(self._get_row_index_str(stop_row_index))))
                column_index += num_cols

            lines_list.append('')  # extra line for new clump

        return "\n".join(lines_list)

    def _stripes(self):
        """print rows as stripes style"""
        max_margin = 0
        for item in self.schema:
            length = len(item[0]) + 1 # to account for the '='
            if length > max_margin:
                max_margin = length
        if not self.margin or max_margin < self.margin:
            self.margin = max_margin
        self.margin += 1  # count 0, such that margin is number of characters to the left of the =
        thick_line = "=" * self.margin
        thin_line = "-" * self.margin
        schema_line = "[%s]" % ', '.join(["%s:%s" % (name, data_type) for name, data_type in FrameSchema.from_types_to_strings(self.schema)])
        lines_list = [schema_line, thick_line]
        for row in self.rows:
            lines_list.extend([self.format_entry(i, name, value) for i, (name, value) in enumerate(zip(map(lambda x: x[0], self.schema), row))])
            lines_list.append(thin_line)

        return "\n".join(lines_list)

    def get_value_formatter(self, data_type):
        if self.round and is_type_float(data_type):
            return self.get_rounder(data_type)
        if self.truncate and is_type_unicode(data_type):
            return self.truncate_string
        return self.identity

    def format_entry(self, i, name, value):
        return "%s=%s" % (self.format_name(name), self.value_formatters[i](value))

    @staticmethod
    def identity(value):
        return value

    def format_name(self, n):
        return n + ' ' * (self.margin - len(n) - 1)

    def format_schema_entry(self, name, data_type, size):
        # from taprootanalytics.rest.iatypes import get_rest_str_from_data_type
        entry = "%s" % name  # :%s" % (name, get_rest_str_from_data_type(data_type))
        if size > self.width:
            size = self.width
        return ' ' * (size - len(entry)) + entry

    def format_value_entry(self, data, size, i, bonus):
        entry = "%s" % data
        if isinstance(data, basestring):
            lines = entry.splitlines()
            if len(lines) > 1:
                entry = lines[0]
                bonus.append((i, lines[1:]))
            return entry + ' ' * (size - len(entry))
        else:
            return ' ' * (size - len(entry)) + entry

    def truncate_string(self, s):
        return s[:self.truncate] + "..."

    def get_rounder(self, float_type):
        def rounder(value):
            return float_type.round(float_type(value), self.round)
        return rounder


f_schema = [('i32', ta.int32),
            ('floaties', ta.float64),
            ('long_column_name_ugh', str),
            ('long_value', str),
            ('s', str)]

f_rows = [
    [1,
     3.14159265358,
     'a',
     '''The sun was shining on the sea,
Shining with all his might:
He did his very best to make
The billows smooth and bright--
And this was odd, because it was
The middle of the night.

The moon was shining sulkily,
Because she thought the sun
Had got no business to be there
After the day was done--
"It's very rude of him," she said,
"To come and spoil the fun!"''',
    'one'],
    [2,
     8.014512183,
     'b',
     '''I'm going down.  Down, down, down, down, down.  I'm going down.  Down, down, down, down, down.  I've got my head out the window and my big feet on the ground.''',
    'two'],
    [32,
     1.0,
     'c',
     'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
     'thirty-two']]

#print FrameInspection(f_schema, f_rows, truncate=30, round=None, margin=12)
#print create_frame_string(schema, rows)

def inspect(wrap=None, truncate=None, round=None, width=80, margin=None):
    print FrameInspection(f_schema, f_rows, wrap, truncate=truncate, round=round, width=width, margin=margin)

#inspect(wrap=1)
#inspect(wrap=2)
inspect(wrap=1)
