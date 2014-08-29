debug = False

"""
This program is to build a .csv file and fill it with random data.
First, set up any functions needed to support the main program
"""

################################################################################

def acceptable_types(unicode_format, allow_bool):
    """
    Create a list of types of variables allowed.

    Parameters
    ----------
    unicode_format : str
        The unicode format string if defined. Can be none.
    allow_bool : bool
        True if boolean column types are permitted

    Returns
    -------
    list
        A list of valid types

    Examples
    --------
    >>>
    for this example, we call the function with None and None
    >>> ty = acceptablee_types(None, None)
    ty is now an array equal to only valid types

    """
    valid_types = ['int', 'int32', 'int64', 'float', 'float64', 'float32', 'str']
    if unicode_format:
        valid_types.append('unicode')
    if allow_bool:
        valid_types.append('bool')
    return valid_types
    # end of def acceptable_types

################################################################################

def build_columns(file_name, number_columns, column_names, column_types, valid_types, talkative):
    """
    Build a list of column definitions.

    Structure of elements is [(name, type), (name,type), ...]

    Parameters
    ----------
    file_name : str
        The base name for the file holding the final types
    number_columns : int
        The number of columns of data
    column_names : string or array of strings or None
        The names to give the columns
    column_types : string or array of strings or None
        The types to assign the columns
    valid_types : array of strings
        An array of valid types to assign columns
    talkative : bool
        A flag to indicate a verbose response

    Returns
    -------
    list of strings
        A list detailing the column names and types

    Notes
    -----
    * if there are more than one column, using a string (or list with a single string element) for the 'column_names' will cause the routine to use it as a prefix to numbered columns. For example, entering 'a' will result in columns 'a0001', 'a0002', for as many columns as there are.
    * Using a single type for column_types will cause all columns to have the specified data type.

    Examples
    --------
    >>>
    >>> fn = "work"
    >>> ty = acceptable_types('U+00000', True)
    ty is now ['int', 'int32', 'int64', 'float', 'float64', 'float32', 'str', 'unicode', 'bool']
    >>> ve = args.verbose
    >>> bc = build_columns(fn, 100, None, None, ty, ve)
    bc is now something like: [("col_001", 'int'), ("col_002", 'str'), ... ("col_100", 'float')]
    The file 'work.nam' exists and looks like "col_001, col_002, ... col_100"
    The file 'work.typ' exists and looks like "int, str, ... float"
    <BLANKLINE>
    >>> cn = "C_"
    >>> bc = build_columns(fn, 100, cn, None, ty, ve)
    bc is now something like: [("C_001", 'str'), ("C_002", 'int64'), ... ("C_100", 'float')]
    The file 'work.nam' exists and looks like "C_001, C_002, ... C_100"
    The file 'work.typ' exists and looks like "str, int64, ... float"
    <BLANKLINE>
    >>> cnl = ['ID', 'NAME']
    >>> tpl = ['int', 'str']
    >>> bc = build_columns(fn, 22, cnl, tpl, ty, ve)
    bc is now [('ID', 'int'), ('NAME', 'str')]
    The file 'work.nam' exists and looks like "ID, NAME"
    The file 'work.typ' exists and looks like "int, str"
    <BLANKLINE>
    >>> tp = 'bool'
    >>> bc = build_columns(fn, 100, None, tp, ty, ve)
    bc is now something like: [("col_001", 'bool'), ("col_002", 'bool'), ... ("col_100", 'bool')]
    The file 'work.nam' exists and looks like "col_001, col_002, ... col_100"
    The file 'work.typ' exists and looks like "bool, bool, ... bool""

    """
    working_columns = []
    if debug: print "Building columns: col_names=>{}< type is {}".format(column_names, type(column_names))
    if debug: print "Building columns: col_types=>{}< type is {}".format(column_types, type(column_types))
    if debug: print "Building columns: cols=>{}< type is {}". format(number_columns, type(number_columns))
    # the following two 'if' statements are a patch because max is temporarily broken
    if len(column_names) > number_columns:
        number_columns = len(column_names)
    if column_types and len(column_types) > number_columns:
        number_columns = len(column_types)
    # check to see if the list of column names is just a prefix for the column names
    col_prefix = None
    if number_columns != 1 and len(column_names) == 1:
        col_prefix = column_names[0]
    # check to see if the list of column types is a constant, so all columns would be that type
    col_type = None
    if column_types and len(column_types) == 1:
        col_type = column_types[0]
    if talkative:
        print "Generating columns:"
    # create a list of columns
    column_pointer = 0
    while column_pointer < number_columns:
        if col_prefix:
            form = "{0}{1:0" + "{}".format(int(numpy.log10(number_columns) + 1)) + "d}"
            if debug and column_pointer == 0: print "Column form is '{}'".format(form)
            col_name = form.format(col_prefix, column_pointer+1)
        else:
            col_name = column_names[column_pointer]
        if col_type:
            working_columns.append((col_name, col_type))
        else:
            if column_types and len(column_types) > 1:
                working_columns.append((col_name, column_types[column_pointer]))
            else:
                working_columns.append((col_name, random.choice(valid_types)))
        if talkative:
            print working_columns[column_pointer],
        column_pointer += 1
        # end of while column_pointer < number_columns:
    if talkative:
        print
    """
    open/overwrite the output file
    write a line stating variable type
    write a line stating column names
    """
    f_names = open_for_write(file_name + ".nam", talkative)
    f_types = open_for_write(file_name + ".typ", talkative)
    add_comma = False
    comma = ", "
    for c in working_columns:
        if add_comma:
            f_names.write(comma)
            f_types.write(comma)
        add_comma = True
        f_names.write(c[0])
        f_types.write(c[1])
    f_names.write("\n")
    f_types.write("\n")
    f_names.close()
    f_types.close()
    return working_columns
    # end of def build_columns

################################################################################

def build_cols_from_file(f_name, talkative, valid_types=None):
    """
    Read column names or types from a file.

    Parameters
    ----------
    f_name : str
        The name of the file to be read, without extension
    talkative : bool
       A flag to indicate a verbose response
    valid_types : list of strings
        The currently valid types of data

    Returns
    -------
    list or None
        A list of strings representing the names of the columns.
        If something goes wrong, returns None

    Examples
    --------
    >>>
    For this example, we have a file with the names of the columns, separated by commas
    >>> fn = "columns"
    >>> ve = args.verbose
    >>> nas = build_cols_from_file(fn, ve)
    nas now looks something like: ['col_1', 'col_2']
    <BLANKLINE>
    For this example, we have a file with the types of the columns, separated by commas
    >>> fn = "columns"
    >>> ve = args.verbose
    >>> vt = ['int', 'float']
    >>> nas = build_cols_from_file(fn, vt, ve)
    nas now looks something like: ['int', 'float']

    """
    if valid_types:
        extension = ".typ"
    else:
        extension = ".nam"
    if f_name:
        if talkative:
            print "Reading column info from {}".format(f_name + extension)
        w_list = []
        val = ""
        f = open_for_read(f_name + extension, talkative)
        if f:
            line = f.readline().strip(' ')
            while len(line) > 1:
                line = line.strip()
                string_pointer = 0
                if debug: print "Read: {}".format(line)
                while string_pointer <= len(line):
                    if debug: print string_pointer,
                    if string_pointer == len(line) or line[string_pointer] == ',':
                        if not valid_types or val in valid_types:
                            w_list.append(val)
                            val = ""
                        elif val not in valid_types:
                            print "\nMalformed list of Column Types\nType '{}' detected\nThe types supported are: {}".format(val, valid_types)
                            print "Proceeding with default types\n"
                            line = ""
                            w_list = []
                            val = ""
                    elif line[string_pointer] in string.whitespace:
                        # ignore whitespace
                        pass
                    elif line[string_pointer] in string.printable:
                        val += line[string_pointer]
                    else:
                        print "\nInvalid character '{}' found in file {}".format(line[string_pointer], f_name + extension)
                        print "Proceeding without character\n"
                    string_pointer += 1
                    # end of while string_pointer <= len(line):
                line = f.readline().strip(' ')
                # end of while len(line) > 1:
            f.close()
            if debug: print "Column names read: {}.".format(w_list)
            if len(w_list) > 0:
                if debug: print "w_list = {}".format(w_list)
                return w_list
            #end of if f:
        # end of if f_names:
    return None
    # end of def build_cols_from_names

################################################################################

def build_rows(fid, working_columns, comma, quote, number_rows, valid_types, unicode_format, allow_commas, float_type, percent_error, talkative):
    """
    Create rows of data and write it to the file.

    Parameters
    ----------
    fid : str
        Name of the output file, including extension
    working_columns : list of tuple
        Definition of columns
    comma : str
        Character to be used as a delimiter
    quote : str
        Character(s) to be used to surround strings and unicode
    number_rows : int32
        Number of rows of data to generate
    valid_types : list of str
        Column data types
    unicode_format : str / None
        The format of the unicode fields
    allow_commas : bool
        Allow delimiters in string and unicode columns
    float_type : bool
        True to use scientific notation for float data types
    percent_error : float
        Probability of error in the data
    talkative : bool
        A flag to indicate a verbose response

    Examples
    --------
    >>> build_rows("data.csv", [('col_1', 'str'), ('col_2', 'float64')], ',', ':', 5000, ['str', 'float64'], None, True, False, 0.1, False)
    This creates a file called "data.csv" with two columns and 5000 rows of data. The first column should be all strings beginning and ending with ':'. The second row should be all float64 values and are not shown in scientific notation. There is a 1 in 1000 chance that anything could be wrong with the data.

    """
    if debug: print "build_rows with", fid, working_columns, comma, quote, number_rows, valid_types, unicode_format, allow_commas, float_type, percent_error, talkative
    fid = open_for_write(fid, talkative)
    if fid:
        row_index = 0
        last_print = 0
        print
        while row_index < number_rows:
            add_comma = False
            for c in working_columns:
                """
                check to see if this data type is to be corrupted
                """
                if percent_error >= 100*random.random():
                    random_types = valid_types
                    col_type = random.choice(random_types.append('skip'))
                else:
                    col_type = c[1]
                """
                process the various data types
                """
                if col_type == 'int':
                    val = random_int()
                elif col_type == 'int32':
                    val = random_int32()
                elif col_type == 'int64':
                    val = random_int64()
                elif col_type == "float":
                    val = random_float(float_type)
                elif col_type == "float32":
                    val = random_float32(float_type)
                elif col_type == "float64":
                    val = random_float64(float_type)
                elif col_type == 'str':
                    val = random_string(quote, comma, allow_commas, percent_error)
                elif col_type == 'unicode':
                    val = random_unicode(unicode_format, quote, comma, allow_commas, percent_error)
                elif col_type == 'bool':
                    val = random.choice(["True", "False"])
                else:
                    val = ""

                # add a comma to indicate another field is coming
                if add_comma and percent_error <= 10000*random.random():
                    fid.write(comma)

                # write the data to the file
                if percent_error <= 1000.0*random.random():
                    fid.write('{}'.format(val))
                add_comma = True
                # end of for c in working_columns:

            fid.write("\n")
            row_index += 1
            if talkative and number_rows > 9:
                percent_complete = int(100.0 * row_index / number_rows)
                step_value = 10.0
                if debug: print "Row: {}, Step: {}, Printed: {}, % Complete: {}".format(row_index, step_value, last_print, percent_complete)
                if percent_complete >= int(last_print + step_value):
                    last_print += int(step_value)
                    print "{}%".format(last_print)
            # end of while row_index < number_rows:

        if talkative:
            print "\nFile complete.\n"
    #end of def build_rows

################################################################################

def calculate_float(base, precision, scientific_notation):
    """
    Calculate a random float value.

    Parameters
    ----------
    base : int
        16, 32, or 64 depicting upper range of number
    precision : int
        number of digits of precision
    Scientific_notation : bool
        True to return number in scientific notation

    Returns
    -------
    str
        A float number as a string

    Examples
    --------
    >>> calculate_float(16, 6, False)
    could return values such as:
    5351.600000
      -0.292454
       1.337418
       0.028898
      -0.108681
     -13.850325
      -2.444748
    >>> calculate_float(64, 20, True)
    could return values such as:
      1.855290160742823379e2
      1.071828856758441084e8
     -1.772037209408281688e8
    -1.926474631005816062e13
     6.892375019737539076e2
     3.851431090568648407e17
     7.117330082027846671e13

    """
    numerator = float(random.randrange(1 - (2 ** base - 1), 2 ** base - 1))
    denominator = float(random.randrange(1,10 ** (1 + random.randrange(precision))))
    if denominator != 0:
        num = numerator/denominator
    else:
        num = 0
    if scientific_notation:
        return scientific_format(num, precision)
    else:
        form = "{}0:.{}f{}".format('{', precision, '}')
        return form.format(num)
    # end of def calculate_float

################################################################################

def check_fatals(params):
    """
    check the inputs

    Parameters
    ----------
    params : <class 'argparse.Namespace'>
        A class containing all the arguments passed to the program from the command line

    Notes
    -----
    If there are any problems, the program will quit

    """
    if debug: print "Check_fatals({})".format(params)
    if debug: print type(params)
    if params.extension not in ['csv', 'json', 'xml']:
        p.print_help()
        print "\n'{}' is not a valid file type".format(params.extension)
        print "Valid file types are 'csv', 'json', or 'xml'\n"
        quit(1)
    if os.path.isfile(params.file_name + '.' + params.extension) and not params.overwrite:
        print "\n{} exists. Add the -o flag to overwrite the existing file\n".format(params.file_name + '.' + params.extension)
        quit(1)
    if os.path.isfile(params.file_name + '.nam') and not params.overwrite:
        print "\n{} exists. Add the -o flag to overwrite the existing file\n".format(params.file_name + '.nam')
        quit(1)
    if os.path.isfile(params.file_name + '.typ') and not params.overwrite:
        print "\n{} exists. Add the -o flag to overwrite the existing file\n".format(params.file_name + '.typ')
        quit(1)
    if not params.file_name  or not isinstance(params.file_name, basestring):
        p.print_help()
        print "\nFile name must be non-empty string\n"
        quit(1)
    if params.cols < 1 or params.rows < 1:
        p.print_help()
        print "\nColumns and rows must be positive\n"
        quit(1)
    if params.unicode and ("#" not in params.unicode or params.unicode[0].upper() != 'U' or ("'" in params.unicode and '"' in params.unicode)):
        p.print_help()
        print "\nUnicode field must be formatted properly\nFor example: U+#### or u\\\\####\n"
        quit(1)
    if params.unicode:
        for q in params.quote:
            if q in params.unicode:
                p.print_help()
                print "\nQuote character can not be used in a unicode format"
                quit(1)
    if params.pe < 0.0 or params.pe > 100.0:
        p.print_help()
        print "\nPercent error possibility must be between 0.0 and 100.0, inclusive\n"
        quit(1)
    # check the number of columns vs the number of names vs the number of types
    l_match = True
    l_cols = params.cols
    l_names = len(params.col_names)
    if params.col_types:
        l_types = len(params.col_types)
    else:
        l_types = 1
    # check columns vs column names
    if l_cols != l_names and l_cols != 1 and l_names != 1:
        l_match = False
    # check columns vs column types
    if l_cols != l_types and l_cols != 1 and l_types != 1:
        l_match = False
    # check column names vs column types
    if l_names != l_types and l_names!= 1 and l_types != 1:
        l_match = False
    if not l_match:
        p.print_help()
        print "\nThe number of columns({}),".format(l_cols if l_cols > 1 else "default"),
        print "column names({}),".format(l_names if l_names > 1 else "default"),
        print "and column types({}) do not match".format(l_types if l_types > 1 else "default")
        print "Quitting program\n"
        quit(1)
    # end of def check_fatals

################################################################################

def open_for_read(file_name, talkative):
    """
    open a file to read data from it.

    Parameters
    ----------
    file_name : str
        Name of the file to open including extension
    talkative : bool
        A flag to indicate a verbose response

    Returns
    -------
    file handle or None

    """
    if talkative:
        print "Reading the file '{}'".format(file_name)
    """
    open/overwrite the input file
    """
    try:
        f = open(file_name, 'r')
    except:
        print "Input file {} could not be opened.".format(file_name)
        f = None
    return f
    # end of def open_for_read

################################################################################

def open_for_write(file_name, talkative):
    """
    open a file to write data to it.

    Parameters
    ----------
    file_name : str
        Name of the file to open including extension
    talkative : bool
        A flag to indicate a verbose response

    Returns
    -------
    file handle

    Notes
    -----
    Failure to open the file will stop the program

    """
    if talkative:
        print "\nBuilding the file '{}'".format(file_name),
    """
    open/overwrite the output file
    """
    try:
        return open(file_name, 'w')
    except:
        print "\nOutput file {} could not be opened. Quitting.".format(file_name)
        quit(1)
    # end of def open_for_write

################################################################################

def random_float(scientific):
    """
    build a random float value.

    Parameters
    ----------
    scientific : bool
        True will return the float in scientific format

    Returns
    -------
    string
        A random float value

    """
    return calculate_float(16,6, scientific)
    
################################################################################

def random_float32(scientific):
    """
    build a random float32 value.

    Parameters
    ----------
    scientific : bool
        True will return the float in scientific format

    Returns
    -------
    string
        A random float32 value

    """
    return calculate_float(32, 10, scientific)

################################################################################

def random_float64(scientific):
    """
    build a random float64 value.

    Parameters
    ----------
    scientific : bool
        True will return the float in scientific format

    Returns
    -------
    string
        A random float64 value

    """
    return calculate_float(64, 20, scientific)

################################################################################

def random_int():
    """
    build a random int value.

    Returns
    -------
    string
        A random int value

    """
    return "{}".format(random.randrange(1 - (2 ** 16 - 1), 2 ** 16 - 1))

################################################################################

def random_int32():
    """
    build a random int32 value.

    Returns
    -------
    string
        A random int32 value

    """
    return "{}".format(random.randrange(1 - (2 ** 32 - 1), 2 ** 32 - 1))

################################################################################

def random_int64():
    """
    build a random int64 value.

    Returns
    -------
    string
        A random int64 value

    """
    return "{}".format(random.randrange(1 - (2 ** 64 - 1), 2 ** 64 - 1))

################################################################################

def random_string(user_quote, comma, allow_comma, param_pe):
    """
    Build a string of random length up to 'char_max' characters, surrounded by either ' or " characters

    Parameters
    ----------
    user_quote : str
        A character or string of characters which can be used to surround strings
    comma : str
        The string used to separate columns
    allow_commas : bool
        Allow delimiter symbols to exist within the string
    param_pe : float
        The possibility of creating an error in the formatting of the result string

    Returns
    -------
    string
        A string of random characters

    """
    string_len = random.randrange(char_max)
    if len(user_quote) > 1:
        quote = random.choice(user_quote)
    else:
        quote = user_quote
    val = ''
    while len(val) < string_len:
        next_char = chr(random.randrange(32,127))
        if next_char != quote and (next_char != comma or allow_comma) and param_pe <= 10000*random.random():
            val += next_char
    return "{0}{1}{0}".format(quote, val)

################################################################################

def random_unicode(unicode_format, user_quote, comma, allow_comma, percent_error):
    """
    Build a string of characters with characters outside of the normal ASCII range expressed as unicode characters

    Parameters
    ----------
    unicode_format : str
        The format of the unicode character
    percent_error : float
        The possibility of creating an error in the formatting of the result string

    Returns
    -------
    str
        A string of length up to 'char_max' with unicodes imbedded

    """
    string_len = random.randrange(char_max)
    if len(user_quote) > 0:
        quote = random.choice(user_quote)
    else:
        quote = user_quote
    string_pointer = 0
    val = ""
    while string_pointer < string_len:
        next_char = random.randrange(1, 256)
        if next_char != ord(quote) and next_char > 31 and next_char < 127 and (next_char != comma or allow_comma) and percent_error <= 10000*random.random():
            val += chr(next_char)
        else:
            next_char = ''
            for c in unicode_format:
                if c == '#':
                    next_char += random.choice("0123456789ABCDEF")
                else:
                    next_char += c
                # end of for c in unicode_format:
            val += next_char
            # end of if next_char != ord(quote) and ...
        string_pointer += 1
        # end of while string_pointer < string_len:
    return "{0}{1}{0}".format(quote, val)
    # end of def random_unicode

################################################################################

def scientific_format(num, precision):
    """
    Build a number string in scientific format.

    Scientific format in this case means a single digit before the decimal point, the digits after the decimal point, the letter 'e' to represent "times 10 to the power of", and an exponent.

    Parameters
    ----------
    num : [int | int32 | int64 | float | float32 | float64]
        The number to be formatted
    precision: int
        The number of digits after the decimal point

    Returns
    -------
    str
        The number formatted

    """
    if type(num) in [int, int32, int64]:
        num = float(num)
    if num == 0:
        exponent = 0
        sign = ''
    elif num < 0:
        sign = '-'
        num=abs(num)
        exponent = int(numpy.log10(abs(num)))
    else:
        sign = ''
        exponent = int(numpy.log10(abs(num)))
    if exponent < 0:
        exponent -= 1
    if exponent == 0 and num < 1:
        exponent -= 1
    single_digit = int(num/(10 ** exponent))
    form = "{{0:.{}f}}".format(precision)
    remainder_str = form.format((num/(10 ** exponent)) - single_digit)
    if remainder_str[1] == '.':
        return '{0}{1}.{2}e{3}'.format(sign, single_digit, remainder_str[2:precision], exponent)
    elif remainder_str[2] == '.':
        return '{0}{1}.{2}e{3}'.format(sign, single_digit, remainder_str[3:precision], exponent)
    else:
        return '{0}{1}.{2}e{3}'.format(sign, single_digit, remainder_str[4:precision], exponent)
    #end of def scientific_notation

################################################################################

def set_defaults(args):
    """
    Set the defaults if not specified by the user

    Parameters
    ----------
    args : <class 'argparse.Namespace'>
        A class containing all the arguments passed to the program from the command line

    Returns
    -------
    args
        Modified with default values as necessary

    """

    valid_types = acceptable_types(args.unicode, args.bool)
    if not args.extension:
        args.extension = "csv"
    if not args.file_name:
        args.file_name = "datagen"
    if not args.cols:
        args.cols = 1
    if not args.rows:
        args.rows = 1
    if not args.comma:
        args.comma = ','
    if not args.quote:
        args.quote = '"'+"'"
    if not args.pe:
        args.pe = 0
    if debug: print "Set names='{}'\nSet types='{}'\nSet columns={}\n".format(args.col_names, args.col_types, args.cols)
    if not args.col_names:
        args.col_names = ['col_']
    if isinstance(args.col_names, str):
        args.col_names = [args.col_names]
    if args.col_types and isinstance(args.col_types, str):
        args.col_types = [args.col_types]
    if args.f_names and os.path.isfile(args.f_names + ".nam"):
        args.col_names = build_cols_from_file(args.f_names, args.verbose)
        if args.col_names and args.cols == 1:
            args.cols = len(args.col_names)
    if args.f_types and os.path.isfile(args.f_types + ".typ"):
        args.col_types = build_cols_from_file(args.f_types, args.verbose, valid_types)
        if args.col_types and args.cols == 1:
            args.cols = len(args.col_types)
    return valid_types
    # end of def set_defaults

################################################################################

"""
First, import other packages used for support.
"""
import argparse
from intelanalytics import *
import numpy
import os.path
import random
import string
import sys

global char_max
char_max = 100

p = argparse.ArgumentParser(description="Build a data file with random data",
    epilog="Do not put extensions on file names." +
        " Give unicode format with # where digits should be. Giving the unicode format allows unicode columns.")

p.add_argument("-v", "--verbose", action="store_true", help="tell you what is going on")

g_output = p.add_argument_group("Output Options")
g_output.add_argument("-f", "--file", type=str, help="the name of the data file to be created [datagen]", dest='file_name', metavar='*')
g_output.add_argument("-e", "--extension", type=str, help="csv, json, or xml [csv]", dest='extension', metavar='X')
g_output.add_argument("-o", "--overwrite", action="store_true", dest='overwrite', help="overwrite the existing file if it exists")

g_size = p.add_argument_group("Size Options")
g_size.add_argument("-c", "--cols", type=long, help="the number of columns of data [1]", metavar='#')
g_size.add_argument("-r", "--rows", type=long, help="the number of rows of data [1]", metavar='#')

g_column = p.add_argument_group("Column Options")
g_column.add_argument("-n", "--names", type=str, help="column prefix or a list of column names [col_]", nargs='+', dest='col_names', metavar='X')
g_column.add_argument("-t", "--types", type=str, help="type of all columns or a list of column types", nargs='+', dest='col_types', metavar='X')
g_column.add_argument("-fn", "--f_names", type=str, help="file with the column names", dest='f_names', metavar='*')
g_column.add_argument("-ft", "--f_types", type=str, help="file with the column types", dest='f_types', metavar='*')

g_specialty = p.add_argument_group("Specialty Options")
g_specialty.add_argument("-a", "--allow", action="store_true", help="allow the delimiter inside strings and unicode strings")
g_specialty.add_argument("-b", "--bool", action="store_true", help="permit boolean columns")
g_specialty.add_argument("-d", "--delimiter", type=str, help="the delimiter defining fields [,]", dest='comma', metavar='X')
g_specialty.add_argument("-p", "--percent", type=float, help="percent error possibility (0.0 - 100.0) [0.0]", dest='pe', metavar='#')
g_specialty.add_argument("-q", "--quote", type=str, help="symbol(s) to enclose strings ['"+'"]', dest='quote', metavar='X')
g_specialty.add_argument("-s", "--scientific", action="store_true", help="use scientific notation for floats", dest='scientific')
g_specialty.add_argument("-u", "--unicode", type=str, help="unicode format", metavar='U#')

args = p.parse_args()
if debug:
    print "args =", args

types = set_defaults(args)
check_fatals(args)
columns = build_columns(args.file_name, args.cols, args.col_names, args.col_types, types, args.verbose)
build_rows(args.file_name + "." + args.extension, columns, args.comma, args.quote, args.rows, types, args.unicode, args.allow, args.scientific, 
    args.pe, args.verbose)
