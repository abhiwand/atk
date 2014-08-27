#!/usr/local/bin/python

# move the existing .tex file to a .bak file

import os
import shutil
tex = ".//build//latex//IntelAnalytics.tex"
bak = ".//build//latex//IntelAnalytics.bak"
try:
    shutil.move(tex, bak)
except:
    print "Can not move .tex file to .bak extension."
    quit(1)

reference_section = False
in_table = False
found_index = False
found_end = False
found_index_2 = False
found_end_2 = False

# open the .bak file to read and the .tex file to write
with open(bak, 'r') as b:
    with open(tex, 'w') as t:

# process every line of input file
        for l in b:

            # We need to move the table of contents, from where it is placed, to the Contents section.
            # the line is the table of contents
            if l == "\\tableofcontents\n":
                l = ""
            if l.find("\\label{index:contents}") >= 0:
                t.write(l)
                l = "\\tableofcontents\n"
            # Any other references to contents (normally at the beginning of sections) will be deleted.
            if l == "Contents:\n":
                l = ""

            # Need to change the methods tables. They are generated as two columns, one for the function name, page number,
            # and parameters. The other column is for the summary line from the docstring.
            # We are going to change it so there are three columns. The first will be for the function and the parameters.
            # The second column will have the summary and the third will have the page number.
            # We will also put a box around the entire table and put a horizontal line across on every third function.
            if l == "\\paragraph{Methods}\n":
                in_table = True
                line_count = 0
            if in_table and l == "\\end{longtable}\n":
                in_table = False
            if in_table:
                if l == "\\begin{longtable}{ll}\n":
                    l = "\\begin{longtable}{|lll|}\n"
                if l.find("multicolumn{2}") >= 0:
                    l = l[:l.find("multicolumn{2}")] + "multicolumn{3}" + l[l.find("multicolumn{2}") + len("multicolumn{2}") :]
                if l.find("\\autopageref") >= 0:
                    started_search = l.find(" (\\autopageref")
                    l1 = l[:started_search]
                    l2 = l[started_search - 1:]
                    line_index = 0
                    paren_count = 0
                    for c in l2:
                        if c == '(':
                            paren_count += 1
                            started_search = line_index
                        if c == ')':
                            paren_count -= 1
                            if paren_count == 0:
                                ended_search = line_index
                                break
                        line_index += 1
                    l1 += l2[ended_search + 1:]
                    l2 = " & " + l2[started_search:ended_search+1] + '\n'
                    l = l1
                    if line_count % 3 == 0 and line_count > 0:
                        t.write("\\hline\n")
                if l[0] == '\\' and l[1] == '\\':
                    t.write(l2)
                    line_count += 1

            # this is the start of the references section which we will eliminate.
            if l == "\\part{References}\n":
                reference_section = True
            if l.find("\\renewcommand") >=0:
                reference_section = False
            if reference_section:
                l = ""


            # the line is the first of the index section
            if not found_index and not found_end and l == "\\renewcommand{\\indexname}{Python Module Index}\n":
                found_index = True
            elif found_index and not found_end and l == "\\end{theindex}\n":
                found_end = True
            elif found_index and found_end:
                
                # the line is the first line of the duplicate index section
                if l == "\\renewcommand{\\indexname}{Python Module Index}\n":
                    found_index_2 = True
                    l = ""
                elif found_index_2 and l == "\\end{theindex}\n":
                    found_end_2 = True
                    l = ""
                elif found_index_2 and not found_end_2:
                    l = ""

            # the line should be written to the file
            t.write(l)

        # end of loop

        # close the files
        t.close()
    b.close()
    # exit
