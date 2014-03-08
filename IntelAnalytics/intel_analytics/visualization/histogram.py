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

import matplotlib.pyplot as plt

def is_number(val):
    try:
        float(val)
        return True
    except ValueError:
        return False 

def plot_histogram(datafile, xlabel='', ylabel='', title='',
                   textfile = ''):
    """
    Plots a histogram
    Parameters
    ----------
    datafile: String
        filename for the histogram data
    xlabel: String
        x axis label
    ylabel: String
        y axis label
    title: String
        histogram title
    textfile: String
        filename for the text data to be placed aside to histogram

    Returns
    -------
    result: List
        list of lines read from textfile
    """

    result = []
                              
    with open(datafile) as h:
        hlines = [x.strip() for x in h.readlines()]
        
    slines = []
    if textfile:
        with open(textfile) as s:
            slines = [x.strip() for x in s.readlines()]

    
    data_x = []
    data_y = []
    for i in range(len(hlines)):
        t = hlines[i].split()
        if len(t) == 1:
            slines.append('missing_values=' + t[0])
        else:
            data_x.append(t[0])
            data_y.append(int(t[1]))

    # Sort and remove any duplicates from text
    slines = list(set(slines))
    # Sort the data
    data_x, data_y = zip(*sorted(zip(data_x,data_y), key=lambda x:eval(x[0]) if is_number(x[0]) else x[0]))

    result.extend(slines)

    fig = plt.figure()
    axes = fig.add_axes([0.1,0.1,0.8,0.8])

    plt.bar(range(len(data_y)), data_y, align='center')
    plt.xticks(range(len(data_y)), data_x)

    latex_symbols = {'max' : '$\max$', 'min' : '$\min$', 'avg' : '$\mu$', 'stdev' : '$\sigma$', 'var' : '$\sigma^2$'}
    for i,j in latex_symbols.iteritems():
        slines = [w.replace(i, j) for w in slines]
    
    stats = "\n".join(sorted(slines))
    
    font_size = 12
    plt.xlabel(xlabel, fontsize = font_size)
    plt.ylabel(ylabel, fontsize = font_size)
    plt.title(title, fontsize = font_size)
    plt.figtext(1.2,0.1, r'%s' % (stats), fontsize = 18)
    plt.grid(True)
    plt.show()

    return result 

