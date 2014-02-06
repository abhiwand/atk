import os
import sys

sys.path.insert(0, os.path.abspath(os.curdir) + '/../..')
from intel_analytics.table.builtin_functions import EvalFunctions
import inspect

def print_class(name, cls):
    print name
    print "+" * len(name)
    print ""
    for k in sorted(cls.__dict__.iterkeys()):
        if not k.startswith('__'):
            print '| ' + k

print ".. _evalfunctions:\n"
print "EvalFunctions"
print "============="
print "Enumeration of built-in evaluation functions, groups by category\n"
print "| Example:  EvalFunctions.Math.LOG"
print ">>> frame.transform('rating', 'log_rating', EvalFunctions.Math.LOG)"
print "\n"
for k,v in sorted(EvalFunctions.__dict__.iteritems()):
    if inspect.isclass(v):
        print_class(k, v)
        print "\n"