import iatest
iatest.init()

from intelanalytics.meta.command import Doc

doc1 = """Computes a cumulative percent sum.

    A cumulative percent sum is computed by sequentially stepping through the
    column values and keeping track of the current percentage of the total sum
    accounted for at the current value."""


d = Doc.get_from_str(doc1)

print d.one_line
print "-------------------------------------------------------"
print d.extended