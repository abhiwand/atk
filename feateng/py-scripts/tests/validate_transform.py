import subprocess
import sys
import argparse
import getopt
import math

sys.path.append("py-scripts/") #etl_hbase_client and config modules are in the upper directory
from etl_hbase_client import ETLHBaseClient
from config import CONFIG_PARAMS

DIFF_EPSILON = 0.01#diff used for floating point comparisons

def main(argv):
    parser = argparse.ArgumentParser(description='validates transformation')
    parser.add_argument('-f', '--feature', dest='feature', help='the feature that the transformation is applied', required=True)
    parser.add_argument('-n', '--new-feature-name', dest='new_feature_name', help='the name of the new feature obtained as a result of transformation', required=True)
    parser.add_argument('-i', '--input', dest='input', help='input HBase table to validate (output of the transformation process)', required=True)
    parser.add_argument('-t', '--transformation', dest='transformation_function', help='transformation function applied to given feature', required=True)
    parser.add_argument('-a', '--transformation-args', dest='transformation_function_args', help='transformation function arguments')
    cmd_line_args = parser.parse_args()
    print cmd_line_args
    with ETLHBaseClient(CONFIG_PARAMS['hbase-host']) as hbase_client:
        scanner = hbase_client.client.scannerOpen(cmd_line_args.input,'',[CONFIG_PARAMS['etl-column-family']],{})
        row = hbase_client.client.scannerGet(scanner)
        i = 0
        #scan all rows & validate the transformation result
        while row:
            col_dict = row[0].columns
            feature_val = col_dict[CONFIG_PARAMS['etl-column-family'] + cmd_line_args.feature].value
            new_feature_val = col_dict[CONFIG_PARAMS['etl-column-family'] + cmd_line_args.new_feature_name].value
            if feature_val == '':
                assert feature_val == new_feature_val
            else:
                if cmd_line_args.transformation_function == "POW":
                    pow_value = math.pow(float(feature_val),float(cmd_line_args.transformation_function_args))
                    assert (float(new_feature_val) - pow_value) < DIFF_EPSILON, "%f vs. %f" % (float(new_feature_val), pow_value)
                elif cmd_line_args.transformation_function == "LOG":
                    if float(feature_val) == 0.0:
                        assert new_feature_val == '-Infinity', "%s should have been -Infinity" % (new_feature_val)
                    else:
                        log_value = math.log(float(feature_val))
                        assert (float(new_feature_val) - log_value) < DIFF_EPSILON, "%f vs. %f" % (float(new_feature_val), log_value)
                elif cmd_line_args.transformation_function == "LOG10":
                    if float(feature_val) == 0.0:
                        assert new_feature_val == '-Infinity', "%s should have been -Infinity" % (new_feature_val)
                    else:
                        log10_value = math.log10(float(feature_val))
                        assert (float(new_feature_val) - log10_value) < DIFF_EPSILON, "%f vs. %f" % (float(new_feature_val), log10_value)
                elif cmd_line_args.transformation_function == "ABS":
                    abs_value = math.fabs(float(feature_val))
                    assert (float(new_feature_val) - abs_value) < DIFF_EPSILON, "%f vs. %f" % (float(new_feature_val), abs_value)
                elif cmd_line_args.transformation_function == "EXP":
                    #NOTE: THE EXP VALIDATION MAY FAIL BECAUSE
                    # I have seen differences between Java 1.6 and Java 1.7 floating point arithmetic results
                    # for example:
                    # for the input 189.0
                    # when we apply the exp function
                    # python 2.7 on windows returns: 1.2068605179340022e+82
                    # java 1.7 on windows returns:  1.2068605179340022E82
                    # java 1.6 on linux returns: 1.2068605179340024E82 [NOTICE THE DIFF TOWARDS THE END !]
                    # related JVM bugs: http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=7021568 and http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=7019078
                    try:
                        exp_value = math.exp(float(feature_val))
                        assert (float(new_feature_val) - exp_value)      < DIFF_EPSILON    , "%f vs. %f" % (float(new_feature_val), exp_value)
                    except OverflowError:
                        assert new_feature_val == 'Infinity', "%s should have been Infinity" % (new_feature_val)
                               
                else:
                    print "Currently we do not support transformation %s, for the available transformations see pig_transform.py" % (cmd_line_args.transformation_function)
            print '{0}\r'.format("validated row %d" % (i)),
            i+=1
            row = hbase_client.client.scannerGet(scanner)
    print
    print "Done validating all rows"
if __name__ == "__main__":
    main(sys.argv)