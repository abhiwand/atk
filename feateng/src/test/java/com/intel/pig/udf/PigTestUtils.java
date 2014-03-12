package com.intel.pig.udf;

import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * Utility methods for writing tests related to Pig code
 */
public class PigTestUtils {

    private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();

    /**
     * Make sure a bag contains the supplied tuple.
     * 
     * Fail test if not found.
     * 
     * @param dataBag the bag to look for the expected tuple
     * @param expectedTuple what you expect to find
     */
    public static void assertBagContainsTuple(DataBag dataBag, Tuple expectedTuple) throws ExecException {
        assertTrue("Bag did not contain tuple.  Bag contents was: " + bagToString(dataBag),
                bagContainsTuple(dataBag, expectedTuple));
    }

    /**
     * Make sure a bag contains the supplied tuple.
     * 
     * @param dataBag the bag to look for the expected tuple
     * @param expectedTuple what you expect to find
     */
    public static boolean bagContainsTuple(DataBag dataBag, Tuple expectedTuple) throws ExecException {
        Iterator iterator = dataBag.iterator();
        while (iterator.hasNext()) {
            Tuple output = (Tuple) iterator.next();

            if (equals(expectedTuple, output)) {
                // we found what we wanted so we can return early
                return true;
            }
        }
        return false;
    }

    public static void assertTupleEquals(Tuple tuple1, Tuple tuple2) throws ExecException {
        assertTrue(equals(tuple1, tuple2));
    }

    /**
     * Equals that handles nulls.
     * 
     * equals(null, null) is true. false if only one param is null.
     * 
     * @return true if equal, false otherwise
     */
    public static boolean equals(Tuple tuple1, Tuple tuple2) throws ExecException {
        if (tuple1 == null && tuple2 == null) {
            return true;
        }
        if (tuple1 == null || tuple2 == null || tuple1.size() != tuple2.size()) {
            return false;
        }
        for (int i = 0; i < tuple1.size(); i++) {
            Object o1 = tuple1.get(i);
            Object o2 = tuple2.get(i);
            if (!ObjectUtils.equals(o1, o2)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Helper method for creating tuples with all string values
     */
    public static Tuple createTuple(String... values) throws ExecException {
        Tuple tuple = TUPLE_FACTORY.newTuple(values.length);
        int fieldNum = 0;
        for (String value : values) {
            tuple.set(fieldNum, value);
            fieldNum++;
        }
        return tuple;
    }

    /**
     * For easy to debug error messages when assertions fail
     */
    private static String bagToString(DataBag dataBag) throws ExecException {
        StringBuilder s = new StringBuilder();
        Iterator iterator = dataBag.iterator();
        while (iterator.hasNext()) {
            Tuple output = (Tuple) iterator.next();
            s.append(output.toDelimitedString(", "));
            s.append("\n");
        }
        return s.toString();
    }
}
