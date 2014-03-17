package com.intel.pig.udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class AfterDateTest {
    EvalFunc<?> testFn;
    @Before
    public void setup() throws Exception {
        testFn = (EvalFunc<?>) PigContext
                .instantiateFuncFromSpec("com.intel.pig.udf.AfterDate");
    }

    @Test(expected=IllegalArgumentException.class)
    public void invalid_input() throws IOException {
        DateTime d1 = new DateTime(2014, 3, 13, 11, 50, 0, 0);
        List<DateTime> list = new ArrayList<DateTime>();
        list.add(d1);

        Tuple inTuple = TupleFactory.getInstance().newTuple(list);
        testFn.exec(inTuple);
    }

    @Test
    public void date_equal() throws IOException {
        DateTime d1 = new DateTime(2014, 3, 13, 11, 50, 0, 0);
        DateTime d2 = new DateTime(2014, 3, 13, 11, 50, 0, 0);
        List<DateTime> list = new ArrayList<DateTime>();
        list.add(d1);
        list.add(d2);

        Tuple inTuple = TupleFactory.getInstance().newTuple(list);
        assertEquals(false, testFn.exec(inTuple));
    }

    @Test
    public void date_bigger() throws IOException {
        DateTime d1 = new DateTime(2015, 3, 13, 11, 50, 0, 0);
        DateTime d2 = new DateTime(2014, 3, 13, 11, 50, 0, 0);
        List<DateTime> list = new ArrayList<DateTime>();
        list.add(d1);
        list.add(d2);

        Tuple inTuple = TupleFactory.getInstance().newTuple(list);
        assertEquals(true, testFn.exec(inTuple));
    }

    @Test
    public void date_smaller() throws IOException {
        DateTime d1 = new DateTime(2014, 3, 13, 11, 50, 0, 0);
        DateTime d2 = new DateTime(2015, 3, 13, 11, 50, 0, 0);
        List<DateTime> list = new ArrayList<DateTime>();
        list.add(d1);
        list.add(d2);

        Tuple inTuple = TupleFactory.getInstance().newTuple(list);
        assertEquals(false, testFn.exec(inTuple));
    }
}
