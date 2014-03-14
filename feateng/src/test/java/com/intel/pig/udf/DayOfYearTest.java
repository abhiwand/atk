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

public class DayOfYearTest {
    EvalFunc<?> testFn;
    @Before
    public void setup() throws Exception {
        testFn = (EvalFunc<?>) PigContext
                .instantiateFuncFromSpec("com.intel.pig.udf.DayOfTheYear");
    }

    @Test
    public void first_day_of_year() throws IOException {
        DateTime d1 = new DateTime(2014, 1, 1, 11, 50, 0, 0);
        List<DateTime> list = new ArrayList<DateTime>();
        list.add(d1);;

        Tuple inTuple = TupleFactory.getInstance().newTuple(list);
        assertEquals(1, testFn.exec(inTuple));
    }

    @Test
    public void day_of_year_6_22_no_leap_year() throws IOException {
        DateTime d1 = new DateTime(2013, 6, 22, 11, 50, 0, 0);
        List<DateTime> list = new ArrayList<DateTime>();
        list.add(d1);;

        Tuple inTuple = TupleFactory.getInstance().newTuple(list);
        assertEquals(173, testFn.exec(inTuple));
    }

    @Test
    public void day_of_year_6_22_leap_year() throws IOException {
        DateTime d1 = new DateTime(2012, 6, 22, 11, 50, 0, 0);
        List<DateTime> list = new ArrayList<DateTime>();
        list.add(d1);;

        Tuple inTuple = TupleFactory.getInstance().newTuple(list);
        assertEquals(174, testFn.exec(inTuple));
    }
}
