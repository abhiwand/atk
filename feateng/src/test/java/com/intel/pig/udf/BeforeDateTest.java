package com.intel.pig.udf;


import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class BeforeDateTest {
    EvalFunc<?> testFn;
    @Before
    public void setup() throws Exception {
        testFn = (EvalFunc<?>) PigContext
                .instantiateFuncFromSpec("com.intel.pig.udf.BeforeDate");
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
        assertEquals(false, testFn.exec(inTuple));
    }

    @Test
    public void date_smaller() throws IOException {
        DateTime d1 = new DateTime(2014, 3, 13, 11, 50, 0, 0);
        DateTime d2 = new DateTime(2015, 3, 13, 11, 50, 0, 0);
        List<DateTime> list = new ArrayList<DateTime>();
        list.add(d1);
        list.add(d2);

        Tuple inTuple = TupleFactory.getInstance().newTuple(list);
        assertEquals(true, testFn.exec(inTuple));
    }

    @Test
    public void receiveByteArray() throws IOException {
        DataByteArray test1 = new DataByteArray();
        DateTime d1 = new DateTime(2014, 3, 13, 11, 50, 0, 0);
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o = new ObjectOutputStream(b);
        o.writeObject(d1);
        test1.append(b.toByteArray());

        DataByteArray test2 = new DataByteArray();
        DateTime d2 = new DateTime(2014, 3, 13, 11, 50, 0, 0);
        ByteArrayOutputStream b2 = new ByteArrayOutputStream();
        ObjectOutputStream o2 = new ObjectOutputStream(b2);
        o2.writeObject(d2);
        test2.append(b2.toByteArray());

        List<DataByteArray> list = new ArrayList<DataByteArray>();
        list.add(test1);
        list.add(test2);

        Tuple inTuple = TupleFactory.getInstance().newTuple(list);
        assertEquals(false, testFn.exec(inTuple));
    }
}
