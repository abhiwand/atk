package com.intel.pig.udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
        testFn.exec(null);
    }

    @Test
    public void getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = testFn.getArgToFuncMapping();
        assertEquals(2, funcList.size());

        Set<String> expectedSchema = new HashSet<String>();
        expectedSchema.add("{datetime,datetime}");
        expectedSchema.add("{bytearray,bytearray}");

        FuncSpec f1 = funcList.get(0);
        FuncSpec f2 = funcList.get(1);

        expectedSchema.remove(f1.getInputArgsSchema().toString());
        expectedSchema.remove(f2.getInputArgsSchema().toString());
        assertEquals(0, expectedSchema.size());
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

    @Test
    public void receiveByteArray() throws IOException {
        DataByteArray test1 = new DataByteArray();
        test1.append("2012-01-30T04:54:10-08:00".getBytes());

        DataByteArray test2 = new DataByteArray();
        test2.append("2015-01-30T04:54:10-08:00".getBytes());

        List<DataByteArray> list = new ArrayList<DataByteArray>();
        list.add(test1);
        list.add(test2);

        Tuple inTuple = TupleFactory.getInstance().newTuple(list);
        assertEquals(false, testFn.exec(inTuple));
    }
}
