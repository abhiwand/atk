package com.intel.pig.udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
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

public class OrdinalDateTest {
    EvalFunc<?> testFn;
    @Before
    public void setup() throws Exception {
        testFn = (EvalFunc<?>) PigContext
                .instantiateFuncFromSpec("com.intel.pig.udf.OrdinalDate");
    }

    @Test
    public void get_ordinal_date() throws IOException {
        DateTime d1 = new DateTime(2014, 1, 1, 11, 50, 0, 0);
        List<DateTime> list = new ArrayList<DateTime>();
        list.add(d1);;

        Tuple inTuple = TupleFactory.getInstance().newTuple(list);
        assertEquals("2014-001", testFn.exec(inTuple));
    }

    @Test
    public void getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = testFn.getArgToFuncMapping();
        assertEquals(2, funcList.size());

        Set<String> expectedSchema = new HashSet<String>();
        expectedSchema.add("{datetime}");
        expectedSchema.add("{bytearray}");

        FuncSpec f1 = funcList.get(0);
        FuncSpec f2 = funcList.get(1);

        expectedSchema.remove(f1.getInputArgsSchema().toString());
        expectedSchema.remove(f2.getInputArgsSchema().toString());
        assertEquals(0, expectedSchema.size());
    }
}
