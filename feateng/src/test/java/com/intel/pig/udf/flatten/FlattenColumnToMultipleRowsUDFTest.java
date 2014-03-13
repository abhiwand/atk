package com.intel.pig.udf.flatten;

import static com.intel.pig.udf.PigTestUtils.*;
import static org.junit.Assert.assertEquals;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.junit.Test;

public class FlattenColumnToMultipleRowsUDFTest {

    @Test
    public void flatten_should_handle_empty() throws Exception {

        // create test data
        Tuple input = createTuple("1", "foo", "");

        // instantiate class under test
        FlattenColumnToMultipleRowsUDF udf = new FlattenColumnToMultipleRowsUDF("2", ",", "(", ")", "false");

        // invoke method under test
        DataBag bag = udf.exec(input);

        // assertions
        assertEquals(1, bag.size());

        Tuple output = bag.iterator().next();
        assertTupleEquals(input, output);
    }

    @Test
    public void flatten_should_handle_single_value() throws Exception {

        // create test data
        Tuple input = createTuple("1", "foo", "c");

        // instantiate class under test
        FlattenColumnToMultipleRowsUDF udf = new FlattenColumnToMultipleRowsUDF("2", ",", "(", ")", "false");

        // invoke method under test
        DataBag bag = udf.exec(input);

        // assertions
        assertEquals(1, bag.size());

        Tuple output = bag.iterator().next();
        assertTupleEquals(input, output);
    }

    @Test
    public void flatten_should_handle_single_value_with_start_and_end() throws Exception {

        // create test data
        Tuple input = createTuple("1", "foo", "(c)");

        // instantiate class under test
        FlattenColumnToMultipleRowsUDF udf = new FlattenColumnToMultipleRowsUDF("2", ",", "(", ")", "false");

        // invoke method under test
        DataBag bag = udf.exec(input);

        // assertions
        assertEquals(1, bag.size());

        Tuple output = bag.iterator().next();
        assertTupleEquals(createTuple("1", "foo", "c"), output);
    }

    @Test
    public void flatten_should_handle_multiple_values() throws Exception {

        // create test data
        Tuple input = createTuple("1", "foo", "a,b,c");

        // instantiate class under test
        FlattenColumnToMultipleRowsUDF udf = new FlattenColumnToMultipleRowsUDF("2", ",", "(", ")", "false");

        // invoke method under test
        DataBag bag = udf.exec(input);

        // assertions
        assertEquals(3, bag.size());

        assertBagContainsTuple(bag, createTuple("1", "foo", "a"));
        assertBagContainsTuple(bag, createTuple("1", "foo", "b"));
        assertBagContainsTuple(bag, createTuple("1", "foo", "c"));
    }

    @Test
    public void flatten_should_handle_multiple_values_with_start_and_end() throws Exception {

        // create test data
        Tuple input = createTuple("1", "foo", "(a,b,c)");

        // instantiate class under test
        FlattenColumnToMultipleRowsUDF udf = new FlattenColumnToMultipleRowsUDF("2", ",", "(", ")", "false");

        // invoke method under test
        DataBag bag = udf.exec(input);

        // assertions
        assertEquals(3, bag.size());

        assertBagContainsTuple(bag, createTuple("1", "foo", "a"));
        assertBagContainsTuple(bag, createTuple("1", "foo", "b"));
        assertBagContainsTuple(bag, createTuple("1", "foo", "c"));
    }

}
