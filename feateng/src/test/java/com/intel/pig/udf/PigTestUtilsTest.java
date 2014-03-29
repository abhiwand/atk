package com.intel.pig.udf;

import static com.intel.pig.udf.PigTestUtils.bagContainsTuple;
import static com.intel.pig.udf.PigTestUtils.createTuple;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.junit.Test;

/**
 * Tests for PigTestUtils class.
 *
 * (Yes, these are tests for test code)
 */
public class PigTestUtilsTest {

    private static final BagFactory BAG_FACTORY = BagFactory.getInstance();

    @Test
    public void bagContainsTuple_true_with_one_item() throws Exception {
        DataBag bag = BAG_FACTORY.newDefaultBag();
        bag.add(createTuple("A"));

        assertTrue(bagContainsTuple(bag, createTuple("A")));
    }

    @Test
    public void bagContainsTuple_true_with_three_items() throws Exception {
        DataBag bag = BAG_FACTORY.newDefaultBag();
        bag.add(createTuple("A"));
        bag.add(createTuple("B"));
        bag.add(createTuple("C"));

        assertTrue(bagContainsTuple(bag, createTuple("A")));
    }

    @Test
    public void bagContainsTuple_false_with_one_item() throws Exception {
        DataBag bag = BAG_FACTORY.newDefaultBag();
        bag.add(createTuple("A"));

        assertFalse(bagContainsTuple(bag, createTuple("B")));
    }

    @Test
    public void bagContainsTuple_false_with_three_items() throws Exception {
        DataBag bag = BAG_FACTORY.newDefaultBag();
        bag.add(createTuple("A"));
        bag.add(createTuple("B"));
        bag.add(createTuple("C"));

        assertFalse(bagContainsTuple(bag, createTuple("X")));
    }

    @Test
    public void tuples_with_identical_fields_are_equal() throws Exception {
        assertTrue(PigTestUtils.equals(createTuple("1"), createTuple("1")));
        assertTrue(PigTestUtils.equals(createTuple("a", "b", "c"), createTuple("a", "b", "c")));
    }

    @Test
    public void tuples_with_different_fields_are_not_equal() throws Exception {
        assertFalse(PigTestUtils.equals(createTuple("A"), createTuple("B")));
        assertFalse(PigTestUtils.equals(createTuple("A"), null));
        assertFalse(PigTestUtils.equals(createTuple("a", "b", "c"), createTuple("a", "b", "x")));
        assertFalse(PigTestUtils.equals(createTuple("a", "b", "c"), createTuple("x", "b", "c")));
        assertFalse(PigTestUtils.equals(createTuple("a", "b", "c"), createTuple("a", "b")));
    }

    @Test
    public void both_empty_is_equal() throws Exception {
        assertTrue(PigTestUtils.equals(createTuple(), createTuple()));
    }

    @Test
    public void both_null_is_equal() throws Exception {
        assertTrue(PigTestUtils.equals(null, null));
    }
}
