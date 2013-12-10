package com.intel.hadoop.graphbuilder.util;


import com.intel.hadoop.graphbuilder.graphelements.GraphElement;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ArgumentBuilderTest {

    private static final String testArgument = "zombies";
    private static final String testArgumentValue = "like brains";

    private ArgumentBuilder args;

    @After
    public void tearDown(){
        args = null;
    }

    @Test
    public void test_setting_and_getting(){
        args = new ArgumentBuilder().with(testArgument, testArgumentValue);

        assertTrue("verify exists with arg name", args.exists(testArgument));
        assertTrue("verify exists with arg object", args.exists((Object)testArgumentValue));
        assertTrue("verify type matches", args.isType(testArgument, String.class));
        assertEquals("verify set value and the get value", args.get(testArgument), testArgumentValue);

        args = ArgumentBuilder.newArguments().with(testArgument, testArgumentValue);

        assertTrue("verify exists with arg name", args.exists(testArgument));
        assertTrue("verify exists with arg object", args.exists((Object)testArgumentValue));
        assertTrue("verify type matches", args.isType(testArgument, String.class));
        assertEquals("verify set value and the get value", args.get(testArgument), testArgumentValue);
    }

    @Test
    public void test_isEmpty(){
        args = new ArgumentBuilder();
        assertFalse("This value should not exist", args.exists("some arg name"));
        assertFalse("This value should not exist", args.exists((Object)testArgumentValue));

        args.with(testArgument, testArgumentValue);
        assertTrue("argument should exist", args.exists(testArgument));
        assertTrue("argument should exist", args.exists((Object)testArgumentValue));


        args = ArgumentBuilder.newArguments();
        assertFalse("This value should not exist", args.exists("some arg name"));
        assertFalse("This value should not exist", args.exists((Object)testArgumentValue));

        args = ArgumentBuilder.newArguments().with(testArgument, testArgumentValue);
        assertTrue("argument should exist", args.exists(testArgument));
        assertTrue("argument should exist", args.exists((Object) testArgumentValue));
    }

    @Test
    public void test_without(){
        args = new ArgumentBuilder();
        args.with(testArgument, testArgumentValue);

        verifyExistsArgument();

        args.withOut(testArgument);
        verifyDoesntExistsArgument();

        args = ArgumentBuilder.newArguments().with(testArgument, testArgumentValue);

        verifyExistsArgument();

        args.withOut(testArgument);
        verifyDoesntExistsArgument();
    }

    @Test
    public void test_empty(){
        args = new ArgumentBuilder();
        assertTrue("we should have an empty arg set", args.isEmpty());

        args = ArgumentBuilder.newArguments();
        assertTrue("we should have an empty arg set", args.isEmpty());
    }

    @Test
    public void test_get_with_default_value(){
        args = new ArgumentBuilder();
        assertTrue("make sure we get the default value when empty", (args.get(testArgument, Boolean.TRUE) == Boolean.TRUE));
        args.with(testArgument, testArgumentValue);
        assertTrue("make sure we get the argument builder value when set and no the default value",
                (args.get(testArgument, Boolean.TRUE) == testArgumentValue));


        args = ArgumentBuilder.newArguments();
        assertTrue("make sure we get the default value when empty", (args.get(testArgument, Boolean.TRUE) == Boolean.TRUE));
        args.with(testArgument, testArgumentValue);
        assertTrue("make sure we get the argument builder value when set and no the default value",
                (args.get(testArgument, Boolean.TRUE) == testArgumentValue));

    }

    private void verifyExistsArgument(){
        assertTrue("verify exists with arg name", args.exists(testArgument));
        assertTrue("verify exists with arg object", args.exists((Object)testArgumentValue));
        assertTrue("verify type matches", args.isType(testArgument, testArgumentValue.getClass()));
        //verify against any other class to make sure it's not always returning true
        assertFalse("verify type doesn't match", args.isType(testArgument, GraphElement.class));
    }

    private void verifyDoesntExistsArgument(){
        assertFalse("verify exists with arg name", args.exists(testArgument));
        assertFalse("verify exists with arg object", args.exists((Object) testArgumentValue));
    }

}
