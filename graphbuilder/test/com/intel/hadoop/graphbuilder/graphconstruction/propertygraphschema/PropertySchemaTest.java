package com.intel.hadoop.graphbuilder.graphconstruction.propertygraphschema;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

public class PropertySchemaTest {


    @Test
    public void testPropertySchemaConstructorGetters() {

        final String   A        = new String("A");
        final Class<?> dataType = Integer.class;

        PropertySchema propertySchema = new PropertySchema(A, dataType);


        String   testName     = propertySchema.getName();
        Class<?> testDataType = propertySchema.getType();

        assertNotNull(testName);
        assert(testName.compareTo(A) == 0);
        assertEquals(dataType,testDataType);
    }

    @Test
    public void testPropertySchemaSetGet() {
        final String   A         = new String("A");
        final Class<?> dataTypeA = Integer.class;

        final String   B         = new String("A");
        final Class<?> dataTypeB = Float.class;

        final String   C         = new String("C");
        final Class<?> dataTypeC = String.class;

        PropertySchema propertySchemaA = new PropertySchema(A, dataTypeA);
        PropertySchema propertySchemaB = new PropertySchema(B, dataTypeB);

        propertySchemaA.setName(C);

        String   testName1 = propertySchemaA.getName();
        Class<?> testType1 = propertySchemaA.getType();
        assert(testName1.compareTo(C) == 0);
        assertEquals(dataTypeA, testType1);

        propertySchemaA.setType(dataTypeC);

        String   testName2 = propertySchemaA.getName();
        Class<?> testType2 = propertySchemaA.getType();
        assert(testName2.compareTo(C) == 0);
        assertEquals(dataTypeC, testType2);

        propertySchemaA.setName(A);

        String   testName3 = propertySchemaA.getName();
        Class<?> testType3 = propertySchemaA.getType();
        assert(testName3.compareTo(A) == 0);
        assertEquals(dataTypeC, testType3);

        propertySchemaA.setType(dataTypeA);

        String   testName4 = propertySchemaA.getName();
        Class<?> testType4 = propertySchemaA.getType();
        assert(testName4.compareTo(A) == 0);
        assertEquals(dataTypeA, testType4);

        String   distinctName = propertySchemaB.getName();
        Class<?> distinctType = propertySchemaB.getType();
        assert(distinctName.compareTo(B) == 0);
        assertEquals(dataTypeB, distinctType);
    }

}
