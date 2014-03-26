package com.intel.pig.udf.flatten;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;

/**
 * Convert a table with multiple values in a cell into multiple rows each with a single value.
 * 
 * <pre>
 * For example, start table:
 * 1 a,b,c
 * 2 b,c
 * 3 b
 * 
 * Result table:
 * 1 a
 * 1 b
 * 1 c
 * 2 b
 * 2 c
 * 3 b
 * </pre>
 * 
 * <p>
 * This UDF is confusingly named from a Pig perspective because Pig has a built-in called FLATTEN. We're calling it
 * flatten here because that is what the Python operation will be called.
 * </p>
 * <p>
 * UTF-8 is assumed for DataType.BYTEARRAY's.
 * </p>
 */
public class FlattenColumnToMultipleRowsUDF extends EvalFunc<DataBag> {

    private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();
    private static final BagFactory BAG_FACTORY = BagFactory.getInstance();
    private static final String[] STRING_ARRAY_WITH_ONE_EMPTY_STRING = new String[]{""};

    private final int columnToFlatten;
    private final StringSplitter splitter;

    /**
     * Convert a tuple to a dataBag of one or more tuples depending on the value in the columnToFlatten.
     * 
     * @param columnToFlatten (int) index of column to flatten (zero based)
     * @param delimiter (String) to split the column value on (required)
     * @param trimStart (String) string to trim from the start (optional: can be null or empty)
     * @param trimEnd (String) string to trim from end (optional: can be null or empty)
     * @param trimWhitespace (boolean) should whitespace be trimmed when splitting the cell value
     */
    public FlattenColumnToMultipleRowsUDF(String columnToFlatten, String delimiter, String trimStart, String trimEnd, String trimWhitespace) {
        // We're parsing here because Pig requires all constructor args to be Strings
        this.columnToFlatten = Integer.parseInt(columnToFlatten);
        this.splitter = new StringSplitter(new StringSplitOptions(delimiter, trimStart, trimEnd,
                Boolean.parseBoolean(trimWhitespace)));

    }

    /**
     * Flatten the input tuple by creating copies, where one column is flattened into multiple rows.
     * 
     * @param input the Tuple to be processed.
     * @return DataBag with one or more tuples
     */
    @Override
    public DataBag exec(Tuple input) throws IOException {
        DataBag dataBag = BAG_FACTORY.newDefaultBag();

        String cellValue = DataType.toString(input.get(columnToFlatten));

        for (String flattenedValue : flatten(cellValue)) {
            Tuple tuple = TUPLE_FACTORY.newTuple(input.getAll());
            tuple.set(columnToFlatten, toOutputType(flattenedValue));
            dataBag.add(tuple);
        }

        return dataBag;
    }

    /**
     * Split the string but always with at least one empty value returned
     */
    protected String[] flatten(String cellValue) {
        String[] parts = splitter.split(cellValue);
        if (parts.length == 0) {
            // ensure that flattening produces at least one item so that we don't delete rows
            parts = STRING_ARRAY_WITH_ONE_EMPTY_STRING;
        }
        return parts;
    }

    /**
     * Convert String back to DataType.BYTEARRAY or other appropriate DataType as needed
     * @param flattenedValue a part of the original value
     * @return the appropriate output type based on the InputSchema
     */
    private Object toOutputType(String flattenedValue) throws FrontendException {
        if (getInputSchema() == null || getInputSchema().getField(columnToFlatten) == null) {
            return flattenedValue;
        }
        byte type = getInputSchema().getField(columnToFlatten).type;
        if (type == DataType.CHARARRAY || type == DataType.BIGCHARARRAY) {
            return flattenedValue;
        }
        if (type == DataType.BYTEARRAY) {
            return new DataByteArray(flattenedValue);
        }

        throw new RuntimeException("Flatten only supports CHARARRAY's and BYTEARRAY's as input column. "
                    + " Input was :" + DataType.findTypeName(type));
    }
}
