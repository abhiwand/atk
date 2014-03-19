package com.intel.pig.udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * UDF to get ordinal date
 */
public class OrdinalDate extends EvalFunc<String> {

    /**
     * Get ordinal date
     * @param objects datetime object
     * @return ordinal date
     * @throws IOException
     */
    @Override
    public String exec(Tuple objects) throws IOException {
        if(objects == null)
            throw new IllegalArgumentException("Have to pass a single valid DateTime object");

        DateTime date = DateTimeUtils.getDateTime(objects.get(0));
        DateTimeFormatter fmt = ISODateTimeFormat.ordinalDate();
        return fmt.print(date);
    }

    public List<FuncSpec> getArgToFuncMapping() {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        List<Schema.FieldSchema> dateTimefields = new ArrayList<Schema.FieldSchema>();
        dateTimefields.add(new Schema.FieldSchema(null, DataType.DATETIME));

        Schema oneDateTimeArg = new Schema(dateTimefields);
        funcList.add(new FuncSpec(this.getClass().getName(), oneDateTimeArg));

        List<Schema.FieldSchema> byteArrayfields = new ArrayList<Schema.FieldSchema>();
        byteArrayfields.add(new Schema.FieldSchema(null, DataType.BYTEARRAY));
        Schema oneByteArrayArg = new Schema(byteArrayfields);
        funcList.add(new FuncSpec(this.getClass().getName(), oneByteArrayArg));
        return funcList;
    }
}
