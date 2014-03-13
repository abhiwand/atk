package com.intel.pig.udf;


import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;
import org.joda.time.DateTime;

import java.io.IOException;

public class AfterDate extends FilterFunc {
    @Override
    public Boolean exec(Tuple objects) throws IOException {

        if(objects.size() != 2)
            throw new IOException("Have to pass two DateTime objects");

        DateTime first = (DateTime) objects.get(0);
        DateTime second = (DateTime) objects.get(1);

        return first.isAfter(second);
    }
}
