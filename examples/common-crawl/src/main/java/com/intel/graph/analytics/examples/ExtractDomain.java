package com.intel.graph.analytics.examples;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.builtin.MonitoredUDF;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

@MonitoredUDF(errorCallback = GenericErrorHandler.class, duration=30, timeUnit= TimeUnit.MINUTES)
public class ExtractDomain extends EvalFunc<String> {

    @Override
    public String exec(Tuple objects) throws IOException {

        try {
            URL url = new URL((String)objects.get(0));
            String host = url.getHost();
            if (host.startsWith("www.")) {
                host = host.substring(4);
            }
            return host;
        } catch (Exception e) {
            warn("Exception parsing the base of url " + (String)objects.get(0), PigWarning.UDF_WARNING_1);
            return null;
        }
    }

}
