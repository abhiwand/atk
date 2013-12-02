package com.intel.graph.analytics.examples;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.pig.EvalFunc;
import org.apache.pig.builtin.MonitoredUDF;
import org.apache.pig.data.Tuple;

import org.apache.pig.impl.util.WrappedIOException;


@MonitoredUDF(errorCallback = GenericErrorHandler.class, duration=30, timeUnit=TimeUnit.MINUTES)
public class RemoveSymbols extends EvalFunc<String> {
		 
	@Override
	public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        try{
            String str = (String)input.get(0);
            return str.replaceAll("[^A-Za-z0-9 ]", " ");
        }catch(Exception e){
            throw WrappedIOException.wrap("Caught exception processing input row ", e);
        }
        
    }
	
	
}