

package com.intel.hadoop.graphbuilder.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;

/**
 *  The PropertyMap class implements a property map.
 *  MapWritable with a friendly toString() method.
 */
public class PropertyMap implements Writable
{
    private MapWritable properties = null;

    public PropertyMap() {
        this.properties = new MapWritable();
    }

    public void setProperty(final String key, final Writable value) {
        StringType keyStr = new StringType(key);
        this.properties.put(keyStr, value);
    }

    public Writable removeProperty(final String key) {
        StringType keyStr = new StringType(key);
        return properties.remove(keyStr);
    }

    public Writable getProperty(final String key) {
        StringType keyStr = new StringType(key);
        return this.properties.get(keyStr);
    }

    public Set<Writable> getPropertyKeys() {
        return this.properties.keySet();
    }

    public void mergeProperties(PropertyMap propertyMap) {
        for(Writable key : propertyMap.getPropertyKeys()) {
            this.setProperty(key.toString(), propertyMap.getProperty(key.toString()));
        }
    }

    @Override
    public  void readFields(DataInput in) throws IOException {
        this.properties.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.properties.write(out);
    }


    @Override
    public int hashCode() {
        return properties.hashCode();
    }

    @Override
    public String toString() {

        String s = new String("");

        if (!properties.isEmpty())
        {
            s.concat("[" + "\t");

            for (Map.Entry<Writable, Writable> entry : properties.entrySet())
            {
               s += (entry.getKey() + ":" + entry.getValue()) + "\t";
            }

            s.concat("\t" + "]");
        }

        return s;
    }
}
