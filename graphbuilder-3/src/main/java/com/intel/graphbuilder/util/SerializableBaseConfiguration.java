// TODO: Copyright is Apache on this one

package com.intel.graphbuilder.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.map.LinkedMap;
import org.apache.commons.configuration.AbstractConfiguration;

/**
 * An implementation that implements Serializable that was
 * otherwise copied from Apache's BaseConfiguration.
 */
public class SerializableBaseConfiguration extends AbstractConfiguration implements Serializable {

    /** stores the configuration key-value pairs */
    private Map store = new LinkedMap();

    /**
     * Adds a key/value pair to the map. This routine does no magic morphing. It ensures the keylist is maintained
     * 
     * @param key key to use for mapping
     * @param value object to store
     */
    protected void addPropertyDirect(String key, Object value) {
        Object previousValue = getProperty(key);

        if (previousValue == null) {
            store.put(key, value);
        } else if (previousValue instanceof List) {
            // the value is added to the existing list
            ((List) previousValue).add(value);
        } else {
            // the previous value is replaced by a list containing the previous value and the new value
            List list = new ArrayList();
            list.add(previousValue);
            list.add(value);

            store.put(key, list);
        }
    }

    /**
     * Read property from underlying map.
     * 
     * @param key key to use for mapping
     * 
     * @return object associated with the given configuration key.
     */
    public Object getProperty(String key) {
        return store.get(key);
    }

    /**
     * Check if the configuration is empty
     * 
     * @return <code>true</code> if Configuration is empty, <code>false</code> otherwise.
     */
    public boolean isEmpty() {
        return store.isEmpty();
    }

    /**
     * check if the configuration contains the key
     * 
     * @param key the configuration key
     * 
     * @return <code>true</code> if Configuration contain given key, <code>false</code> otherwise.
     */
    public boolean containsKey(String key) {
        return store.containsKey(key);
    }

    /**
     * Clear a property in the configuration.
     * 
     * @param key the key to remove along with corresponding value.
     */
    protected void clearPropertyDirect(String key) {
        if (containsKey(key)) {
            store.remove(key);
        }
    }

    public void clear() {
        fireEvent(EVENT_CLEAR, null, null, true);
        store.clear();
        fireEvent(EVENT_CLEAR, null, null, false);
    }

    /**
     * Get the list of the keys contained in the configuration repository.
     * 
     * @return An Iterator.
     */
    public Iterator getKeys() {
        return store.keySet().iterator();
    }

}
