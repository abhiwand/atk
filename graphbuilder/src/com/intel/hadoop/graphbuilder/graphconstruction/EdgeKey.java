package com.intel.hadoop.graphbuilder.graphconstruction;

import com.intel.hadoop.graphbuilder.util.Triple;

/**
 * The EdgeKey class is used in the TextGraphReducer to detect and handle duplicate edges.
 * Edges are considered duplicate if they have identical source, destination and label.
 * (Property maps are not used in comparison, rather, if two identical edges are found, some rule is used
 * to combine their property maps.)
 *
 * @see com.intel.hadoop.graphbuilder.graphconstruction.outputmrjobs.textgraph.TextGraphReducer
 */
public class EdgeKey {

    private Triple<Object, Object, Object> triple = null;

    /**
     * Construct an EdgeKey
     *
     * @param src
     * @param dst
     * @param label
     */

    public EdgeKey(Object src, Object dst, Object label) {
        triple = new Triple(src, dst, label);
    }

    /**
     * @return the getSrc object
     */
    public Object getSrc() {
        return triple.getA();
    }

    /**
     * @return the dest object
     */
    public Object getDst() {
        return triple.getB();
    }

    /**
     * @return the label.
     */
    public Object getLabel() {
        return triple.getC();
    }

    /**
     * @param val the new value for the getSrc.
     */
    public void setSrc(Object val) {
        triple.setA(val);
    }

    /**
     * @param val the new value for the destination.
     */
    public void setDst(Object val) {
        triple.setB(val);
    }

    /**
     * @param val the new value for the label.
     */
    public void setLabel(Object val) {
        triple.setC(val);
    }

    /**
     * @return reverse first two coordinates
     */
    public EdgeKey reverseEdge() {
        return new EdgeKey(this.getDst(), this.getSrc(), this.getLabel());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof EdgeKey) {
            EdgeKey k = (EdgeKey) obj;
            return this.triple.equals(k.triple);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return triple.hashCode();
    }

    @Override
    public String toString() {
        return triple.toString();
    }
}
