package com.intel.hadoop.graphbuilder.graphelements;

import com.intel.hadoop.graphbuilder.util.Triple;

/**
 * The identifying information of an edge: Its source, its destination, and its label.
 *
 * <p>
 * It is used in the initial reducer of an {@code GraphGenerationJob}to detect and handle duplicate edges.
 * Edges are considered duplicate if they have identical source, destination and label.
 * Property maps are <i>not</i> used for purposes of comparison.
 *    </p>
 * @see com.intel.hadoop.graphbuilder.pipeline.output.textgraph.TextGraphReducer
 * @see com.intel.hadoop.graphbuilder.pipeline.output.titan.EdgesIntoTitanReducer
 */
public class EdgeID {

    private Triple<Object, Object, Object> triple = null;


    /**
     * Construct {@code EdgeID} from parameters.
     * @param src  the source vertex of the edge
     * @param dst  the destination vertex of the edge
     * @param label the label of the edge
     */
    public EdgeID(Object src, Object dst, Object label) {
        triple = new Triple(src, dst, label);
    }

    /**
     * @return the source vertex of the edge
     */
    public Object getSrc() {
        return triple.getA();
    }

    /**
     * @return the destination vertex of the edge
     */
    public Object getDst() {
        return triple.getB();
    }

    /**
     * @return the label of the edge
     */
    public Object getLabel() {
        return triple.getC();
    }

    /**
     * @param val the new value for EdgeID's source vertex.
     */
    public void setSrc(Object val) {
        triple.setA(val);
    }

    /**
     * @param val the new value for EdgeID's destination vertex.
     */
    public void setDst(Object val) {
        triple.setB(val);
    }

    /**
     * @param val the new value for EdgeID's label
     */
    public void setLabel(Object val) {
        triple.setC(val);
    }

    /**
     * Reverse the direction of the edge.
     * @return New edge whose source is the destination of the base edge, whose destination is the source of the base edge,
     * and whose label is the same label as the base edge..
     */
    public EdgeID reverseEdge() {
        return new EdgeID(this.getDst(), this.getSrc(), this.getLabel());
    }

    /**
     * Equality test: Is the other object an {@code EdgeID} whose components are the same objects as this one?
     * @param obj  any old object
     * @return
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof EdgeID) {
            EdgeID k = (EdgeID) obj;
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
