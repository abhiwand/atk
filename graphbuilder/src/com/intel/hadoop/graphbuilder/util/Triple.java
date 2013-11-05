package com.intel.hadoop.graphbuilder.util;

import java.util.Objects;

/**
 * Represents a triple of objects.
 *
 *
 * @param <A>
 * @param <B>
 * @param <C>
 */
public class Triple<A, B, C> {

    /**
     * Construct a triple
     *
     * @param a
     * @param b
     * @param c
     */
    public Triple(A a, B b, C c) {
        this.a = a;
        this.b = b;
        this.c = c;
    }

    /**
     * @return the A value.
     */
    public A getA() {
        return this.a;
    }

    /**
     * @return the B value.
     */
    public B getB() {
        return this.b;
    }

    /**
     * @return the C value.
     */
    public C getC() {
        return this.c;
    }

    /**
     * @param val
     *          the new value for the A value.
     */
    public void setA(A val) {
        this.a = val;
    }

    /**
     * @param val
     *          the new value for the B value.
     */
    public void setB(B val) {
        this.b = val;
    }

    /**
     * @param val
     *          the new value for the C value.
     */
    public void setC(C val) {
        this.c = val;
    }

    /**
     * @return reverse first two coordinates
     */
    public Triple<B, A, C> swapAB() {
        return new Triple<B, A, C>(b, a, c);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Triple) {
            Triple t = (Triple) obj;
            return Objects.equals(t.a,a)
                   && Objects.equals(t.b,b)
                   && Objects.equals(t.c,c);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return HashUtil.hashtriple(a,b,c);
    }

    @Override
    public String toString() {
        return "(" + a + ", " + b + ", " + c + ")";
    }

    private A a;
    private B b;
    private C c;
}

