package com.flipkart.yak.sep;

/**
 * Marker typed exception for issues in SerDe
 * 
 * @author gokulvanan.v
 *
 */
public class SerDeException extends Exception {

    public SerDeException(Exception e) {
        super(e);
    }

    /**
     * 
     */
    private static final long serialVersionUID = -2165549081156905507L;

}
