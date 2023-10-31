package com.aerospike.firefly.io.utils;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public class ExceptionMessages {
    public static final String RECORD_TOO_BIG = "Error: Vertex / Edge exceeded max size. " +
            "This can be due to too many Properties / Edges added to an Element. Consider breaking " +
            "this Vertex / Edge into more Elements.";
    public static final String ELEMENT_NOT_FOUND = "Error: Element was dropped and no longer exists.";
}
