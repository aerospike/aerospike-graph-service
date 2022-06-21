package com.aerospike.firefly;

import static org.junit.Assert.fail;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class TestHelper {
    public static <T extends Throwable> void assertThrows(Class<T> exceptionType, LambdaFunc func) {
        try {
            func.func();
            fail("Expected exception to be thrown.");
        } catch (Throwable actualException) {
            if (!exceptionType.isInstance(actualException)) {
                fail("Exception type thrown (" + actualException.getClass().getName() + ") does not match exception type expected (" + exceptionType.getName() + "). Exception message: '" + actualException.getMessage() + "'.");
            }
        }
    }

    public interface LambdaFunc {
        void func();
    }
}
