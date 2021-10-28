package utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ArrayUtilsTest {

    @Test
    public void testArrayToString(){
        String a = ArrayUtils.arrayToString(new int[]{1, 3, 3});
        String b = "1 3 3";
        assertEquals(a, b);
    }
}