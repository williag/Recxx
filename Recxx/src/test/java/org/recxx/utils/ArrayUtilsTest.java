package org.recxx.utils;

import junit.framework.Assert;
import org.junit.Test;

/**
 * Created by Shaine Ismail.
 * User: sismail
 * Date: 23/08/2011
 * Time: 18:28
 * Copyright SNI-Services ltd
 */
public class ArrayUtilsTest {

    @Test
    public void testMergeArrays() throws Exception {
        String[] one = new String[]{"one", "two"};
        String[] two = new String[]{"three", "four"};
        String[] result = new String[]{"one", "two", "three", "four"};
        String[] mergedResult = ArrayUtils.mergeArrays(one, two);
        Assert.assertEquals(result[0], mergedResult[0]);
        Assert.assertEquals(result[1], mergedResult[1]);
        Assert.assertEquals(result[2], mergedResult[2]);
        Assert.assertEquals(result[3], mergedResult[3]);
    }

    @Test(expected = NullPointerException.class)
    public void testMergeIfOneOFTheArraysIsNull() throws Exception {
        String[] one = new String[]{"one", "two"};
        String[] two = new String[]{"three", "four"};
        String[] result = new String[]{"one", "two", "three", "four"};
        Assert.assertEquals(result, ArrayUtils.mergeArrays(one, null));
        Assert.assertEquals(0, ArrayUtils.mergeArrays(null, null).length);
        Assert.assertEquals(two, ArrayUtils.mergeArrays(null, two));
    }

    @Test
    public void testMergeIfOneOFTheArraysIsEmpty() throws Exception {
        String[] one = new String[]{"one", "two"};
        String[] two = new String[]{"three", "four"};
        String[] empty = new String[]{};
        String[] result = new String[]{"one", "two", "three", "four"};
        Assert.assertEquals(one.length, ArrayUtils.mergeArrays(one, empty).length);
        Assert.assertEquals(0, ArrayUtils.mergeArrays(empty, empty).length);
        Assert.assertEquals(two.length, ArrayUtils.mergeArrays(empty, two).length);
    }

}
