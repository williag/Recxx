package org.recxx;

import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Shaine Ismail.
 * User: sismail
 * Date: 23/08/2011
 * Time: 07:10
 * Copyright SNI-Services ltd
 */
public class AbstractRecFeedTest {
    @Test
    public void testCheckKeyColumnsWithMatchingRecords() throws Exception {
        List<String> key = new ArrayList<String>();
        key.add("test1");
        key.add("test2");
        List<String> columns = new ArrayList<String>();
        columns.add("test1");
        columns.add("test2");
        boolean test = AbstractRecFeed.keysPresentInColumns(key.toArray(new String[]{}), columns.toArray(new String[]{}));
        Assert.assertTrue(test);
    }

    @Test
    public void testCheckKeyColumnsWithMatchingWithMissingKey() throws Exception {
        List<String> key = new ArrayList<String>();
        key.add("test1");
        key.add("test2");
        List<String> columns = new ArrayList<String>();
        columns.add("test1");
        boolean test = AbstractRecFeed.keysPresentInColumns(key.toArray(new String[]{}), columns.toArray(new String[]{}));
        Assert.assertFalse(test);
    }

    @Test
    public void testDelimiterSplit() throws Exception {
        String line = "one two three four";
        String[] items = AbstractRecFeed.convertStringKeyToArray(line);
        Assert.assertEquals("one", items[0]);
        Assert.assertEquals("two", items[1]);
        Assert.assertEquals("three", items[2]);
        Assert.assertEquals("four", items[3]);

    }

    @Test
    public void testDelimiterSplitWillNullDelimiter() throws Exception {
        String line = "one two three four";
        String[] items = AbstractRecFeed.convertStringKeyToArray(line, null);
        Assert.assertEquals("one", items[0]);
        Assert.assertEquals("two", items[1]);
        Assert.assertEquals("three", items[2]);
        Assert.assertEquals("four", items[3]);
    }


    @Test
    public void testIndexOfKeyColumns() throws Exception {
        List<String> key = new ArrayList<String>();
        key.add("test1");
        key.add("test2");
        List<String> columns = new ArrayList<String>();
        columns.add("test1");
        columns.add("test2");

        List<Integer> test = AbstractRecFeed.getColumnsPosition(key.toArray(new String[]{}), columns.toArray(new String[]{}));
        Assert.assertEquals(0, test.get(0).intValue());
        Assert.assertEquals(1, test.get(1).intValue());

    }
}
