package eafim;

import org.junit.Test;

import static org.junit.Assert.*;

public class HashTreeTest {
    @Test
    public void testBuild(){
        HashTree tree = HashTree.build(new int[][]{new int[]{1, 2, 3}, new int[]{1, 2, 4}, new int[]{2, 3, 1}});
//        assertTrue(tree.contains(new int[]{1, 2, 3}));
//        assertFalse(tree.contains(new int[]{1, 2, 5}));
//        assertTrue(tree.contains(new int[]{2, 3, 1}));
//        assertFalse(tree.contains(new int[]{2, 3, 3}));

    }
}