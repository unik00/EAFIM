package eafim;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class CandidateGeneratorTest {
    @Test
    public void testGen(){
        int[][] frequents = new int[][]{new int[]{1, 2, 4}, new int[]{1, 2, 3}, new int[]{2, 3, 1}};
        assertTrue(Arrays.deepEquals(CandidateGenerator.gen(frequents), new int[][]{new int[]{1, 2, 4, 3}}));
    }
}