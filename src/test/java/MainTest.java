import org.junit.Test;

import static java.lang.Math.min;
import static org.junit.Assert.assertEquals;

public class MainTest {
    private void test(int[] minSupCounts, int[] outputs, String file, Integer... limit){
        int n = min(limit.length > 0 ? limit[0] : 1000000000, minSupCounts.length);
        for(int i = 0; i < n; i++) {
            assertEquals(Main.main(new String[]{"-i", file, "-l", "-mc", Integer.toString(minSupCounts[i])}), outputs[i]);
        }
    }

    @Test
    public void testChess(){
        int[] minSupCounts = new int[]{3000, 2700, 2500, 2000};
        int[] outputs = new int[]{155, 3134, 11493, 166580};
        test(minSupCounts, outputs, "datasets/chess.dat.txt", 2);
    }

    @Test
    public void testRetail(){
        int[] inputs = new int[]{1000, 500};
        int[] outputs = new int[]{135, 468};
        test(inputs, outputs, "datasets/retail.dat");

    }

    @Test
    public void testConnect(){
        int[] minSupCounts = new int[]{66666, 66000, 60000, 59000, 58000, 40534, 30400, 27022};
        int[] outputs = new int[]{58, 261, 41143, 69847, 109663, 21252795, 173538667, 339987447};
        test(minSupCounts, outputs, "datasets/connect.dat", 2);
    }

    @Test
    public void testMushroom(){
        int[] inputs = new int[]{500};
        int[] outputs = new int[]{2365};
        test(inputs, outputs, "datasets/mushroom.dat");
    }

    @Test
    public void testSmall(){
        int[] inputs = new int[]{1, 2};
        int[] outputs = new int[]{15, 7};
        test(inputs, outputs, "datasets/small.txt");
    }

    /*
    TODO: increase stack size (Stack overflow)
    @Test
    public void testWebdocs(){
        int[] inputs = new int[]{400000};
        int[] outputs = new int[]{2365};
        test(inputs, outputs, "datasets/webdocs.dat");
    }
     */
}