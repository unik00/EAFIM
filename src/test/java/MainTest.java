import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MainTest {
    @Test
    public void testSmall(){
        Main.main(new String[]{"-i", "datasets/small.txt",  "-l", "-mc", "1"});
    }

    @Test
    public void testChess(){
        assertEquals(Main.main(new String[]{"-i", "datasets/chess.dat.txt",  "-l", "-mc", "2700"}), 3134);
    }

    @Test
    public void testRetail(){
        Main.main(new String[]{"-i", "datasets/retail.dat",  "-l", "-mc", "100"});
    }

    @Test
    public void testMushroom(){
        Main.main(new String[]{"-i", "datasets/mushroom.dat",  "-l", "-mc", "2500"});
    }

}