import org.junit.Test;

public class MainTest {
    @Test
    public void testSmall(){
        Main.main(new String[]{"-i", "datasets/small.txt",  "-l", "-mc", "1"});
    }

    @Test
    public void testChess(){
        Main.main(new String[]{"-i", "datasets/chess.dat.txt",  "-l", "-mc", "3000"});
    }
}