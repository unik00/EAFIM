import org.junit.Test;

public class MainTest {
    @Test
    public void testMain(){
        Main.main(new String[]{"-i", "chess.dat",  "-l"});
    }
}