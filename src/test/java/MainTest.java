import org.junit.Test;

public class MainTest {
    @Test
    public void clgt(){
        Main.main(new String[]{"-i", "chess.dat",  "-l"});
    }
}