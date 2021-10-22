import picocli.CommandLine;
import picocli.CommandLine.Option;

class Tar {
    @Option(names = {"-l", "--local"}, description = "")
    boolean isLocal;

    @Option(names = { "-i", "--input" }, paramLabel = "inputFilename", description = "")
    String inputFilename;

    @Option(names = { "-mp", "--minSupPercent" }, description = "")
    float minSupPercent = -1;

    @Option(names = { "-mc", "--minSupCount" }, description = "")
    long minSupCount = -1;
}

public class Main {
    public static void main(String[] args) {
        Tar tar = new Tar();
        new CommandLine(tar).parseArgs(args);
        System.out.println(tar.inputFilename);
        System.out.println(tar.isLocal);
        System.out.println(tar.minSupPercent);
        System.out.println(tar.minSupCount);

    }
}
