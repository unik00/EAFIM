import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import picocli.CommandLine;
import picocli.CommandLine.Option;

class EAFIMConfig {
    @Option(names = {"-l", "--local"}, description = "local mode")
    boolean isLocal;

    @Option(names = { "-i", "--input" }, paramLabel = "inputFilename", description = "input file name")
    String inputFilename;

    @Option(names = { "-mc", "--minSupCount" }, description = "min sup count (in integer)")
    long minSupCount = -1;

    @Override
    public String toString() {
        return "EAFIMConfig{" +
                "isLocal=" + isLocal +
                ", inputFilename='" + inputFilename + '\'' +
                ", minSupCount=" + minSupCount +
                '}';
    }
}

public class Main {
    public static void main(String[] args) {
        EAFIMConfig eafimConfig = new EAFIMConfig();
        new CommandLine(eafimConfig).parseArgs(args);

        Logger.getLogger("org").setLevel(Level.toLevel("error"));
        Logger.getLogger("akka").setLevel(Level.toLevel("error"));
        SparkConf sparkConf = new SparkConf().setAppName("BIGMiner").set("spark.driver.maxResultSize", "40g").set("spark.memory.storageFraction", "0.1").set("spark.driver.cores", "4");
        if (eafimConfig.isLocal) sparkConf.setMaster("local[*]");

        System.out.println(eafimConfig);
    }
}
