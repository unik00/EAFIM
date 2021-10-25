package eafim;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import utils.ArrayUtils;

import java.util.Arrays;

import static utils.ArrayUtils.stringToArray;

public class Miner {
    int minSup;
    JavaRDD<int[]> inputRdd;
    JavaSparkContext sparkContext;
    String inputName;

    public Miner(String inputName, int minSup, JavaSparkContext sc){
        this.inputName = inputName;
        this.minSup = minSup;
        sparkContext = sc;
    }

    public int run(){
        JavaRDD<String> rawTrans = sparkContext.textFile(inputName, 60).cache();
        inputRdd = rawTrans.map(ArrayUtils::stringToArray);

        int k = 1;
        boolean converged = false;
        Broadcast<int[][]> previousFrequent = sparkContext.broadcast(new int[1][]);

        int totalFrequents = 0;

        while (!converged){
            System.out.println("Mining " + k + " itemsets...");
            int[][] currentFrequents = FrequentFinder.findFrequents(inputRdd, previousFrequent, k, minSup);
            System.out.println(Arrays.deepToString(currentFrequents));
            totalFrequents += currentFrequents.length;
            if (currentFrequents.length == 0) converged = true;
            else {
                /*
                TODO: implement updateInputRDD
                if (currentFrequents.length < previousFrequent.getValue().length){
                    inputRdd = InputRDDUpdater.updateInputRDD(inputRdd, currentFrequents, k, minSup);
                }
                 */
                previousFrequent = sparkContext.broadcast(currentFrequents);
                k++;
            }
            System.out.println("Finished mining " + k + " itemsets.");
        }
        System.out.println("Total frequent itemsets: " + totalFrequents);
        return totalFrequents;
    }
}
