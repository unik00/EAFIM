package eafim;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import utils.ArrayUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

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
        inputRdd = rawTrans.map(ArrayUtils::stringToSortedArray).cache();

        boolean converged = false;

        int totalFrequents = 0;
        HashMap<Integer, Integer> singletonOrder = new HashMap<>();

        int minSupCopy = this.minSup;
        int[][] previousFrequent;
        List<Tuple2<Integer, Integer>> items = inputRdd.flatMap(arr -> ArrayUtils.primitiveArrayToArrayList(arr).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum)
                .filter(pair -> pair._2 >= minSupCopy)
                .collect();

        for(Tuple2<Integer, Integer> pair: items){
            singletonOrder.put(pair._1, pair._2);
        }

        previousFrequent = new int[items.size()][];
        int i = 0;
        for(Tuple2<Integer, Integer> tp: items) previousFrequent[i++] = new int[]{tp._1};
        Arrays.sort(previousFrequent, (x, y) -> InputRDDUpdater.ascendingSupport(x[0], y[0], singletonOrder));

        totalFrequents += previousFrequent.length;
        System.out.println(Arrays.deepToString(previousFrequent));

        InputRDDUpdater.updateInputRDD(this,
                previousFrequent,
                1,
                sparkContext.broadcast(singletonOrder),
                sparkContext);

        int k = 2;

        while (!converged){
            System.out.println("Mining " + k + " itemsets...");

            System.out.println("Finding frequents...");
            int[][] currentFrequents = FrequentFinder.findFrequents(inputRdd, previousFrequent, k, minSup);
            System.out.println(currentFrequents.length + " frequent itemsets.");
            System.out.println(Arrays.deepToString(currentFrequents));
            totalFrequents += currentFrequents.length;
            if (currentFrequents.length == 0) converged = true;
            else {
                if (currentFrequents.length < previousFrequent.length) {
                    System.out.println("Updating Input RDD...");
                    InputRDDUpdater.updateInputRDD(this,
                            currentFrequents,
                            k,
                            sparkContext.broadcast(singletonOrder),
                            sparkContext);
                }
                previousFrequent = currentFrequents;
                k++;
            }
            System.out.println("Finished mining " + (k-1) + " itemsets.");
        }
        System.out.println("Total frequent itemsets: " + totalFrequents);
        return totalFrequents;
    }
}
