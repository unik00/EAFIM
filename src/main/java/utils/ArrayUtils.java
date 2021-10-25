package utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ArrayUtils {
    public static int[] listToPrimitiveArray(List<Integer> in){
        if (in == null) return new int[0];

        int[] result = new int[in.size()];
        for(int i = 0; i < in.size(); i++) result[i] = in.get(i);
        return result;
    }

    public static ArrayList<Integer> primitiveArrayToArrayList(int[] a){
        if (a == null) return new ArrayList<>();

        ArrayList<Integer> result = new ArrayList<>();
        for (int x : a) result.add(x);
        return result;
    }

    public static int[] stringToArray(String s){
        return Arrays.stream(s.split(" ")).mapToInt(Integer::parseInt).toArray();
    }
}
