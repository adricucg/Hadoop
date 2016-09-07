package communitydetection;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by ac318 on 12/12/15.
 */
public class TwitterNodeReducer extends Reducer<IntWritable, IntIntPair, IntWritable, TwitterNode> {

    // For emitting a node and all its neighbours
    private TwitterNode resultNode = new TwitterNode();

    public void reduce(IntWritable nid, Iterable<IntIntPair> pairs, Context context) throws IOException, InterruptedException {

        HashMap<Integer, Integer> map = new HashMap<Integer, Integer> ();
        ArrayList<Integer> neighbours = new ArrayList<Integer>();

        for (IntIntPair pair : pairs) {
            int label = pair.getFirst().get();
            if(map.containsKey(label)) {
                int count = map.get(label);
                map.put(label, count + 1);
            } else {
                map.put(label, 1);
            }
        }

        // exclude as neighbours nodes that do not present mutuality
        for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
            if (entry.getValue() == 2) {
               neighbours.add(entry.getKey());
            }
        }

        // this line is not commented when running the extractor as part of MCE algorithm
        //Collections.sort(neighbours);

        int size = neighbours.size();
        // output the node if it has at least one neighbour
        if(size > 0) {
            int[] nodes = new int[size];
            for(int i = 0; i < size; i++) {
                nodes[i] = neighbours.get(i);
            }

            resultNode.set(nid.get(), nodes);

            context.write(nid, resultNode);
        }
    }
}

