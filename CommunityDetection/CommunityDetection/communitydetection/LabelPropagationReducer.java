package communitydetection;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ac318 on 12/12/15.
 */
public class LabelPropagationReducer extends Reducer<IntWritable, IntIntPair, IntWritable, TwitterNode> {

    private TwitterNode resultNode;

    // computes a new label and propagates that to the neighbours
    public void reduce(IntWritable nid, Iterable<IntIntPair> neighbours, Context context) throws IOException, InterruptedException {

        HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
        ArrayList<Integer> adjList = new ArrayList<Integer>();


       for(IntIntPair pair : neighbours) {
           // compute the most frequent label
           int label = pair.getFirst().get();
           if(map.containsKey(label)) {
               int count = map.get(label);
               map.put(label, count + 1);
           } else {
               map.put(label, 1);
           }

           adjList.add(new Integer(pair.getSecond().get()));
       }

        int maxCount = 1;
        int maxKey = -1;
        for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
            if (entry.getValue() > maxCount) {
                maxCount = entry.getValue();
                maxKey = entry.getKey();
            }
        }

        int[] adjListInt = new int[adjList.size()];
        for(int i = 0; i < adjList.size(); i++) {
            adjListInt[i] = adjList.get(i);
        }

        int newLabel;
        if(maxCount > 1) {
            newLabel = maxKey;
        } else {
            int newLabelIndex = (int)(Math.random() * adjListInt.length);
            newLabel = adjListInt[newLabelIndex];
        }

        resultNode = new TwitterNode();
        resultNode.set(nid.get(), adjListInt, newLabel);

        context.write(nid, resultNode);
    }
}
