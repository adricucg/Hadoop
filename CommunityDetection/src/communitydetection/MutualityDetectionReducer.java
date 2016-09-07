package communitydetection;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ac318 on 12/12/15.
 */
public class MutualityDetectionReducer extends Reducer<IntIntPair, IntIntPair, IntWritable, IntWritable> {

    // For emitting an edge that implies mutuality: v follows u and u follows v
    // discard entry otherwise

    public void reduce(IntIntPair key, Iterable<IntIntPair> pairs, Context context) throws IOException, InterruptedException {

        int count = 0;
        for (IntIntPair pair : pairs) {
            count = count + 1;
        }

        if(count > 1) {
            context.write(key.getFirst(), key.getSecond());
            context.write(key.getSecond(), key.getFirst());
        }
    }
}
