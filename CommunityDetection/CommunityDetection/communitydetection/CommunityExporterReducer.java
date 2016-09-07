package communitydetection;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ac318 on 12/13/15.
 */
public class CommunityExporterReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

    Text result = new Text();
    IntWritable community = new IntWritable();

    // outputs the communities
    public void reduce(IntWritable nid, Iterable<IntWritable> nodes, Context context) throws IOException, InterruptedException {

        StringBuilder s = new StringBuilder();
        int count = 0;
        for (IntWritable i : nodes) {
            s.append(i.get());
            s.append(",");

            count = count + 1;
        }

        // output at least triangles, so communities with at least 2 users
        if(count > 1) {
            result.set(s.toString());
            community.set(nid.get());

            context.write(community, result);
        }
    }
}
