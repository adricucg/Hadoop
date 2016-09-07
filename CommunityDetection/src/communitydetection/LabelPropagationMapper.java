package communitydetection;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by ac318 on 12/12/15.
 */

public class LabelPropagationMapper extends Mapper<IntWritable, TwitterNode, IntWritable, IntIntPair> {

    private IntWritable nid = new IntWritable();

    public void map(IntWritable key, TwitterNode node, Context context) throws IOException, InterruptedException{

        int[] neighbours = node.getNeighbours();
        for(int i = 0; i< neighbours.length; i++) {
            nid.set(neighbours[i]);

            if(node.getCommunityId() != 0) {
                context.write(nid, new IntIntPair(node.getCommunityId(), key.get()));
            } else {
                // emit the actual vertex id as the community id
                context.write(nid, new IntIntPair(key.get(), key.get()));
            }
        }
    }
}
