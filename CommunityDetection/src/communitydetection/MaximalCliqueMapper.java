package communitydetection;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by ac318 on 12/12/15.
 */
public class MaximalCliqueMapper extends Mapper<IntWritable, TwitterNode, IntWritable, TwitterNode> {

    private IntWritable neighbourKey = new IntWritable();

    public void map(IntWritable nid, TwitterNode node, Context context) throws IOException, InterruptedException {

        int[] allNeighbours = node.getNeighbours();

        for(int neighbour : allNeighbours) {

            neighbourKey.set(neighbour);
            context.write(neighbourKey, node);
        }
    }
}

