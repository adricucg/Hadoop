package communitydetection;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ac318 on 12/14/15.
 */
public class CommunityExporterMapper extends Mapper<IntWritable, TwitterNode, IntWritable, IntWritable> {

    private IntWritable community = new IntWritable();

    public void map(IntWritable key, TwitterNode node, Context context) throws IOException, InterruptedException{

        community.set(node.getCommunityId());

        context.write(community, key);
    }
}
