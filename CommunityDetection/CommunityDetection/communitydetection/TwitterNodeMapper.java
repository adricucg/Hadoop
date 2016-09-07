package communitydetection;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by ac318 on 12/12/15.
 */
public class TwitterNodeMapper extends Mapper<LongWritable, Text, IntWritable, IntIntPair> {

    private IntWritable followed = new IntWritable();
    private IntWritable follower = new IntWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        //parse text. Format: 'followed follower'
        String[] parts = value.toString().split("\\s+");

        int idFollowed = Integer.parseInt(parts[0]);
        int idFollower = Integer.parseInt(parts[1]);

        followed.set(idFollowed);
        follower.set(idFollower);

        context.write(followed, new IntIntPair(idFollower, 1));
        context.write(follower, new IntIntPair(idFollowed, 0));
    }

}

