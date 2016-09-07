package communitydetection;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ac318 on 12/12/15.
 */
public class MutualityDetectionMapper extends Mapper<LongWritable, Text, IntIntPair, IntIntPair> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        //parse text. Format: 'followed follower'
        String[] parts = value.toString().split("\\s+");

        int idFollowed = Integer.parseInt(parts[0]);
        int idFollower = -1;

        if(parts.length == 2)
        {
            idFollower = Integer.parseInt(parts[1]);
        }

        if(idFollower > -1) {

            int min = idFollowed;
            int max = idFollower;
            if(idFollower < idFollowed) {
                min = idFollower;
                max = idFollowed;
            }

            context.write(new IntIntPair(min, max), new IntIntPair(idFollowed, idFollower));
        }
    }
}
