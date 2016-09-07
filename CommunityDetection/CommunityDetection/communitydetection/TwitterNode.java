package communitydetection;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;

/**
 * Created by ac318 on 12/12/15.
 */
public class TwitterNode implements WritableComparable<TwitterNode> {

    private IntWritable id;
    private ArrayWritable neighbours;
    private IntWritable communityId;

    public TwitterNode() {
        ArrayWritable aw = new ArrayWritable(IntWritable.class);
        aw.set(new Writable[0]);
        set(new IntWritable(), aw, new IntWritable());
    }

    public TwitterNode(IntWritable id, ArrayWritable neighbours) {
        set(id, neighbours);
    }

    public void set(IntWritable id, ArrayWritable neighbours) {
        this.id = id;
        this.neighbours = neighbours;
    }

    public void set(IntWritable id, ArrayWritable neighbours, IntWritable communityId) {
        this.id = id;
        this.neighbours = neighbours;
        this.communityId = communityId;
    }

    public void set(int id, int[] neighbours) {
        this.id.set(id);
        IntWritable[] values = new IntWritable[neighbours.length];

        for (int i = 0; i < neighbours.length; i++) {
            values[i] = new IntWritable(neighbours[i]);
        }

        this.neighbours.set(values);
    }

    public void set(int id, int[] neighbours, int communityId) {
        this.id.set(id);
        this.communityId.set(communityId);
        IntWritable[] values = new IntWritable[neighbours.length];

        for (int i = 0; i < neighbours.length; i++) {
            values[i] = new IntWritable(neighbours[i]);
        }

        this.neighbours.set(values);
    }

    public void setId(int id) {
        this.id.set(id);
    }

    public void setCommunityId(int id) {
        this.communityId.set(id);
    }

    public int getId() {
        return id.get();
    }

    public int getCommunityId() {
        return communityId.get();
    }

    public int[] getNeighbours() {
        Writable[] arr = neighbours.get();
        int[] ldest = new int[arr.length];
        for (int i = 0; i < arr.length; i++) {
            ldest[i] = ((IntWritable)arr[i]).get();
        }
        return ldest;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        id.write(out);
        neighbours.write(out);
        communityId.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id.readFields(in);
        neighbours.readFields(in);
        communityId.readFields(in);
    }

    @Override
    public int compareTo(TwitterNode node) {
        Integer self = this.getId();

        return self.compareTo(node.getId());
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TwitterNode) {
            TwitterNode node = (TwitterNode) obj;
            return this.getId() == node.getId();
        }
        return false;
    }

}


