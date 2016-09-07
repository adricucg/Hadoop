package communitydetection;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by ac318 on 12/12/15.
 */
public class MaximalCliqueReducer extends Reducer<IntWritable, TwitterNode, IntWritable, Text> {

    private IntWritable keyId = new IntWritable();
    private Text maximalClique = new Text();
    private MaximalCliqueUtil maxUtil = new MaximalCliqueUtil();

    // Enumerates the maximal cliques containing the input vertex
    public void reduce(IntWritable nid, Iterable<TwitterNode> neighbours, Context context) throws IOException, InterruptedException {

        ArrayList<Integer> currentClique = new ArrayList<Integer>();
        ArrayList<TwitterNode> inputNodeNeighbours = new ArrayList<TwitterNode>();
        ArrayList<Integer> neighboursIds = new ArrayList<Integer>();
        ArrayList<TwitterNode> visitedNodes = new ArrayList<TwitterNode>();
        ArrayList<TwitterNode> nodesToIterate;

        // current clique starts as the input vertex id
        currentClique.add(nid.get());

        int inputNodeDegree = 0;

        for (TwitterNode neighbour : neighbours) {
            TwitterNode newNode = new TwitterNode();
            newNode.set(neighbour.getId(), neighbour.getNeighbours());
            inputNodeNeighbours.add(newNode);

            neighboursIds.add(neighbour.getId());

            inputNodeDegree = inputNodeDegree + 1;
        }

        nodesToIterate = new ArrayList<TwitterNode>(inputNodeNeighbours);

        // pruning by degree ordering
        for (TwitterNode neighbour : nodesToIterate) {
            int degree = neighbour.getNeighbours().length;
            int intNodeId = neighbour.getId();
            int inputNodeId = nid.get();

            // degree ordering
            if (degree < inputNodeDegree || (degree == inputNodeDegree && intNodeId < inputNodeId)) {

                neighboursIds.remove(new Integer(intNodeId));
                inputNodeNeighbours.remove(neighbour);
                visitedNodes.add(neighbour);
            }
        }

        enumerateMaximalClique(
                currentClique,
                inputNodeNeighbours,
                neighboursIds,
                visitedNodes,
                nid.get(),
                context);
    }

    private void enumerateMaximalClique(
            ArrayList<Integer> currentClique,
            ArrayList<TwitterNode> todo,
            ArrayList<Integer> neighboursIds,
            ArrayList<TwitterNode> done,
            int nid,
            Context context) throws IOException, InterruptedException {

        if(todo.size() == 0 && done.size() == 0) {

            StringBuilder maxClique = new StringBuilder();
            for(int id : currentClique) {
                maxClique.append(id);
                maxClique.append(",");
            }

            // output at least triangles
            if(currentClique.size() > 2) {
                maximalClique.set(maxClique.toString());
                keyId.set(nid);

                context.write(keyId, maximalClique);
            }

            return;
        }

        TwitterNode pivot = maxUtil.getPivot(maxUtil.union(todo, done), neighboursIds);

        ArrayList<Integer> pivotNeighbours = new ArrayList<Integer>();
        int[] pNeighbours = pivot.getNeighbours();
        for(int i = 0; i < pNeighbours.length; i++) {
            pivotNeighbours.add(pNeighbours[i]);
        }

        ArrayList<TwitterNode> doing = maxUtil.substract(todo, pivotNeighbours);
        ArrayList<TwitterNode> currentInputNeighbours = maxUtil.intersect(todo, pivotNeighbours);
        ArrayList<Integer> currentNeighboursIds = new ArrayList<Integer>();

        for(TwitterNode s : currentInputNeighbours) {
            currentNeighboursIds.add(s.getId());
        }

        for (TwitterNode node : doing) {

            // recursive traversal
            enumerateMaximalClique(
                    maxUtil.union(currentClique, node),
                    currentInputNeighbours,
                    currentNeighboursIds,
                    maxUtil.intersect(done, pivotNeighbours),
                    nid,
                    context);

            todo.remove(node);
            done.add(node);
            currentClique.remove(new Integer(node.getId()));
        }
    }
}


