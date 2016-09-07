package communitydetection;

import java.util.*;

/**
 * Created by ac318 on 12/12/15.
 */
public class MaximalCliqueUtil {

    public TwitterNode getPivot(ArrayList<TwitterNode> nodesToTraverse, ArrayList<Integer> inputNodeNeighbours) {

        if(inputNodeNeighbours.size() == 0) {
            return new TwitterNode();
        }

        Map<Integer, TwitterNode> map = new HashMap<Integer, TwitterNode>();
        int maxNeighbours = 0;

        for(TwitterNode node : nodesToTraverse) {
            int count =0;
            int[] neighbours = node.getNeighbours();
            for(int i = 0; i < neighbours.length; i++) {
                if(inputNodeNeighbours.contains(neighbours[i])) {
                    count = count + 1;
                }
            }

            map.put(count, node);
        }

        for (Map.Entry<Integer, TwitterNode> entry : map.entrySet()) {
            if (entry.getKey() > maxNeighbours) {
                maxNeighbours = entry.getKey();
            }
        }

        if(maxNeighbours > 0) {
            return map.get(maxNeighbours);
        }

        return new TwitterNode();
    }

    public ArrayList<TwitterNode> intersect(ArrayList<TwitterNode> array1, ArrayList<Integer> array2) {
        ArrayList<TwitterNode> result = new ArrayList<TwitterNode>();

        for(TwitterNode s : array1) {
            if(array2.contains(s.getId())) {
                result.add(s);
            }
        }

        return result;
    }

    public ArrayList<TwitterNode> substract(ArrayList<TwitterNode> array1, ArrayList<Integer> array2) {
        ArrayList<TwitterNode> result = new ArrayList<TwitterNode>();

        for(TwitterNode s : array1) {
            if(!array2.contains(s.getId())) {
                result.add(s);
            }
        }

        return result;
    }

    public ArrayList<TwitterNode> union(ArrayList<TwitterNode> array1, ArrayList<TwitterNode> array2) {
        Set<TwitterNode> hashSet = new HashSet<TwitterNode>();

        for(TwitterNode s1 : array1) {
            hashSet.add(s1);
        }

        for(TwitterNode s2 : array2) {
            hashSet.add(s2);
        }

        return new ArrayList<TwitterNode>(hashSet);
    }

    public ArrayList<Integer> union(ArrayList<Integer> array1, TwitterNode node) {
        Set<Integer> hashSet = new HashSet<Integer>();

        for(Integer s : array1) {
            hashSet.add(s);
        }

        hashSet.add(node.getId());

        return new ArrayList<Integer>(hashSet);
    }
}
