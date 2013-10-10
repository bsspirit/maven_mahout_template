package org.conan.mymahout.cluster06;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.clustering.kmeans.KMeansClusterer;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.Vector;

public class Kmeans {

    public static void main(String[] args) throws IOException {
        List<Vector> sampleData = MathUtil.readFileToVector("datafile/randomData.csv");

        int k = 3;
        double threshold = 0.01;

        List<Vector> randomPoints = MathUtil.chooseRandomPoints(sampleData, k);
        for (Vector vector : randomPoints) {
            System.out.println("Init Point center: " + vector);
        }

        List<Cluster> clusters = new ArrayList<Cluster>();
        for (int i = 0; i < k; i++) {
            clusters.add(new Cluster(randomPoints.get(i), i, new EuclideanDistanceMeasure()));
        }

        List<List<Cluster>> finalClusters = KMeansClusterer.clusterPoints(sampleData, clusters, new EuclideanDistanceMeasure(), k, threshold);
        for (Cluster cluster : finalClusters.get(finalClusters.size() - 1)) {
            System.out.println("Cluster id: " + cluster.getId() + " center: " + cluster.getCenter().asFormatString());
        }
    }

}
