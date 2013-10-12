package org.conan.mymahout.recommendation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;
import org.conan.mymahout.hdfs.HdfsDAO;

public class UserCFHadoop {

    private static final String HDFS = "hdfs://192.168.1.210:9000";

    final static int NEIGHBORHOOD_NUM = 2;
    final static int RECOMMENDER_NUM = 3;

    public static void main(String[] args) throws Exception {
        String localFile = "datafile/item.csv";
        String inPath = HDFS + "/user/hdfs/userCF";
        String inFile = inPath + "/item.csv";
        String outPath = HDFS + "/user/hdfs/userCF/result/";
        String outFile = outPath + "/part-r-00000";
        String tmpPath = HDFS + "/tmp/" + System.currentTimeMillis();

        Configuration conf = config();
        HdfsDAO hdfs = new HdfsDAO(conf);
        hdfs.rmr(inPath);
        hdfs.copyFile(localFile, inPath);
        hdfs.cat(inFile);

        StringBuilder sb = new StringBuilder();
        sb.append("--input ").append(inPath);
        sb.append(" --output ").append(outPath);
        sb.append(" --booleanData true");
        sb.append(" --similarityClassname org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.EuclideanDistanceSimilarity");
        sb.append(" --tempDir ").append(tmpPath);
        args = sb.toString().split(" ");

        RecommenderJob job = new RecommenderJob();
        job.setConf(conf);
        job.run(args);

        hdfs.cat(outFile);
    }

    public static JobConf config() {
        JobConf conf = new JobConf(UserCFHadoop.class);
        conf.setJobName("UserCFHadoop");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        return conf;
    }

}
