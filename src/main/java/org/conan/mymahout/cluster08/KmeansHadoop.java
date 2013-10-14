package org.conan.mymahout.cluster08;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.canopy.CanopyDriver;
import org.apache.mahout.clustering.conversion.InputDriver;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.utils.clustering.ClusterDumper;

public class KmeansHadoop extends AbstractJob {

//    private static final String DIRECTORY_CONTAINING_CONVERTED_INPUT = "data";
//
//    public static void main(String[] args) throws Exception {
//        JobConf conf = new JobConf(KmeansHadoop.class);
//        conf.setJobName("KmeansHadoop");
//        conf.addResource("classpath:/hadoop/core-site.xml");
//        conf.addResource("classpath:/hadoop/hdfs-site.xml");
//        conf.addResource("classpath:/hadoop/mapred-site.xml");
//
//        String inPath = "/user/hdfs/mix_data/";
//        String outPath = "/user/hdfs/mix_data/result/";
//
//        int k = 3;
//        Path input = new Path(inPath);
//        Path output = new Path(outPath);
//
//        HdfsDAO hdfs = new HdfsDAO(conf);
//        hdfs.ls(inPath);
//        
//        hdfs.rmr(outPath);
//        hdfs.ls(inPath);
//        
//        run(conf, input, output, new EuclideanDistanceMeasure(), k, 0.5, 10);
//        
//        
//
//        //
//        // System.out.println("Running with default arguments");
//        // // Path output = new Path("output");
//        // // Configuration conf = new Configuration();
//        // HadoopUtil.delete(conf, output);
//        // run(conf, input, output, new EuclideanDistanceMeasure(), k, 0.5, 10);
//    }
//
//    public int run(String[] args) throws Exception {
//        return 0;
//    }

    // public int run(String[] args) throws Exception {
    // addInputOption();
    // addOutputOption();
    // addOption(DefaultOptionCreator.distanceMeasureOption().create());
    // addOption(DefaultOptionCreator.numClustersOption().create());
    // addOption(DefaultOptionCreator.t1Option().create());
    // addOption(DefaultOptionCreator.t2Option().create());
    // addOption(DefaultOptionCreator.convergenceOption().create());
    // addOption(DefaultOptionCreator.maxIterationsOption().create());
    // addOption(DefaultOptionCreator.overwriteOption().create());
    //
    // Map<String, List<String>> argMap = parseArguments(args);
    // if (argMap == null) {
    // return -1;
    // }
    //
    // Path input = getInputPath();
    // Path output = getOutputPath();
    // String measureClass =
    // getOption(DefaultOptionCreator.DISTANCE_MEASURE_OPTION);
    // if (measureClass == null) {
    // measureClass = SquaredEuclideanDistanceMeasure.class.getName();
    // }
    // double convergenceDelta =
    // Double.parseDouble(getOption(DefaultOptionCreator.CONVERGENCE_DELTA_OPTION));
    // int maxIterations =
    // Integer.parseInt(getOption(DefaultOptionCreator.MAX_ITERATIONS_OPTION));
    // if (hasOption(DefaultOptionCreator.OVERWRITE_OPTION)) {
    // HadoopUtil.delete(getConf(), output);
    // }
    // DistanceMeasure measure = ClassUtils.instantiateAs(measureClass,
    // DistanceMeasure.class);
    // if (hasOption(DefaultOptionCreator.NUM_CLUSTERS_OPTION)) {
    // int k =
    // Integer.parseInt(getOption(DefaultOptionCreator.NUM_CLUSTERS_OPTION));
    // run(getConf(), input, output, measure, k, convergenceDelta,
    // maxIterations);
    // } else {
    // double t1 =
    // Double.parseDouble(getOption(DefaultOptionCreator.T1_OPTION));
    // double t2 =
    // Double.parseDouble(getOption(DefaultOptionCreator.T2_OPTION));
    // run(getConf(), input, output, measure, t1, t2, convergenceDelta,
    // maxIterations);
    // }
    // return 0;
    // }

    /**
     * Run the kmeans clustering job on an input dataset using the given the
     * number of clusters k and iteration parameters. All output data will be
     * written to the output directory, which will be initially deleted if it
     * exists. The clustered points will reside in the path
     * <output>/clustered-points. By default, the job expects a file containing
     * equal length space delimited data that resides in a directory named
     * "testdata", and writes output to a directory named "output".
     * 
     * @param conf
     *            the Configuration to use
     * @param input
     *            the String denoting the input directory path
     * @param output
     *            the String denoting the output directory path
     * @param measure
     *            the DistanceMeasure to use
     * @param k
     *            the number of clusters in Kmeans
     * @param convergenceDelta
     *            the double convergence criteria for iterations
     * @param maxIterations
     *            the int maximum number of iterations
     */
//    public static void run(JobConf conf, Path input, Path output, DistanceMeasure measure, int k, double convergenceDelta, int maxIterations) throws Exception {
//        Path directoryContainingConvertedInput = new Path(output, DIRECTORY_CONTAINING_CONVERTED_INPUT);
//        System.out.println("Preparing Input");
//        new HdfsDAO(conf).rmr(directoryContainingConvertedInput.toString());
//        inputDriver(conf,input, directoryContainingConvertedInput, "org.apache.mahout.math.RandomAccessSparseVector");
//        System.out.println("Running random seed to get initial clusters");
//        Path clusters = new Path(output, "random-seeds");
//        clusters = RandomSeedGenerator.buildRandom(conf, directoryContainingConvertedInput, clusters, k, measure);
//        clusters = new Path("/user/hdfs/mix_data/result/random-seeds/part-randomSeed");
//        System.out.printf("Running KMeans with k = %d\n", k);
//        KMeansDriver.run(conf, directoryContainingConvertedInput, clusters, output, measure, convergenceDelta, maxIterations, true, 0.0, false);
//        // // run ClusterDumper
//        // Path outGlob = new Path(output, "clusters-*-final");
//        // Path clusteredPoints = new Path(output, "clusteredPoints");
//        // System.out.printf("Dumping out clusters from clusters: %s and clusteredPoints: %s\n",
//        // outGlob, clusteredPoints);
//        // ClusterDumper clusterDumper = new ClusterDumper(outGlob,
//        // clusteredPoints);
//        // clusterDumper.printClusters(null);
//    }
    
    
//    public static void inputDriver(JobConf conf, Path input, Path output, String vectorClassName) throws IOException, InterruptedException, ClassNotFoundException {
//            conf.set("vector.implementation.class.name", vectorClassName);
//            conf.setOutputKeyClass(Text.class);
//            conf.setOutputValueClass(VectorWritable.class);
//            conf.setOutputFormat(SequenceFileOutputFormat.class);
//            conf.setMapperClass(InputMapper.class);   
//            conf.setNumReduceTasks(0);
//            conf.setJarByClass(InputDriver.class);
//            
//            FileInputFormat.addInputPath(conf, input);
//            FileOutputFormat.setOutputPath(conf, output);
//            
//            JobClient.runJob(conf);
//            System.exit(0);
//            
////            boolean succeeded = conf.waitForCompletion(true);
////            if (!succeeded) {
////              throw new IllegalStateException("Job failed!");
////            }
//          }

    /**
     * Run the kmeans clustering job on an input dataset using the given
     * distance measure, t1, t2 and iteration parameters. All output data will
     * be written to the output directory, which will be initially deleted if it
     * exists. The clustered points will reside in the path
     * <output>/clustered-points. By default, the job expects the a file
     * containing synthetic_control.data as obtained from
     * http://archive.ics.uci.
     * edu/ml/datasets/Synthetic+Control+Chart+Time+Series resides in a
     * directory named "testdata", and writes output to a directory named
     * "output".
     * 
     * @param conf
     *            the Configuration to use
     * @param input
     *            the String denoting the input directory path
     * @param output
     *            the String denoting the output directory path
     * @param measure
     *            the DistanceMeasure to use
     * @param t1
     *            the canopy T1 threshold
     * @param t2
     *            the canopy T2 threshold
     * @param convergenceDelta
     *            the double convergence criteria for iterations
     * @param maxIterations
     *            the int maximum number of iterations
     */
//    public static void run(Configuration conf, Path input, Path output, DistanceMeasure measure, double t1, double t2, double convergenceDelta, int maxIterations) throws Exception {
//        Path directoryContainingConvertedInput = new Path(output, DIRECTORY_CONTAINING_CONVERTED_INPUT);
//        System.out.println("Preparing Input");
//        InputDriver.runJob(input, directoryContainingConvertedInput, "org.apache.mahout.math.RandomAccessSparseVector");
//        System.out.println("Running Canopy to get initial clusters");
//        Path canopyOutput = new Path(output, "canopies");
//        CanopyDriver.run(new Configuration(), directoryContainingConvertedInput, canopyOutput, measure, t1, t2, false, 0.0, false);
//        System.out.println("Running KMeans");
//        KMeansDriver.run(conf, directoryContainingConvertedInput, new Path(canopyOutput, Cluster.INITIAL_CLUSTERS_DIR + "-final"), output, measure, convergenceDelta, maxIterations, true, 0.0, false);
//        // run ClusterDumper
//        ClusterDumper clusterDumper = new ClusterDumper(new Path(output, "clusters-*-final"), new Path(output, "clusteredPoints"));
//        clusterDumper.printClusters(null);
//    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        return 0;
    }

    // public static void main(String[] args) throws IOException,
    // InterruptedException, ClassNotFoundException {
    // JobConf conf = new JobConf(KmeansHadoop.class);
    // conf.setJobName("KmeansHadoop");
    // conf.addResource("classpath:/hadoop/core-site.xml");
    // conf.addResource("classpath:/hadoop/hdfs-site.xml");
    // conf.addResource("classpath:/hadoop/mapred-site.xml");
    //
    // String inPath = "/user/hdfs/mix_data/";
    // String outPath = "/user/hdfs/mix_data/result/";
    //
    // int k=3;
    // Path input = new Path(inPath);
    // Path output = new Path(outPath);
    //
    // DistanceMeasure measure = new EuclideanDistanceMeasure();
    // RandomSeedGenerator.buildRandom(conf, input, output, k, measure);
    //
    // InputDriver.runJob(input, output,
    // "org.apache.mahout.math.RandomAccessSparseVector");
    //
    //
    // // KMeansDriver.run(input, clustersIn, output, measure, convergenceDelta,
    // maxIterations, runClustering, threshold, runSequential);//.run(conf,
    // input, clustersIn, output, measure, convergenceDelta, maxIterations,
    // runClustering, clusterClassificationThreshold, runSequential)
    //
    //
    // // CanopyClusteringJob.runJob(output + "/data", output, measureClass, t1,
    // t2);
    // //KMeansDriver.run.runJob(output + "/data", output + "/canopies", output,
    // measureClass, convergenceDelta, maxIterations,1);
    // //KMeansDriver.runJob(output + "/data", output + "/canopies", output,
    // measureClass, convergenceDelta, maxIterations,1);
    // // OutputDriver.runJob(output + "/points", output + "/clustered-points");
    //
    //
    // // Path clustersIn = new Path(output, "random-seeds");
    // // DistanceMeasure measure = new EuclideanDistanceMeasure();
    // // RandomSeedGenerator.buildRandom(conf, input, clustersIn, k, measure);
    // //
    // // double convergenceDelta = 0.01;
    // // int maxIterations = 10;
    // // boolean runClustering = true;
    // // double threshold = 0.01;
    // // boolean runSequential = true;
    // //
    // //
    // // KMeansDriver.run(input, clustersIn, output, measure, convergenceDelta,
    // maxIterations, runClustering, threshold, runSequential);//.run(conf,
    // input, clustersIn, output, measure, convergenceDelta, maxIterations,
    // runClustering, clusterClassificationThreshold, runSequential)
    // //
    // //// List<List<Cluster>> Clusters = ClusterHelper.readClusters(conf,
    // output);
    // //// for (Cluster cluster : Clusters.get(Clusters.size() - 1)) {
    // //// System.out.println("Cluster id: " + cluster.getId() + " center: " +
    // cluster.getCenter().asFormatString());
    // //// }
    // //
    // //// JobClient.runJob(conf);
    // // System.exit(0);
    // }

}
