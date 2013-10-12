package org.conan.mymahout.common;


import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import com.google.common.collect.Lists;

public class InputMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, VectorWritable> {
    
  private static final Pattern SPACE = Pattern.compile(",");

  private Constructor<?> constructor;

    @Override
    public void map(LongWritable key, Text values, OutputCollector<Text, VectorWritable> output, Reporter reporter) throws IOException {
        String[] numbers = SPACE.split(values.toString());
      // sometimes there are multiple separator spaces
      Collection<Double> doubles = Lists.newArrayList();
      for (String value : numbers) {
        if (!value.isEmpty()) {
          doubles.add(Double.valueOf(value));
        }
      }
      // ignore empty lines in data file
      if (!doubles.isEmpty()) {
        try {
          Vector result = (Vector) constructor.newInstance(doubles.size());
          int index = 0;
          for (Double d : doubles) {
            result.set(index++, d);
          }
          VectorWritable vectorWritable = new VectorWritable(result);
          output.collect(new Text(String.valueOf(index)), vectorWritable);
        } catch (InstantiationException e) {
          throw new IllegalStateException(e);
        } catch (IllegalAccessException e) {
          throw new IllegalStateException(e);
        } catch (InvocationTargetException e) {
          throw new IllegalStateException(e);
        }
      }
    }
    
//  private static final Pattern SPACE = Pattern.compile(" ");
//
//  private Constructor<?> constructor;
//
//  @Override
//  protected void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
//
//    String[] numbers = SPACE.split(values.toString());
//    // sometimes there are multiple separator spaces
//    Collection<Double> doubles = Lists.newArrayList();
//    for (String value : numbers) {
//      if (!value.isEmpty()) {
//        doubles.add(Double.valueOf(value));
//      }
//    }
//    // ignore empty lines in data file
//    if (!doubles.isEmpty()) {
//      try {
//        Vector result = (Vector) constructor.newInstance(doubles.size());
//        int index = 0;
//        for (Double d : doubles) {
//          result.set(index++, d);
//        }
//        VectorWritable vectorWritable = new VectorWritable(result);
//        context.write(new Text(String.valueOf(index)), vectorWritable);
//
//      } catch (InstantiationException e) {
//        throw new IllegalStateException(e);
//      } catch (IllegalAccessException e) {
//        throw new IllegalStateException(e);
//      } catch (InvocationTargetException e) {
//        throw new IllegalStateException(e);
//      }
//    }
//  }
//
//  @Override
//  protected void setup(Context context) throws IOException, InterruptedException {
//    super.setup(context);
//    Configuration conf = context.getConfiguration();
//    String vectorImplClassName = conf.get("vector.implementation.class.name");
//    try {
//      Class<? extends Vector> outputClass = conf.getClassByName(vectorImplClassName).asSubclass(Vector.class);
//      constructor = outputClass.getConstructor(int.class);
//    } catch (NoSuchMethodException e) {
//      throw new IllegalStateException(e);
//    } catch (ClassNotFoundException e) {
//      throw new IllegalStateException(e);
//    }
//  }

}
