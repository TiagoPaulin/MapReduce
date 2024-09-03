package org.example.question06;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class MostAndLeastExpensive {

    // Most expensive and least expensive transaction in Brazil in 2016.

    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure();
        Configuration config = new Configuration();

        Path input = new Path("input/operacoes_comerciais_inteira.csv");
        Path output = new Path("output/question06");

        Job job = new Job(config, "MostAndLeastExpensive");

        job.setJarByClass(MostAndLeastExpensive.class);
        job.setMapperClass(TransactionMap.class);
        job.setReducerClass(TransactionReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true)?0:1);

    }

    public static class TransactionMap extends Mapper<LongWritable, Text, Text, LongWritable> {

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

            if (key.get() == 0) {

                return;

            }

            String target1 = "Brazil";
            String target2 = "2016";

            String row = value.toString();

            String[] fields = row.split(";");

            if (fields.length > 0 && target1.equals(fields[0]) && target2.equals(fields[1]) && !fields[5].isEmpty()) {

                con.write(new Text("Brasil_2016"), new LongWritable(Long.parseLong(fields[5])));

            }

        }

    }

    public static class TransactionReduce extends Reducer<Text, LongWritable, Text, LongWritable> {

        public void reduce(Text key, Iterable<LongWritable> values, Context con) throws IOException, InterruptedException {

            long max = Long.MIN_VALUE;
            long min = Long.MAX_VALUE;

            for (LongWritable obj : values) {

                if (obj.get() > max) {

                    max = obj.get();

                }

                if (obj.get() < min) {

                    min = obj.get();

                }

            }

            con.write(new Text("More Expensive"), new LongWritable(max));
            con.write(new Text("Cheaper"), new LongWritable(min));

        }

    }

}
