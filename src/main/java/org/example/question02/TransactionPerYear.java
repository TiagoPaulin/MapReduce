package org.example.question02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class TransactionPerYear {

    // Number of transactions per year

    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure();
        Configuration config = new Configuration();

        Path input = new Path("input/operacoes_comerciais_inteira.csv");
        Path output = new Path("output/question02");

        Job job = new Job(config, "transactioPerYearCount");

        job.setJarByClass(TransactionPerYear.class);
        job.setMapperClass(MapForWordCount.class);
        job.setReducerClass(ReduceForWordCount.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job,input);
        FileOutputFormat.setOutputPath(job,output);

        System.exit(job.waitForCompletion(true)?0:1);

    }

    public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            if (key.get() == 0) {

                return;

            }

            String row = value.toString();

            String[] fields = row.split(";");

            if (fields.length > 0 && !fields[1].isEmpty()) {

                con.write(new Text(fields[1]), new IntWritable(1));

            }

        }

    }

    public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable value : values){

                sum += value.get();

            }

            con.write(key, new IntWritable(sum));

        }

    }

}
