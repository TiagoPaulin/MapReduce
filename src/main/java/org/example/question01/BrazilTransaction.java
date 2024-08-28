package org.example.question01;

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

public class BrazilTransaction {

    // Number of transactions involving Brazil

    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure();
        Configuration config = new Configuration();

        Path input = new Path("input/operacoes_comerciais_inteira.csv");
        Path output = new Path("output/result.txt");

        Job job = new Job(config, "brasilTransactionsCount");

        job.setJarByClass(BrazilTransaction.class);
        job.setMapperClass(MapForWordCount.class);
        job.setReducerClass(ReduceForWordCount.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(job,input);
        FileOutputFormat.setOutputPath(job,output);

        //4. exit
        System.exit(job.waitForCompletion(true)?0:1);

    }

    public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String target = "Brazil";

            String row = value.toString();

            if (key.get() == 0 && row.contains("country_or_area")) {

                return;

            }

            String[] fields = row.split(";");

            if (fields.length > 0 && !fields[0].isEmpty() && target.equals(fields[0])) {

                con.write(new Text(target), new IntWritable(1));

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
