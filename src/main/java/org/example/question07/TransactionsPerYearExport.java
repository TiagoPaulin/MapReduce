package org.example.question07;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class TransactionsPerYearExport {

    //Average transaction value per year, considering only export transactions conducted in Brazil.

    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure();
        org.apache.hadoop.conf.Configuration config = new Configuration();

        Path input = new Path("input/operacoes_comerciais_inteira.csv");
        Path output = new Path("output/result.txt");

        Job job = new Job(config, "AverageTransactionPerYear");

        job.setJarByClass(TransactionsPerYearExport.class);
        job.setMapperClass(TransactionMap.class);
        job.setReducerClass(TransactionReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AverageWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true)?0:1);

    }

    public static class TransactionMap extends Mapper<LongWritable, Text, Text, AverageWritable> {

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

            if (key.get() == 0) {

                return;

            }

            String target1 = "Brazil";
            String target2 = "Export";

            String row = value.toString();

            String[] fields = row.split(";");

            if (fields.length > 0 && target1.equals(fields[0]) && target2.equals(fields[4])
                    && !fields[1].isEmpty() && !fields[5].isEmpty()) {

                con.write(new Text(fields[1]), new AverageWritable(Float.parseFloat(fields[5]), 1));

            }

        }

    }

    public static class TransactionReduce extends Reducer<Text, AverageWritable, Text, FloatWritable> {

        public void reduce(Text key, Iterable<AverageWritable> values, Context con) throws IOException, InterruptedException {

            float sumPrice = 0;
            int sumN = 0;

            for (AverageWritable obj : values) {

                sumPrice += obj.getPrice();
                sumN = obj.getN();

            }

            float average = sumPrice / sumN;

            average = Float.parseFloat(String.format("%.2f", average));

            con.write(new Text(key), new FloatWritable(average));

        }

    }

}
