package org.example.question05;

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

public class BrazilAverageTransaction {

    // Average transaction value per year in Brazil only.

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        BasicConfigurator.configure();

        Configuration config = new Configuration();

        Path input = new Path("input/operacoes_comerciais_inteira.csv");
        Path output = new Path("output/result.txt");

        Job job = new Job(config, "AverageBrasilTransaction");

        job.setJarByClass(BrazilAverageTransaction.class);
        job.setMapperClass(AverageTransactionMapper.class);
        job.setReducerClass(AverageTransactionReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AverageWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true)?0:1);

    }

    public static class AverageTransactionMapper extends Mapper<LongWritable, Text, Text, AverageWritable> {

        public void map(LongWritable key, Text value, Context cont) throws IOException, InterruptedException {

            if (key.get() == 0) {

                return;

            }

            String target = "Brazil";

            String row = value.toString();

            String[] fields = row.split(";");

            if (fields.length > 0 && target.equals(fields[0]) && !fields[5].isEmpty()) {

                float price = Float.parseFloat(fields[5]);

                cont.write(new Text(target), new AverageWritable(price, 1));

            }

        }

    }

    public static class AverageTransactionReducer extends Reducer<Text, AverageWritable, Text, FloatWritable> {

        public void reduce(Text key, Iterable<AverageWritable> values, Context con) throws IOException, InterruptedException {

            float sumPrice = 0;
            int sumN = 0;

            for (AverageWritable obj : values) {

                sumPrice += obj.getPrice();
                sumN += obj.getN();

            }

            float average = sumPrice / sumN;

            average = Float.parseFloat(String.format("%.2f", average));

            con.write(new Text(key), new FloatWritable(average));

        }

    }

}
