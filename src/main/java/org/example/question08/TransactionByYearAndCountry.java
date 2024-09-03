package org.example.question08;

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

public class TransactionByYearAndCountry {

    // Transaction with the highest and lowest price (based on the amount column), by year and country.

    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure();
        Configuration config = new Configuration();

        Path input = new Path("input/operacoes_comerciais_inteira.csv");
        Path output = new Path("output/question08");

        Job job = new Job(config, "TransactionByYearAndCountry");

        job.setJarByClass(TransactionByYearAndCountry.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(CompositeValue.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true)?0:1);

    }

    public static class Map extends Mapper<LongWritable, Text, CompositeKey, CompositeValue> {

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

            if (key.get() == 0) {

                return;

            }

            String row = value.toString();

            String[] fields = row.split(";");

            if (fields.length > 0 && !fields[0].isEmpty() && !fields[1].isEmpty() && !fields[5].isEmpty() && !fields[8].isEmpty()) {

                con.write(new CompositeKey(fields[0], fields[1]), new CompositeValue(Long.parseLong(fields[5]), Float.parseFloat(fields[8])));

            }

        }

    }

    public static class Reduce extends Reducer<CompositeKey, CompositeValue, Text, FloatWritable> {

        public void reduce(CompositeKey key, Iterable<CompositeValue> values, Context con) throws IOException, InterruptedException {

            float max = Float.NEGATIVE_INFINITY;
            float min = Float.POSITIVE_INFINITY;

            for (CompositeValue obj : values) {

                float total = obj.getTotal();

                if (total > max) {

                    max = total;

                }

                if (total < min) {

                    min = total;

                }

            }

            con.write(new Text(key.getCountry() + " " + key.getYear() + " More Expensive"), new FloatWritable(max));
            con.write(new Text(key.getCountry() + " " + key.getYear() + " Cheaper"), new FloatWritable(min));

        }

    }

}
