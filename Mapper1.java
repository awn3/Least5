import java.io.IOException;
import java.util.Map;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.naming.Context;

public class Mapper1 {
    // Map function
    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        private Text word = new Text();
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // Splitting the line on spaces
            String[] stringArr = value.toString().split("\\s+");
            for (String str : stringArr) {
                word.set(str);
                context.write(word, new IntWritable(1));
            }
        }
    }

    public static class MyMapper2 extends Mapper<LongWritable, Text, Text, IntWritable>{
        private Text word = new Text();
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // Splitting the line on spaces
            String[] stringArr = value.toString().split("\\s+");
            for (String str : stringArr) {
                word.set(str);
                context.write(word, new IntWritable(1));
            }
        }
    }
    public static class MyMapper3 extends Mapper<LongWritable, Text, Text, IntWritable>{
        private Text word = new Text();
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // Splitting the line on spaces
            String[] stringArr = value.toString().split("\\s+");
            for (String str : stringArr) {
                word.set(str);
                context.write(word, new IntWritable(1));
            }
        }
    }

    // Reduce function
    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        private IntWritable result = new IntWritable();
        TreeMap<String, Integer> tmap2= new TreeMap<String, Integer>();
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            tmap2.put(key.toString(), result.get());
            if (tmap2.size() > 5)
            {
                Integer valToAdd = sum;
                Integer largestVal = 0;
                String keyToRemove = key.toString();
                for (Map.Entry<String, Integer> entry : tmap2.entrySet())
                {
                   if(entry.getValue() < largestVal) {
                       largestVal = entry.getValue();
                       keyToRemove = entry.getKey();
                   }
                }
                tmap2.remove(tmap2.remove(keyToRemove).toString());
            }
            //context.write(key, result);
        }
        @Override
        public void cleanup(Context context) throws IOException,
                InterruptedException
        {

            for (Map.Entry<String, Integer> entry : tmap2.entrySet())
            {

                IntWritable count = new IntWritable(entry.getValue());
                String name = entry.getKey();
                context.write(new Text(name), count);

            }
        }

    }

    public static void main(String[] args)  throws Exception{
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "WC");
        job.setJarByClass(Mapper1.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MyMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MyMapper2.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, MyMapper3.class);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}