package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class MatchCity {
    public static class MatchCityMapper extends Mapper<Object,Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
            String valueString = value.toString();
            String[] SingleMatchData = valueString.split(",");
            context.write(new Text(SingleMatchData[6]),one);
        }


    }

    public static class IntMatchCityReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: MatchCities <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "match citites");
        job.setJarByClass(MatchCity.class);
        job.setMapperClass(MatchCity.MatchCityMapper.class);
        job.setCombinerClass(MatchCity.IntMatchCityReducer.class);
        job.setReducerClass(MatchCity.IntMatchCityReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
