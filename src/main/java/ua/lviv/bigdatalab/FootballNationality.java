package ua.lviv.bigdatalab;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FootballNationality {

    public static class NationalityMapper extends Mapper<Object, Text, Text, IntWritable>{

        private Text nationality = new Text();
        final String DELIMITER = ",";
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if(!value.toString().equals("name,club,age,position,position_cat,market_value,page_views,fpl_value,fpl_sel,fpl_points,region,nationality,new_foreign,age_cat,club_id,big_club,new_signing"))
            {
                String[] tokens = value.toString().split(DELIMITER);
                    nationality.set(tokens[11]);
                context.write(nationality, one);
            }
        }
    }


    public static class WordPartitioner extends Partitioner<Text, IntWritable> {

        public int getPartition(Text key, IntWritable value, int numPartitions) {

            if(numPartitions == 2){
                String partitionKey = key.toString();
                if(partitionKey.charAt(0) >= 'A' && partitionKey.charAt(0)<='K' )
                    return 0;
                else
                    return 1;
            } else if(numPartitions == 1)
                return 0;
            else{
                System.err.println("Partitioner can only handle either 1 or 2 partitions");
                return 0;
            }
        }

    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int res = 0;
            for (IntWritable value : values) {
                res+=value.get();
            }
            result.set(res);
            context.write(key, result);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "nationality sort");
        job.setNumReduceTasks(2);
        job.setJarByClass(FootballNationality.class);
        job.setMapperClass(NationalityMapper.class);
        job.setPartitionerClass(WordPartitioner.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
