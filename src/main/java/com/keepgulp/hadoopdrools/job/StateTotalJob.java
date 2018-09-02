package com.keepgulp.hadoopdrools.job;

import com.keepgulp.hadoopdrools.mr.HadoopDroolsMapper;
import com.keepgulp.hadoopdrools.mr.HadoopDroolsReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class StateTotalJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        System.out.println("StateTotalJob.run(): args = [" + args[0] +" " + args[1] + "]");

        if (args.length != 2) {
            System.out.println("usage: [input] [output]");
            System.exit(-1);
        }

        Job job = Job.getInstance(new Configuration());
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(HadoopDroolsMapper.class);
        job.setReducerClass(HadoopDroolsReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(StateTotalJob.class);

        job.submit();
        return 0;
    }

    public static void main(String[] args) throws Exception {

        System.setProperty("hadoop.home.dir", "/usr/local/hadoop");

        int res = ToolRunner.run(new Configuration(), new StateTotalJob(), args);
        System.out.println("res = [" + res + "]");
        System.exit(res);
    }
}
