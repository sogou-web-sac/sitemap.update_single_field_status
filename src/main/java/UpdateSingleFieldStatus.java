/**
 * Created by zhufangze on 2017/5/28.
 * @param: user.param.sitemap.type
 * @param: user.param.field.name
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class UpdateSingleFieldStatus extends Configured implements Tool {

    public int run(String[] arg0) throws Exception {

        Configuration conf = getConf();
        Job job = new Job(conf, conf.get("mapred.job.name"));
        String output_table = conf.get("hbase.table");

        job.setJarByClass(UpdateSingleFieldStatus.class);
        job.setMapperClass(UpdateSingleFieldStatusMapper.class);
        job.setReducerClass(PutSortReducer.class);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        HTable table = new HTable(conf, output_table);
        job.setOutputFormatClass(HFileOutputFormat2.class);
        HFileOutputFormat2.configureIncrementalLoad(job, table);

        if (job.waitForCompletion(true) && job.isSuccessful()) {
            return 0;
        }
        return -1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        int res = ToolRunner.run(conf, new UpdateSingleFieldStatus(), args);
        System.exit(res);
    }
}
