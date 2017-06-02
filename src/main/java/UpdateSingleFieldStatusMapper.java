/**
 * Created by zhufangze on 2017/5/28.
 */
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class UpdateSingleFieldStatusMapper
        extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    private Long UPDATE_TS = System.currentTimeMillis() / 1000;
    private Long R_TIMESTAMP = 5000000000000L;
    private String SEPARATOR = "#_#!";
    private String UNKNOWN = "unknown";

    public static String toHexString(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();

        for (int i = 0; i < bytes.length; i++) {
            String hex = Integer.toHexString(0xFF & bytes[i]);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }

        return hexString.toString();
    }

    private String md5(String s) throws NoSuchAlgorithmException {
        byte b[] = MessageDigest.getInstance("MD5").digest(s.getBytes());
        return toHexString(b);
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (value == null) {
            context.getCounter("user_mapper", "TOTAL").increment(1L);
            return;
        }

        try {
            String SITEMAP_TYPE = context.getConfiguration().get("user.param.sitemap.type", UNKNOWN);
            String FIELD_NAME = context.getConfiguration().get("user.param.field.name", UNKNOWN);

            if (SITEMAP_TYPE.equals(UNKNOWN) || FIELD_NAME.equals(UNKNOWN)) {
                context.getCounter("user_mapper", "INVALID_PARAM").increment(1L);
                return;
            }

            // parts[0]:root_sitemap, parts[1]:url
            String line = value.toString();
            String[] parts = line.split("\t");
            if (parts.length != 2) {
                context.getCounter("user_mapper", "INVALID_LINE").increment(1L);
                return;
            }

            byte[] k = Bytes.toBytes(md5(parts[1].trim()+SEPARATOR+SITEMAP_TYPE.trim()) ); // key for hbase
            Put put = new Put(k, R_TIMESTAMP);
            put.addImmutable(Bytes.toBytes("i"), Bytes.toBytes(FIELD_NAME), Bytes.toBytes(UPDATE_TS.toString()));
            context.write(new ImmutableBytesWritable(k), put);
        }
        catch(Exception e) {
            context.getCounter("user_mapper", "EXCEPTION").increment(1L);
        }
    }
}


