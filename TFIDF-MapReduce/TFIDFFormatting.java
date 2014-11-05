import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.SortedSet;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TFIDFFormatting extends Configured implements Tool {    
    public static class TFIDFMapper extends Mapper<LongWritable, Text, Text, Text> {

        public TFIDFMapper() {
        }

        private Text wordAndDoc = new Text();
        private Text wordAndCounters = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] wordAndCounters = value.toString().split("\t");
            this.wordAndDoc.set(new Text(wordAndCounters[0]));
            this.wordAndCounters.set(wordAndCounters[1] + "=" + wordAndCounters[2]);
            context.write(this.wordAndDoc, this.wordAndCounters);
        }
    }

    public static class TFIDFReducer extends Reducer<Text, Text, Text, Text> {
        private static final DecimalFormat DF = new DecimalFormat("###.########");
        private Text wordAtDocument = new Text();
        private Text tfidfCounts = new Text();
        public TFIDFReducer() {
        }

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException {
		Map<Integer, String> tempFrequencies = new TreeMap<Integer, String>();
            for (Text val : values) {
		String[] documentAndFrequencies = val.toString().split("=");
		tempFrequencies.put(Integer.valueOf(documentAndFrequencies[0]), documentAndFrequencies[1]);
		}
		String[] label = key.toString().split("@");
		String line ="";
	//SortedSet<Integer> keys = new TreeSet<Integer>(tempFrequencies.keySet());
	for (Integer document : tempFrequencies.keySet()) {
		double tfIdf = Double.valueOf(tempFrequencies.get(document));	
		line += String.valueOf(document)+":"+DF.format(tfIdf)+"\t";
			}
		this.wordAtDocument.set(label[0]);
		this.tfidfCounts.set(line);
                context.write(this.wordAtDocument, this.tfidfCounts);
        }
    }

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
	
	Path userInputPath = new Path(args[0]);
	
        Path userOutputPath = new Path(args[1]);
        if (fs.exists(userOutputPath)) {
            fs.delete(userOutputPath, true);
        }
        Configuration conf4 = getConf();
        Job job4 = new Job(conf4, "TF-IDF Formatting");
        job4.setJarByClass(TFIDFFormatting.class);
        job4.setMapperClass(TFIDFMapper.class);
        job4.setReducerClass(TFIDFReducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        job4.setInputFormatClass(TextInputFormat.class);
        job4.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job4, userInputPath);
        TextOutputFormat.setOutputPath(job4, userOutputPath);

        return job4.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TFIDFFormatting(), args);
        System.exit(res);
    }
}
