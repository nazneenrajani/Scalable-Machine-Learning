import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;
import java.util.LinkedHashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import PorterStemmer;

public class WordFrequenceInDocument extends Configured implements Tool {

    private static final String OUTPUT_PATH_0 = "0-word-freq";
    private static final String OUTPUT_PATH_1 = "1-word-freq";
    private static final String OUTPUT_PATH_2 = "2-word-freq";
    public static class WordFrequenceInDocMapper extends Mapper<FileLineWritable, Text, Text, IntWritable> {
        private static final Pattern PATTERN = Pattern.compile("\\w+");
        private Text word = new Text();
        private IntWritable singleCount = new IntWritable(1);

        public WordFrequenceInDocMapper() {
        }

        public void map(FileLineWritable key, Text value, Context context) throws IOException, InterruptedException {
            Matcher m = PATTERN.matcher(value.toString());
            //String fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
            StringBuilder valueBuilder = new StringBuilder();
		Set<String> stopWords = new LinkedHashSet<String>();
		try {
			BufferedReader SW= new BufferedReader(new FileReader("stopwords"));
			for(String lin;(lin = SW.readLine()) != null;)
				stopWords.add(lin.trim());
			SW.close();
		}
		catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}    
	while (m.find()) {
                String matchedKey = m.group().toLowerCase();
                String pattern = "^[a-zA-Z]*$";
		if (!Character.isLetter(matchedKey.charAt(0)) ||stopWords.contains(matchedKey)|| Character.isDigit(matchedKey.charAt(0))|| matchedKey.contains("_") || matchedKey.length() < 3 || matchedKey.length() >50 ) {
                    continue;
                }
		matchedKey.replaceAll(pattern,"");
		matchedKey.replaceAll("\\p{C}","");
		if(matchedKey.length() < 3)
			continue;
                String l = context.getConfiguration().get("label");
		valueBuilder.append(l+"@"+key.fileName);
                valueBuilder.append("\t");
                valueBuilder.append(matchedKey);
                this.word.set(valueBuilder.toString());
                context.write(this.word, this.singleCount);
                valueBuilder.setLength(0);
            }
        }
    }

    public static class WordFrequenceInDocReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private Map<String, Integer> word_id = new HashMap<String,Integer>();
	private IntWritable wordSum = new IntWritable();
       	private Text countWord = new Text(); 
        public WordFrequenceInDocReducer() {
        }
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, 
                 InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
	    String[] file = key.toString().split("\t");
            this.wordSum.set(sum);
		context.write(key, this.wordSum);
        }
    }

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
	Job job = new Job(conf, "Word Frequence In Document");

        job.setJarByClass(WordFrequenceInDocument.class);
        job.setMapperClass(WordFrequenceInDocMapper.class);
        job.setReducerClass(WordFrequenceInDocReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
	//String path = args[0]+"/pos,"+args[0]+"/neg";
        FileInputFormat.addInputPath(job, new Path(args[0]));
        //FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordFrequenceInDocument(), args);
        System.exit(res);
    }
}
