import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import CFInputFormat;
//import FileLineWritable;

public class WordsInCorpusTFIDF extends Configured implements Tool {
    //private static final String TRAIN_PATH = "train_input"; 
    private static final String OUTPUT_PATH_0 = "0-word-freq";
     private static final String OUTPUT_PATH_1 = "1-word-freq";    
    private static final String OUTPUT_PATH_2 = "2-word-freq";
	private static final String OUTPUT_PATH_F = "2-word-counts";

    public static class WordsInCorpusTFIDFMapper extends Mapper<LongWritable, Text, Text, Text> {

        public WordsInCorpusTFIDFMapper() {
        }
	
        private Text wordAndDoc = new Text();
        private Text wordAndCounters = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String filePath = ((FileSplit)context.getInputSplit()).getPath().getName();    	
		String[] wordAndCounters = value.toString().split("\t");
            //String[] wordAndDoc = wordAndCounters[0].split("@");  //3/1500
            this.wordAndDoc.set(new Text(wordAndCounters[1]));
	//if(filePath.contains("test_input"))
          //  this.wordAndCounters.set("$"+wordAndCounters[0] + "=" + wordAndCounters[2]);
	//else
		this.wordAndCounters.set(wordAndCounters[0] + "=" + wordAndCounters[2]);
            context.write(this.wordAndDoc, this.wordAndCounters);
        }
    }

    public static class WordsInCorpusTFIDFReducer extends Reducer<Text, Text, Text, Text> {

        private static final DecimalFormat DF = new DecimalFormat("###.########");

        private Text wordAtDocument = new Text();

        private Text tfidfCounts = new Text();

        public WordsInCorpusTFIDFReducer() {
        }
	//boolean isTrainWord = false;
	int count =0;
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                InterruptedException {
	   
            int numberOfDocumentsInCorpus = context.getConfiguration().getInt("numberOfDocsInCorpus", 0);
            int numberOfDocumentsInCorpusWhereKeyAppears = 0;
            Map<String, String> tempFrequencies = new HashMap<String, String>();
            for (Text val : values) {
                String[] documentAndFrequencies = val.toString().split("=");
                if (Integer.parseInt(documentAndFrequencies[1].split("/")[0]) > 0) {
                    numberOfDocumentsInCorpusWhereKeyAppears++;
                }
		//if(documentAndFrequencies[0].startsWith("$"))
		tempFrequencies.put(documentAndFrequencies[0], documentAndFrequencies[1]);
            }
	    //if(isTrainWord){	
            count=count+1;
		for (String document : tempFrequencies.keySet()) {
                //if(document.startsWith("$"))
		//continue;
		String[] wordFrequenceAndTotalWords = tempFrequencies.get(document).split("/");

                double tf = Double.valueOf(Double.valueOf(wordFrequenceAndTotalWords[0])
                        / Double.valueOf(wordFrequenceAndTotalWords[1]));

                double idf = Math.log10((double) numberOfDocumentsInCorpus / 
                   (double) ((numberOfDocumentsInCorpusWhereKeyAppears == 0 ? 1 : 0) + 
                         numberOfDocumentsInCorpusWhereKeyAppears));

                double tfIdf = tf * idf;
		if(tfIdf == 0.0)
		continue;
                this.wordAtDocument.set(document + "\t" + String.valueOf(count));
		this.tfidfCounts.set(DF.format(tfIdf));
                context.write(this.wordAtDocument, this.tfidfCounts);
            }
	 }
	//isTrainWord=false;
        //}
    }

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
	conf.set("label","0");
        FileSystem fs = FileSystem.get(conf);
	
        Path zeroPath = new Path(args[0]+"/0");
	Path onePath = new Path(args[0]+"/1");
         Path twoPath = new Path(args[0]+"/2");
	Path userOutputPath = new Path(args[1]);
        if (fs.exists(userOutputPath)) {
            fs.delete(userOutputPath, true);
        }
        Path wordFreqPath = new Path(OUTPUT_PATH_0);
        if (fs.exists(wordFreqPath)) {
            fs.delete(wordFreqPath, true);
        }
	Path wordFreqPath_one = new Path(OUTPUT_PATH_1);
        if (fs.exists(wordFreqPath_one)) {
            fs.delete(wordFreqPath_one, true);
        }
Path wordFreqPath_two = new Path(OUTPUT_PATH_2);
        if (fs.exists(wordFreqPath_two)) {
            fs.delete(wordFreqPath_two, true);
        }
        Path wordCountsPath = new Path(OUTPUT_PATH_F);
        if (fs.exists(wordCountsPath)) {
            fs.delete(wordCountsPath, true);
        }
	//String paths = args[0]+"/pos"+","+args[0]+"/neg";
	Job job = new Job(conf, "Word Frequence In Document");
        job.setJarByClass(WordFrequenceInDocument.class);
        job.setMapperClass(WordFrequenceInDocument.WordFrequenceInDocMapper.class);
        job.setReducerClass(WordFrequenceInDocument.WordFrequenceInDocReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(CFInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job,zeroPath);
        TextOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH_0));

        job.waitForCompletion(true);
	
	 Configuration conf1 = getConf();
        conf1.set("label","1");
        FileSystem fs1 = FileSystem.get(conf1);	
	Job job1 = new Job(conf1, "Word Frequence In Document");
        job1.setJarByClass(WordFrequenceInDocument.class);
        job1.setMapperClass(WordFrequenceInDocument.WordFrequenceInDocMapper.class);
        job1.setReducerClass(WordFrequenceInDocument.WordFrequenceInDocReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setInputFormatClass(CFInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job1,onePath);
        TextOutputFormat.setOutputPath(job1, new Path(OUTPUT_PATH_1));

        job1.waitForCompletion(true);
	
	Configuration conf12 = getConf();
        conf12.set("label","2");
        FileSystem fs12 = FileSystem.get(conf12); 
        Job job12 = new Job(conf12, "Word Frequence In Document");
        job12.setJarByClass(WordFrequenceInDocument.class);
        job12.setMapperClass(WordFrequenceInDocument.WordFrequenceInDocMapper.class);
        job12.setReducerClass(WordFrequenceInDocument.WordFrequenceInDocReducer.class);
        job12.setOutputKeyClass(Text.class);
        job12.setOutputValueClass(IntWritable.class);
        job12.setInputFormatClass(CFInputFormat.class);
        job12.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job12,twoPath);
        TextOutputFormat.setOutputPath(job12, new Path(OUTPUT_PATH_2));

        job12.waitForCompletion(true);

	FileStatus[] userFilesStatusList = fs.listStatus(zeroPath);
        final int numberOfUserInputFiles = userFilesStatusList.length;
        FileStatus[] neg= fs1.listStatus(onePath);
        final int numberOfNeg = neg.length;
	FileStatus[] two= fs12.listStatus(twoPath);
        final int numberOfTwo = two.length;
        String[] fileNames = new String[numberOfUserInputFiles+numberOfNeg+numberOfTwo];
        for (int i = 0; i < numberOfUserInputFiles+numberOfNeg+numberOfTwo; i++) {
            if(i<numberOfUserInputFiles)
                fileNames[i] = "0@"+userFilesStatusList[i].getPath().getName();
            else if(i<numberOfUserInputFiles+numberOfNeg)
                fileNames[i] = "1@"+neg[i-numberOfUserInputFiles].getPath().getName();
        	else
		 fileNames[i] = "2@"+neg[i-numberOfUserInputFiles-numberOfNeg].getPath().getName();
	}
        Configuration conf2 = getConf();
        conf2.setStrings("documentsInCorpusList", fileNames);
        Job job2 = new Job(conf2, "Words Counts");
        job2.setJarByClass(WordCountsInDocuments.class);
        job2.setMapperClass(WordCountsInDocuments.WordCountsForDocsMapper.class);
        job2.setReducerClass(WordCountsInDocuments.WordCountsForDocsReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPaths(job2, OUTPUT_PATH_0+","+OUTPUT_PATH_1+","+OUTPUT_PATH_2);
        TextOutputFormat.setOutputPath(job2, new Path(OUTPUT_PATH_F));

        job2.waitForCompletion(true);

        Configuration conf3 = getConf();
        conf3.setInt("numberOfDocsInCorpus", 3893);
        Job job3 = new Job(conf3, "TF-IDF of Words in Corpus");
        job3.setJarByClass(WordsInCorpusTFIDF.class);
        job3.setMapperClass(WordsInCorpusTFIDFMapper.class);
        job3.setReducerClass(WordsInCorpusTFIDFReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPaths(job3, OUTPUT_PATH_F);
        TextOutputFormat.setOutputPath(job3, new Path(args[1]));

        return job3.waitForCompletion(true) ? 0 : 1;
}

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordsInCorpusTFIDF(), args);
        System.exit(res);
    }
}
