import java.io.IOException;
import java.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class SecondAnswer {

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>  {
    private final static IntWritable one = new IntWritable(1);
    private Text regionText = new Text();

    private String mappingToken = null;
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      String TextLine = value.toString();
      String[] textarray = TextLine.split("::");
      
	
  	if(Integer.parseInt(textarray[2]) <= 18)
	{
     mappingToken = "7 "+textarray[1];
     regionText.set(mappingToken);
     output.collect(regionText, one);
	 }
	 else if(Integer.parseInt(textarray[2]) > 18 && Integer.parseInt(textarray[2]) <= 24)
	 {
	 mappingToken = "24 "+textarray[1];
     regionText.set(mappingToken);
     output.collect(regionText, one);
	 }
	 else if(Integer.parseInt(textarray[2]) >= 25 && Integer.parseInt(textarray[2]) <= 34){
	  mappingToken = "31 "+textarray[1];
     regionText.set(mappingToken);
     output.collect(regionText, one);
	 }
	 else if(Integer.parseInt(textarray[2]) >= 35 && Integer.parseInt(textarray[2]) <= 44){
	  mappingToken = "41 "+textarray[1];
     regionText.set(mappingToken);
     output.collect(regionText, one);
	 }
	 else if(Integer.parseInt(textarray[2]) >= 45 && Integer.parseInt(textarray[2]) <= 55){
	  mappingToken = "51 "+textarray[1];
     regionText.set(mappingToken);
     output.collect(regionText, one);
	 }
	 else if(Integer.parseInt(textarray[2]) >= 56 && Integer.parseInt(textarray[2]) <= 61){
	  mappingToken = "56 "+textarray[1];
     regionText.set(mappingToken);
     output.collect(regionText, one);
	 }
	 else
	 {
	  mappingToken = "62 "+textarray[1];
     regionText.set(mappingToken);
     output.collect(regionText, one);
	 }
    }
  
 }

  public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      output.collect(key, new IntWritable(sum));
    }
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(SecondAnswer.class);
    
    conf.setJobName("SecondAnswer");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
  }
}

