import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

// M/R code to count the # of triangles in a graph.
public class TriangleOutputer extends Configured implements Tool
{
    // Maps values to Long,Long pairs.
    public static class ParseLongLongPairsMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable>
    {
        LongWritable mKey = new LongWritable();
        LongWritable mValue = new LongWritable();

        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            long edge1,edge2;
            if (tokenizer.hasMoreTokens()) {
                edge1 = Long.parseLong(tokenizer.nextToken());
                if (!tokenizer.hasMoreTokens())
                    throw new RuntimeException("invalid edge line " + line);
                edge2 = Long.parseLong(tokenizer.nextToken());
                // Get rid of duplicate edges
                if (edge1 < edge2) {
                    mKey.set(edge1);
                    mValue.set(edge2);
			//Send to reducer in form (edge1, edge 2)
                    context.write(mKey,mValue);
                }
            }
        }
    }

    // Produces original edges and triads.
    public static class CrossProductReducer extends Reducer<LongWritable, LongWritable, Text, LongWritable>
    {
        Text rKey = new Text();
        final static LongWritable zero = new LongWritable((byte)-1);
        final static LongWritable one = new LongWritable((byte)1);
        long []edges = new long[4096];
        int size = 0;

        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException
        {
            // Produce triads - all permutations of pairs where e1 < e2 (value=1).
            // And all original edges (value=-1), in case there is an node with value 0.
            // Sorted by value.
            Iterator<LongWritable> vs = values.iterator();
		//Increase the size of array to fit all edges in.
            for (size = 0; vs.hasNext(); ) {
                if (edges.length==size) {
                    edges = Arrays.copyOf(edges, edges.length*2);
                }

                long edge = vs.next().get();
                edges[size++] = edge;

                // Original edge.
                rKey.set(key.toString() + "," + Long.toString(edge));
		//Put value of -1 for original an original edge
                context.write(rKey, zero);
            }

            Arrays.sort(edges, 0, size);

            // Generate the cross product of Z with Z.
            for (int i=0; i<size; ++i) {
                for (int j=i+1; j<size; ++j) {
                    rKey.set(Long.toString(edges[i]) + "," + Long.toString(edges[j]));
                    context.write(rKey, key);
                }
            }
        }
    }

    // Parses values into {Text,Long} pairs.
    public static class ParseTextLongPairsMapper extends Mapper<LongWritable, Text, Text, LongWritable>
    {
        Text mKey = new Text();
        LongWritable mValue = new LongWritable();

        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            if (tokenizer.hasMoreTokens()) {
                mKey.set(tokenizer.nextToken());
                if (!tokenizer.hasMoreTokens())
                    throw new RuntimeException("invalid intermediate line " + line);
                mValue.set(Long.parseLong(tokenizer.nextToken()));
                context.write(mKey, mValue);
            }
        }
    }

    // Output the 3 nodes of the triangle.
    public static class OutputTriangleReducer extends Reducer<Text, LongWritable, Text, NullWritable>
    {
        long count = 0;
	Text rKey = new Text();
        final static LongWritable zero = new LongWritable(0);
	long []vArray = new long[4096];
        int size = 0;
        

        public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException
        {
            boolean isClosed = false;
            long c = 0, n = 0;
            Iterator<LongWritable> vs = values.iterator();
            // Triad edge value=1, original edge value=0.
            while (vs.hasNext()) {
                
                for (size = 0; vs.hasNext(); ) {
                	if (vArray.length==size) {
                    vArray = Arrays.copyOf(vArray, vArray.length*2);
               		 }
			long e = vs.next().get();
			if(e==-1) isClosed=true;
			else{                
				
                		vArray[size++] = e;
				}
		}
            }
            if (isClosed) {
		for (int i=0; i<size; i++){
			rKey.set(key.toString() + "," + Long.toString(vArray[i]));
			context.write(rKey, null);
			}
			
			
        	}
    	}
    }
    
   
    public int run(String[] args) throws Exception
    {
        Job job1 = new Job(getConf());
        job1.setJobName("crossproduct");

        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(LongWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);

        job1.setJarByClass(TriangleOutputer.class);
        job1.setMapperClass(ParseLongLongPairsMapper.class);
        job1.setReducerClass(CrossProductReducer.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("temp1"));


        Job job2 = new Job(getConf());
        job2.setJobName("triangles");

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(LongWritable.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);

        job2.setJarByClass(TriangleOutputer.class);
        job2.setMapperClass(ParseTextLongPairsMapper.class);
        job2.setReducerClass(OutputTriangleReducer.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job2, new Path("temp1"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));


        int ret = job1.waitForCompletion(true) ? 0 : 1;
        if (ret==0) ret = job2.waitForCompletion(true) ? 0 : 1;
        return ret;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TriangleOutputer(), args);
        System.exit(res);
    }
}
