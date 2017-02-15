/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.mycompany.maprpractice.Mapper;

import com.mycompany.maprpractice.runnerClass.*;

/**
 *
 * @author QAI
 */
import java.io.IOException;
import java.util.StringTokenizer;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
 
	private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line," ");
		while(st.hasMoreTokens()){
			word.set(st.nextToken());
			context.write(word,one);
		}
	}
}
