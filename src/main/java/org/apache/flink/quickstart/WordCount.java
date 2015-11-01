package org.apache.flink.quickstart;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import javax.xml.stream.XMLStreamConstants;//XMLInputFactory;
import java.io.*;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import javax.xml.stream.*;

public class WordCount
{

	public static class XmlInputFormat1 extends TextInputFormat {

		public static final String START_TAG_KEY = "xmlinput.start";
		public static final String END_TAG_KEY = "xmlinput.end";


		public RecordReader<LongWritable, Text> createRecordReader(
				InputSplit split, TaskAttemptContext context) {
			return new XmlRecordReader();
		}

		/**
		 * XMLRecordReader class to read through a given xml document to output
		 * xml blocks as records as specified by the start tag and end tag
		 *
		 */

		public static class XmlRecordReader extends
				RecordReader<LongWritable, Text> {
			private byte[] startTag;
			private byte[] endTag;
			private long start;
			private long end;
			private FSDataInputStream fsin;
			private DataOutputBuffer buffer = new DataOutputBuffer();

			private LongWritable key = new LongWritable();
			private Text value = new Text();
			@Override
			public void initialize(InputSplit split, TaskAttemptContext context)
					throws IOException, InterruptedException {
				Configuration conf = context.getConfiguration();
				startTag = conf.get(START_TAG_KEY).getBytes("utf-8");
				endTag = conf.get(END_TAG_KEY).getBytes("utf-8");
				FileSplit fileSplit = (FileSplit) split;

				// open the file and seek to the start of the split
				start = fileSplit.getStart();
				end = start + fileSplit.getLength();
				Path file = fileSplit.getPath();
				FileSystem fs = file.getFileSystem(conf);
				fsin = fs.open(fileSplit.getPath());
				fsin.seek(start);

			}
			@Override
			public boolean nextKeyValue() throws IOException,
					InterruptedException {
				if (fsin.getPos() < end) {
					if (readUntilMatch(startTag, false)) {
						try {
							buffer.write(startTag);
							if (readUntilMatch(endTag, true)) {
								key.set(fsin.getPos());
								value.set(buffer.getData(), 0,
										buffer.getLength());
								return true;
							}
						} finally {
							buffer.reset();
						}
					}
				}
				return false;
			}
			@Override
			public LongWritable getCurrentKey() throws IOException,
					InterruptedException {
				return key;
			}

			@Override
			public Text getCurrentValue() throws IOException,
					InterruptedException {
				return value;
			}
			@Override
			public void close() throws IOException {
				fsin.close();
			}
			@Override
			public float getProgress() throws IOException {
				return (fsin.getPos() - start) / (float) (end - start);
			}

			private boolean readUntilMatch(byte[] match, boolean withinBlock)
					throws IOException {
				int i = 0;
				while (true) {
					int b = fsin.read();
					// end of file:
					if (b == -1)
						return false;
					// save to buffer:
					if (withinBlock)
						buffer.write(b);
					// check if we're matching:
					if (b == match[i]) {
						i++;
						if (i >= match.length)
							return true;
					} else
						i = 0;
					// see if we've passed the stop point:
					if (!withinBlock && i == 0 && fsin.getPos() >= end)
						return false;
				}
			}
		}
	}


	public static class Map extends Mapper<LongWritable, Text,
			Text, Text> {
		@Override
		protected void map(LongWritable key, Text value,
						   Mapper.Context context)
				throws
				IOException, InterruptedException {
			String document = value.toString();
			System.out.println("‘" + document + "‘");
			try {
				XMLStreamReader reader =
						XMLInputFactory.newInstance().createXMLStreamReader(new
								ByteArrayInputStream(document.getBytes()));
				String username = "";
				String propertyValue = "";
				String currentElement = "";
				while (reader.hasNext()) {
					int code = reader.next();
					switch (code) {
						case XMLStreamConstants.START_ELEMENT: //START_ELEMENT:
							currentElement = reader.getLocalName();
							break;
						case XMLStreamConstants.CHARACTERS:  //CHARACTERS:
							if (currentElement.equalsIgnoreCase("username")) {
								username += reader.getText();
								//System.out.println(username);
							}
							break;
					}
				}
				reader.close();
				context.write(new Text(username.trim()),null);

			}
			catch(Exception e){
				throw new IOException(e);

			}

		}
	}
	public static class Reduce
			extends Reducer<Text, Text, Text, Text> {



		private Text outputKey = new Text();
		public void reduce(Text key, Iterable<Text> values,
						   Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				outputKey.set(constructPropertyXml(key, value));
				context.write(outputKey, null);
			}
		}

		public static String constructPropertyXml(Text name , Text value) {
			StringBuilder sb = new StringBuilder();
			sb.append(name)
					.append(",").append(value);
			return sb.toString();
		}
	}

	public static void helper(String inputPath, String outputPath) throws Exception {
		Configuration conf = new Configuration();

		conf.set("xmlinput.start", "<mediawiki>");
		conf.set("xmlinput.end", "</mediawiki>");
		Job job = new Job(conf);
		job.setJarByClass(WordCount.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(WordCount.Map.class);
		job.setReducerClass(WordCount.Reduce.class);

		job.setInputFormatClass(XmlInputFormat1.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);
	}



	public static void main(String[] args) throws Exception
	{
		WordCount x = new WordCount();
		x.helper("src/main/dump.xml","output");
	}
}