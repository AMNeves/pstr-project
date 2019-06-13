/*
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

package ps2019;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the
 * <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>
 * To package your application into a JAR file for execution, run 'mvn clean
 * package' on the command line.
 *
 * <p>
 * If you change the name of the main class (with the public static void
 * main(String[] args)) method, change the respective entry in the POM.xml file
 * (simply search for 'mainClass').
 */
public class KafkaConsumer
{
	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");

		// only required for Kafka 0.8
		// properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("group.id", "local[2]");

		DataStream<String> text = env
				.addSource(new FlinkKafkaConsumer<>("debs", new SimpleStringSchema(), properties));

		//DataStream<Tuple2<String, Integer>> counts =
				// split up the lines in pairs (2-tuples) containing: (word,1)
			//	text.flatMap(new Tokenizer())
						// group by the tuple field "0" and sum up tuple field
						// "1"
						//.keyBy(0).sum(1);

		text.writeAsText( "log/result.txt");
		
		// execute program
		env.execute("Streaming WordCount");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	/**
	 * Implements the string tokenizer that splits sentences into words as a
	 * user-defined FlatMapFunction. The function takes a line (String) and
	 * splits it into multiple pairs in the form of "(word,1)"
	 * ({@code Tuple2<String,
	 * Integer>}).
	 */
	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>>
	{

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<>(token, 1));
				}
			}
		}
	}
	
	public static final class WordCountData {

		public static final String[] WORDS = new String[] {
			"To be, or not to be,--that is the question:--",
			"Whether 'tis nobler in the mind to suffer",
			"The slings and arrows of outrageous fortune",
			"Or to take arms against a sea of troubles,",
			"And by opposing end them?--To die,--to sleep,--",
			"No more; and by a sleep to say we end",
			"The heartache, and the thousand natural shocks",
			"That flesh is heir to,--'tis a consummation",
			"Devoutly to be wish'd. To die,--to sleep;--",
			"To sleep! perchance to dream:--ay, there's the rub;",
			"For in that sleep of death what dreams may come,",
			"When we have shuffled off this mortal coil,",
			"Must give us pause: there's the respect",
			"That makes calamity of so long life;",
			"For who would bear the whips and scorns of time,",
			"The oppressor's wrong, the proud man's contumely,",
			"The pangs of despis'd love, the law's delay,",
			"The insolence of office, and the spurns",
			"That patient merit of the unworthy takes,",
			"When he himself might his quietus make",
			"With a bare bodkin? who would these fardels bear,",
			"To grunt and sweat under a weary life,",
			"But that the dread of something after death,--",
			"The undiscover'd country, from whose bourn",
			"No traveller returns,--puzzles the will,",
			"And makes us rather bear those ills we have",
			"Than fly to others that we know not of?",
			"Thus conscience does make cowards of us all;",
			"And thus the native hue of resolution",
			"Is sicklied o'er with the pale cast of thought;",
			"And enterprises of great pith and moment,",
			"With this regard, their currents turn awry,",
			"And lose the name of action.--Soft you now!",
			"The fair Ophelia!--Nymph, in thy orisons",
			"Be all my sins remember'd."
		};
	}
	
	//
	// StreamExecutionEnvironment env =
	// StreamExecutionEnvironment.getExecutionEnvironment();
	//
	// DataStream<Tuple2<String, Integer>> dataStream = env
	// .socketTextStream("localhost", 9999)
	// .flatMap(new Splitter())
	// .keyBy(0)
	// .timeWindow(Time.seconds(5))
	// .sum(1);
	//
	// dataStream.print();
	//
	// env.execute("Window WordCount");
	// }
	//
	// public static class Splitter implements FlatMapFunction<String,
	// Tuple2<String, Integer>> {
	// @Override
	// public void flatMap(String sentence, Collector<Tuple2<String, Integer>>
	// out) throws Exception {
	// for (String word: sentence.split(" ")) {
	// out.collect(new Tuple2<String, Integer>(word, 1));
	// } } } }
}
