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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.*;

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
	private static SingleOutputStreamOperator<Tuple2<String, Double>> reduceKeyDouble(SingleOutputStreamOperator<Tuple2<String, Double>> routesQ6) {
		KeyedStream<Tuple2<String, Double>, String> keyedRoutesQ6 = routesQ6.keyBy((KeySelector<Tuple2<String, Double>, String>) value -> value.f0);
		return keyedRoutesQ6.reduce(
				(ReduceFunction<Tuple2<String, Double>>) (value1, value2) ->
						new Tuple2<String, Double>(value1.f0, value1.f1 + value2.f1));
	}

	private static DataStream<String> idleTaxisQ3(DataStream<TaxiTrip> consumer){

		// Q3 - idle taxis
		KeyedStream<TaxiTrip, String> TaxiStreamKeyed = consumer.keyBy((KeySelector<TaxiTrip, String>) value -> value.medallion);
		// Pattern e1 -> e2 withing 1 hour
		Pattern<TaxiTrip, ?> patternQ3 = Pattern.<TaxiTrip>begin("start").followedBy("end").within(Time.hours(1));

		PatternStream<TaxiTrip> patternStreamKeyedQ3 = CEP.pattern(TaxiStreamKeyed, patternQ3);

		//Pattern  e1 -> e2 - e1 car = e2 car - withing 1 hour
		SingleOutputStreamOperator<Tuple2<String, Long>> idleTimes = patternStreamKeyedQ3.process(new PatternProcessFunction<TaxiTrip, Tuple2<String, Long>>() {
			@Override
			public void processMatch(Map<String
					, List<TaxiTrip>> map
					, Context context
					, Collector<Tuple2<String, Long>> collector) {

				TaxiTrip start = map.get("start").get(0);
				TaxiTrip end = map.get("end").get(0);
				if (start.medallion.equals(end.medallion)) { // Make sure start and end are the same car
					collector.collect(new Tuple2<String, Long>(start.medallion, end.dropoff_datetime.getTime() - start.dropoff_datetime.getTime()));
				}
			}
		}).name("Pattern E1 -> E2 same medallion");

		KeyedStream<Tuple2<String, Long>, String> keyedIdleTimes = idleTimes.keyBy((KeySelector<Tuple2<String, Long>, String>) value -> value.f0);

		SingleOutputStreamOperator<Tuple2<String, Long>> idleTimesTotal = keyedIdleTimes.sum(1).name("Idle time total reduce");

		Pattern<Tuple2<String, Long>, ?> patternAlert = Pattern.<Tuple2<String, Long>>begin("start").where(
				new SimpleCondition<Tuple2<String, Long>>() {
					@Override
					public boolean filter(Tuple2<String, Long> event) {
						return event.f1 >= Time.minutes(10).toMilliseconds();
					}
				}
		);
		PatternStream<Tuple2<String, Long>> idlePatterned = CEP.pattern(idleTimesTotal, patternAlert);

		return idlePatterned.process(
				new PatternProcessFunction<Tuple2<String, Long>, String>() {
					@Override
					public void processMatch(
							Map<String, List<Tuple2<String, Long>>> pattern,
							Context ctx,
							Collector<String> out) {
						out.collect("ALERT TIME IDLE BIGGER THAN 10 MINUTES IN THE LAST HOUR");
					}
				}).name("10 minutes alert collector");
	}

	private static SingleOutputStreamOperator<LinkedHashMap<String, Integer>> top10Routes(DataStream<TaxiTrip> consumer){

		// Q1 - Top 10 frequent routes during 30 mins
		DataStream<TaxiTrip> q2Gridded = consumer.map(new GridMap300());

		SingleOutputStreamOperator<Tuple2<String, Integer>> routesQ2 = q2Gridded.map(new MapFunction<TaxiTrip, Tuple2<String, Integer>> () {
				@Override
				public Tuple2<String, Integer> map(TaxiTrip value)  {
					return new Tuple2<String, Integer>(Double.toString(value.pickup_latitude) + value.pickup_longitude + value.dropoff_latitude + value.dropoff_longitude, 1);
				}
		}).name("Map trips per route unique ID");


		KeyedStream<Tuple2<String, Integer>, String> keyedRoutesQ2 = routesQ2.keyBy((KeySelector<Tuple2<String, Integer>, String>) value -> value.f0 );

		SingleOutputStreamOperator<Tuple2<String, Integer>> reducedRoutesQ2 = keyedRoutesQ2.reduce(
				(ReduceFunction<Tuple2<String, Integer>>) (value1, value2) ->
						new Tuple2<String, Integer>(value1.f0, value1.f1 + value2.f1)).name("Reduce trips by route -> Sliding window (30 m, 5s)");

		//sliding window that slides each 5 minutes and aggregates last 30 minutes of data, 5 seconds due to flink's limitations
		WindowedStream<Tuple2<String, Integer>, String, TimeWindow> keyedReducedRoutesQ2 = reducedRoutesQ2.keyBy((KeySelector<Tuple2<String, Integer>, String>) value -> value.f0)
				.window(SlidingEventTimeWindows.of(Time.minutes(30), Time.seconds(5)));

		return keyedReducedRoutesQ2.process(
				new ProcessWindowFunction<Tuple2<String, Integer>, LinkedHashMap<String, Integer>, String, TimeWindow>(){

					@Override
					public void process(String key, Context context, Iterable<Tuple2<String, Integer>> input, Collector<LinkedHashMap<String, Integer>> out) {
						TreeMap<String, Integer> sortedMap = new TreeMap<String, Integer>();

						for (Tuple2<String, Integer> t: input) {
							int count = 0;
							if (sortedMap.containsKey(t.f0)) count = sortedMap.get(t.f0);
							sortedMap.put(t.f0, count + t.f1);
						}

						LinkedHashMap<String, Integer> sortedTopN = sortedMap
								.entrySet()
								.stream()
								.limit(10)
								.collect(LinkedHashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), Map::putAll);

						out.collect(sortedTopN);
					}
				}).name("Top 10 sorted routes collector");
	}

	private static SingleOutputStreamOperator<Tuple2<String, Double>> tipsPerRoute(DataStream<TaxiTrip> consumer){
		DataStream<TaxiTrip> q6Gridded = consumer.map(new GridMap300());

		SingleOutputStreamOperator<Tuple2<String, Double>> routesQ6 = q6Gridded.map(new MapFunction<TaxiTrip, Tuple2<String, Double>> () {
			@Override
			public Tuple2<String, Double> map(TaxiTrip value)  {
				return new Tuple2<String, Double>(Double.toString(value.pickup_latitude) + value.pickup_longitude + value.dropoff_latitude + value.dropoff_longitude, value.tip_amount);
			}
		}).name("Map route tips per unique ID");

		return reduceKeyDouble(routesQ6).name("Reduce tips per route");
	}

	private static SingleOutputStreamOperator<Tuple2<String, Double>> mostPleasentDriver(DataStream<TaxiTrip> consumer){

		// Q5 - Select most pleasant taxi driver
		SingleOutputStreamOperator<Tuple2<String, Double>> driverTips = consumer.map(new MapFunction<TaxiTrip, Tuple2<String, Double>> () {
			@Override
			public Tuple2<String, Double> map(TaxiTrip value)  {
				return new Tuple2<String, Double>(value.medallion, value.tip_amount);
			}
		}).name("Map tips by driver");

		WindowedStream<Tuple2<String, Double>, String, TimeWindow> keyedReducedDriverTips = reduceKeyDouble(driverTips).name("Reduce tips by driver - window 24h").keyBy((KeySelector<Tuple2<String, Double>, String>) value -> value.f0)
				.window(TumblingEventTimeWindows.of(Time.days(1)));

		return keyedReducedDriverTips.process(
				new ProcessWindowFunction<Tuple2<String, Double>, Tuple2<String, Double>, String, TimeWindow>(){

					@Override
					public void process(String key, Context context, Iterable<Tuple2<String, Double>> input, Collector<Tuple2<String, Double>> out) {

						Tuple2<String, Double> topDriver = new Tuple2<String, Double>("placeolder", Double.NEGATIVE_INFINITY);

						for (Tuple2<String, Double> t: input) {
							if(t.f1 >= topDriver.f1){
								topDriver = t;
							}
						}

						out.collect(topDriver);
					}
				}).name("Top driver collector");
	}

	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "p2");

		// Kafka consumer with event time and watermark of 30 seconds
		DataStream<TaxiTrip> consumer = env
				.addSource(new FlinkKafkaConsumer<>("debs", new DebsSchema(), properties));
		consumer.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<TaxiTrip>(Time.seconds(30)) {

								   @Override
								   public long extractTimestamp(TaxiTrip element) {
									   return element.dropoff_datetime.getTime();
								   }
		}).name("Kafka taxi trip consumer");


		// Q3 - Alert idle taxis when greater then 10 minutes
		idleTaxisQ3(consumer).writeAsText("log/q3_idle_alerts.txt", FileSystem.WriteMode.OVERWRITE).name("Q3 - Idle time alerts");

		// Q1 - Top 10 frequent routes during 30 minutes - Extra query for CEP purposes
		top10Routes(consumer).writeAsText( "log/q1_most_frequent.txt", FileSystem.WriteMode.OVERWRITE).name("Q1 - Top 10 frequent routes");

		// Q6 - tips hall of fame
		tipsPerRoute(consumer).writeAsText( "log/q6_route_tips.txt", FileSystem.WriteMode.OVERWRITE).name("Q6 - Tips hall of fame");

		// Q5 - most pleasant driver
		mostPleasentDriver(consumer).writeAsText( "log/q5_pleasantDriver.txt", FileSystem.WriteMode.OVERWRITE).name("Q5 - Most pleasant driver");

		// execute program
		env.execute("Streaming Taxi Trips");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	public static final class GridMap300 implements MapFunction<TaxiTrip, TaxiTrip>
	{
		@Override
		public TaxiTrip map(TaxiTrip value)  {
			value.pickup_longitude =  (int) (-1*((-74.913585 - value.pickup_longitude) / 0.005986 ) + 1);
			value.pickup_latitude =  (int) (((41.474937 - value.pickup_latitude)/ 0.004491556) + 1);
			value.dropoff_longitude = (int)  (-1*((-74.913585 - value.dropoff_longitude) / 0.005986 ) + 1 );
			value.dropoff_latitude = (int)  (((41.474937 - value.dropoff_latitude)/ 0.004491556) + 1);
			return value;
		}
	}

}
