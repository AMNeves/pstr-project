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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.*;

import static java.lang.Math.min;

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
	private static final int SPEEDUP_FACTOR = 60;
	private static SingleOutputStreamOperator<Tuple2<String, Double>> reduceKeyDouble(DataStream<Tuple2<String, Double>> routesQ6) {
		KeyedStream<Tuple2<String, Double>, String> keyedRoutesQ6 = routesQ6.keyBy((KeySelector<Tuple2<String, Double>, String>) value -> value.f0);
		return keyedRoutesQ6.reduce(
				(ReduceFunction<Tuple2<String, Double>>) (value1, value2) ->
						new Tuple2<String, Double>(value1.f0, value1.f1 + value2.f1));
	}

	private static SingleOutputStreamOperator<Long> idleTaxisQ3(DataStream<TaxiTrip> consumer){

		// Q3 - idle taxis
		KeyedStream<TaxiTrip, String> TaxiStreamKeyed = consumer.keyBy((KeySelector<TaxiTrip, String>) value -> value.medallion);

		Pattern<TaxiTrip, ?> patternQ3 = Pattern.<TaxiTrip>begin("start").followedBy("end").within(Time.seconds(60));

		PatternStream<TaxiTrip> patternStreamKeyedQ3 = CEP.pattern(TaxiStreamKeyed, patternQ3);

		//Pattern  e1 -> e2 - e1 car = e2 car - withing 1 hour
		SingleOutputStreamOperator<Long> idleTimes = patternStreamKeyedQ3.select(new PatternSelectFunction<TaxiTrip, Long>() {
			@Override
			public Long select(Map<String, List<TaxiTrip>> pattern) throws Exception {
				TaxiTrip start = pattern.get("start").get(0);
				TaxiTrip end = pattern.get("end").get(0);
				// Make sure start and end are the same car

				if(start.medallion.equals(end.medallion))
					return end.pickup_datetime.getTime() - start.dropoff_datetime.getTime();
				else
					return 0L;
			}
		}).name("Pattern E1 -> E2 same medallion");

		SingleOutputStreamOperator<Long> filteredAlerts = idleTimes.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(60), Time.seconds(1)))
				.sum(0).name("Aggregate all window")
				.filter(new FilterFunction<Long>() {
					@Override
					public boolean filter(Long value) throws Exception {
						return value >= Time.minutes(10).toMilliseconds();
					}
		}).name("Alert 10 minutes filter");

		SingleOutputStreamOperator<String> alertsIdle = filteredAlerts.map(new MapFunction<Long, String>() {
			@Override
			public String map(Long value) throws Exception {
				return "ALERT TIME IDLE BIGGER THAN 10 MINUTES: " + value;
			}
		}).name("10 minutes alert map");

		return filteredAlerts;
	}

	private static SingleOutputStreamOperator<Tuple2<String, Integer>> top10Routes(DataStream<TaxiTrip> consumer){

		// Q1 - Top 10 frequent routes during 30 mins
		DataStream<TaxiTrip> q2Gridded = consumer.map(new GridMap300());

		SingleOutputStreamOperator<TaxiTrip> q2GriddedFilter = q2Gridded.filter(new FilterFunction<TaxiTrip>() {
			@Override
			public boolean filter(TaxiTrip value) throws Exception {
				return value.pickup_longitude >= 1 && value.pickup_latitude >= 1 && value.dropoff_longitude <= 300 && value.dropoff_latitude <= 300
						&& value.pickup_longitude <= 300 && value.pickup_latitude <= 300 && value.dropoff_longitude >= 1 && value.dropoff_latitude >= 1;
			}
		}).name("Out of bounds filter");

		SingleOutputStreamOperator<Tuple2<String, Integer>> routesQ2 = q2GriddedFilter.map(new MapFunction<TaxiTrip, Tuple2<String, Integer>> () {
				@Override
				public Tuple2<String, Integer> map(TaxiTrip value)  {
					return new Tuple2<String, Integer>(Double.toString(value.pickup_latitude) + value.pickup_longitude + value.dropoff_latitude + value.dropoff_longitude, 1);
				}
		}).name("Map trips per route unique ID");

		SingleOutputStreamOperator<Tuple2<String, Integer>> keyedReducedWindowRoutesQ2 = routesQ2.keyBy((KeySelector<Tuple2<String, Integer>, String>) value -> value.f0)
				.window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
				.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
					@Override
					public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
						return new Tuple2<String, Integer>(value1.f0, value1.f1 + value2.f1);
					}
				});

		SingleOutputStreamOperator<Tuple2<String, Integer>> top10Routes = keyedReducedWindowRoutesQ2.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(30)))
				.process(new ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow>() {
					@Override
					public void process(Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {

						List<Tuple2<String, Integer>> sortedList = new ArrayList<>();
						for (Tuple2<String, Integer> e : elements) {
							sortedList.add(e);
						}

						sortedList.sort(new Comparator<Tuple2<String, Integer>>() {
							@Override
							public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
								return o2.f1.compareTo(o1.f1);
							}
						});

						for (int i = 0; i < min(10, sortedList.size()); i++) {
							out.collect(sortedList.get(i));
						}
						SimpleDateFormat formatter= new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss z");
						Date date = new Date(System.currentTimeMillis());
						out.collect(new Tuple2<String, Integer>("-------------------------------------" + date, 0));
					}
				}).name("WindowAll sorter");
		
		return top10Routes;
	}

	private static SingleOutputStreamOperator<Tuple2<String, Double>> tipsPerRoute(DataStream<TaxiTrip> consumer){
		DataStream<TaxiTrip> q6Gridded = consumer.map(new GridMap300());

		SingleOutputStreamOperator<TaxiTrip> q6GriddedFilter = q6Gridded.filter(new FilterFunction<TaxiTrip>() {
			@Override
			public boolean filter(TaxiTrip value) throws Exception {
				return value.pickup_longitude >= 1 && value.pickup_latitude >= 1 && value.dropoff_longitude <= 300 && value.dropoff_latitude <= 300
						&& value.pickup_longitude <= 300 && value.pickup_latitude <= 300 && value.dropoff_longitude >= 1 && value.dropoff_latitude >= 1;
			}
		}).name("Out of bounds filter");

		SingleOutputStreamOperator<Tuple2<String, Double>> routesQ6 = q6GriddedFilter.map(new MapFunction<TaxiTrip, Tuple2<String, Double>> () {
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

		WindowedStream<Tuple2<String, Double>, String, TimeWindow> driverTipsWindowed = driverTips.keyBy((KeySelector<Tuple2<String, Double>, String>) value -> value.f0)
				.window(TumblingProcessingTimeWindows.of(Time.seconds(30)));

		SingleOutputStreamOperator<Tuple2<String, Double>> reducedTips = driverTipsWindowed.reduce(new ReduceFunction<Tuple2<String, Double>>() {
			@Override
			public Tuple2<String, Double> reduce(Tuple2<String, Double> value1, Tuple2<String, Double> value2) throws Exception {
				return new Tuple2<String, Double>(value1.f0, value1.f1 + value2.f1);
			}
		}).name("Reduce tips by driver - window 24h");

		return reducedTips.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(30))).process(new ProcessAllWindowFunction<Tuple2<String, Double>, Tuple2<String, Double>, TimeWindow>() {
			@Override
			public void process(Context context, Iterable<Tuple2<String, Double>> elements, Collector<Tuple2<String, Double>> out) throws Exception {
				Tuple2<String, Double> max = new Tuple2<String, Double>("min", -1.0);

				for(Tuple2<String, Double> e : elements){
						if(e.f1 >= max.f1)
							max = e;
				}
				out.collect(max);

				SimpleDateFormat formatter= new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss z");
				Date date = new Date(System.currentTimeMillis());
				out.collect(new Tuple2<String, Double>("---------------------" + date, 0.0));
			}
		}).name("Aggregate all window max");
	}

	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092,kafka:9092" );
		properties.setProperty("group.id", "p2");

		// Kafka consumer with event time and watermark
		DataStream<TaxiTrip> consumer = env
				.addSource(new FlinkKafkaConsumer<>("debs", new DebsSchema(), properties));

//		SingleOutputStreamOperator<TaxiTrip> consumerTimed = consumer.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<TaxiTrip>() {
//
//			@Override
//			public long extractAscendingTimestamp(TaxiTrip element) {
//				return element.dropoff_datetime.getTime();
//			}
//
//		}).name("Kafka taxi trip consumer Watermarked");


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
