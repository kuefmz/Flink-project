package master;

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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Date;

public class VehicleTelematics {

	public static void main(String[] args) throws Exception {

		// input file path
		String inputFilePath = args[0];
		// output file path
		String outputFilePath = args[1];		

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data reading the text file from given input path
        DataStream<String> text = env.readTextFile(inputFilePath).setParallelism(1);
		
		// parse input to Tuple8
		SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> input = text.
		map(new MapFunction<String, Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
			public Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> map(String in) throws Exception{
				String[] fieldArray = in.split(",");
				Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> out = new Tuple8(
					Integer.parseInt(fieldArray[0]), //Time in seconds
					Integer.parseInt(fieldArray[1]), //VehicleID
					Integer.parseInt(fieldArray[2]), //speed in mph (0-100)
					Integer.parseInt(fieldArray[3]), //highway (0-L-1)
					Integer.parseInt(fieldArray[4]), //lane of the highway (0-4)
					Integer.parseInt(fieldArray[5]), //Direction, 0-Eastbound, 1-Westbound
					Integer.parseInt(fieldArray[6]), //Segment (0-99)
					Integer.parseInt(fieldArray[7])); //Horizontal position (0, 527999)
				return out;
			}
		}).setParallelism(1);




		// Speed radar

		SingleOutputStreamOperator speedFines = input.filter(new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
			@Override
			public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> d) throws Exception {
				return (d.f2 > 90);
			}
		}).map(
			new MapFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>,
				Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {
					public Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> 
					map(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> d) throws Exception{
						Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> res = new Tuple6<> (d.f0, d.f1, d.f3, d.f6, d.f5, d.f2);
						return res;
					}});
		
		// print the results for speedfines
		speedFines.writeAsCsv(outputFilePath+"/speedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);


		// ---------------------------------------------------------------------------------
		// Average speed


		SingleOutputStreamOperator avgspeed =
				input.filter(new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
							@Override
							public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> d) throws Exception {
								return ((52 == d.f6) || (d.f6 == 56));
							}
						}).setParallelism(1)
						.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
							@Override
							public long extractAscendingTimestamp(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> x) {
								return x.f0 * 1000;
							}})
						.map(new MapFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
							@Override
							public Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> map(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> x) {
								// <Time1, Time2, VID, XWay, Dir, Seg1, Seg2, Pos1, Pos2>
								Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> res = new Tuple9<>(x.f0, x.f0, x.f1, x.f3, x.f5, x.f6, x.f6, x.f7, x.f7);
								return res;
							}
						})
						.keyBy(2, 3, 4)
						.window(EventTimeSessionWindows.withGap(Time.seconds(300)))
						.reduce(new ReduceFunction<Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
							@Override
							public Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> reduce(Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> x, Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> y) throws Exception {
								//handle
								int dir = x.f4;
								int startSeg, endSeg, startPos, endPos, startTime, endTime;
								if (dir == 0) {
									startSeg = Math.min(x.f5, y.f5);
									endSeg = Math.max(x.f6, y.f6);
									startPos = Math.min(x.f7, y.f7);
									endPos = Math.max(x.f8, y.f8);
								} else {
									startSeg = Math.max(x.f5, y.f5);
									endSeg = Math.min(x.f6, y.f6);
									startPos = Math.max(x.f7, y.f7);
									endPos = Math.min(x.f8, y.f8);
								}
								startTime = Math.min(x.f0, y.f0);
								endTime = Math.max(x.f1, y.f1);
								// <Time1, Time2, VID, XWay, Dir, Seg1, Seg2, Pos1, Pos2>
								return new Tuple9<>(startTime, endTime, x.f2, x.f3, dir, startSeg, endSeg, startPos, endPos);
							}
						})
						.filter(new FilterFunction<Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
							@Override
							public boolean filter(Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> d) throws Exception {
								return (((d.f5 == 52) && (d.f6 == 56)) || ((d.f5 == 56) && (d.f6 == 52)));
							}
						})
						.map(new MapFunction<Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple6<Integer, Integer, Integer, Integer, Integer, Double>>() {
							@Override
							public Tuple6<Integer, Integer, Integer, Integer, Integer, Double> map(Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> d) {
								Double distanceMiles = Math.abs(d.f8 - d.f7) / 1609.0;
								Double timeHours = (d.f1 - d.f0) / 3600.0;
								Double AvgSpeed = distanceMiles / timeHours;
								// <Time1, Time2, VID, XWay, Dir, AvgSpd>
								return new Tuple6<>(d.f0, d.f1, d.f2, d.f3, d.f4, AvgSpeed);
							}
						})
						.filter(new FilterFunction<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>>() {
							@Override
							public boolean filter(Tuple6<Integer, Integer, Integer, Integer, Integer, Double> d) throws Exception {
								return d.f5 > 60.0;
							}
						});
		avgspeed.writeAsCsv(outputFilePath + "/avgspeedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);




		// Accident Reporter
	
		// A map function that works on the previously filtered and windowed data calculates the start and stop time of each 4 element and returns a tuple7
		class accidentMap implements WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple, GlobalWindow> {
			@Override 
			public void apply(Tuple key, 
							  GlobalWindow gw, 
							  Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> it,
							  Collector<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> c) throws Exception {	
			
			int count = 0;
			Integer startTime = 0, stopTime =0;
			List<Integer> times = new ArrayList<Integer>();
			for(Tuple8 i : it) {
				count += 1;
				times.add((int)i.f0);	
			}
			if(count == 4){
				startTime = Collections.min(times);
				stopTime = Collections.max(times);
				c.collect(new Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>(startTime, stopTime, key.getField(0), key.getField(1), key.getField(2), key.getField(3), key.getField(4)));
			}
		}
	}

		// filter to speed 0
		SingleOutputStreamOperator accidents = input.filter(new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
			@Override
			public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> d) throws Exception {
				return (d.f2 == 0);
			}
		}).setParallelism(1)
			.keyBy(1, 3, 6, 5, 7) // keyby in order to count the wehicles only if they are stopped in the same place
			.countWindow(4, 1) //window of 4 elements slided by each element
			.apply(new accidentMap()); //applyting the map function
		
		// Print the results for accidents
		accidents.writeAsCsv(outputFilePath+"/accidents.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
		
		// execute
		try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

	}

}