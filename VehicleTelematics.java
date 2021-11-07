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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;


import java.io.*;
import java.util.*;
import java.lang.Integer;
import java.lang.String;


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
		
		// filter the rows with speed over 90
		SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> filtered;

		filtered = input.filter(new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
			@Override
			public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> d) throws Exception {
				return (d.f2 > 90);
			}
		});

			// select the needed columns to return
			SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> speedFines = filtered.map(
				new MapFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>,
					Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {
						public Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> 
						map(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> d) throws Exception{
							Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> res = new Tuple6<> (d.f0, d.f1, d.f3, d.f6, d.f5, d.f2);
							return res;
						}});
		
		// print the results for speedfines
		speedFines.writeAsCsv(outputFilePath+"/speedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
		


		
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


		
		// Print the results
		accidents.writeAsCsv(outputFilePath+"/accidents.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
		
		
		// execute
		try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

	}

}