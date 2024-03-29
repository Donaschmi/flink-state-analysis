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

package org.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class ValueStateTest {

	private static final Logger LOG = LoggerFactory.getLogger(ValueStateTest.class);

	public static void main(String[] args) throws Exception {
		ParameterTool params = ParameterTool.fromArgs(args);
		String sourceTextPath = params.getRequired("sourceTextPath");
		int keysKept = Integer.parseInt(params.get("keysStored", String.valueOf(-1)));
		int keysSkipped = Integer.parseInt(params.get("keysSkipped", String.valueOf(-1)));
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> text = env.readTextFile(sourceTextPath);

		// split up the lines in pairs (2-tuples) containing: (word,1)
		text.map(new Tokenizer())
				// group by the tuple field "0" and sum up tuple field "1"
				.keyBy(value -> value.f0)
				.map(new Stater(keysKept, keysSkipped));

		env.execute("Flink Streaming Java API Skeleton");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined
	 * FlatMapFunction. The function takes a line (String) and splits it into multiple pairs in the
	 * form of "(word,1)" ({@code Tuple2<String, Integer>}).
	 */
	public static final class Tokenizer implements MapFunction<String, Tuple2<String, String>> {

		@Override
		public Tuple2<String, String> map(String value) {
			String[] tokens = value.split("\\|");
			return new Tuple2<>(tokens[0], tokens[1]);
		}
	}
	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined
	 * FlatMapFunction. The function takes a line (String) and splits it into multiple pairs in the
	 * form of "(word,1)" ({@code Tuple2<String, Integer>}).
	 */
	public static final class Stater extends RichMapFunction<Tuple2<String, String>, Tuple2<String, String>> {

		private transient ValueState<Tuple2<String, String>> state;
		private Long iterator = 0L;

		private int keysLeft;
		private int keysSkipped;

		Stater(int keysLeft, int keysSkipped) {
			this.keysLeft = keysLeft;
			this.keysSkipped = keysSkipped;
		}

		@Override
		public Tuple2<String, String> map(Tuple2<String, String> value) throws Exception {
			if (keysLeft != 0) {
				if (keysSkipped != -1) {
					if (iterator % keysSkipped == 0) {
						state.update(value);
					}
					iterator++;
				} else {
					state.update(value);
				}
				keysLeft--;
			}
			return value;
		}
		@Override
		public void open(Configuration config) {
			ValueStateDescriptor<Tuple2<String, String>> descriptor =
					new ValueStateDescriptor<>(
							"state", // the state name
							TypeInformation.of(new TypeHint<Tuple2<String, String>>() {})); // type information
			state = getRuntimeContext().getState(descriptor);
		}
	}
}
