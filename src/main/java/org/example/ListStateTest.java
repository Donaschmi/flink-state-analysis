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
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
public class ListStateTest {

	public static void main(String[] args) throws Exception {
		ParameterTool params = ParameterTool.fromArgs(args);
		String sourceTextPath = params.getRequired("sourceTextPath");
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> text = env.readTextFile(sourceTextPath);

		// split up the lines in pairs (2-tuples) containing: (word,1)
		text.map(new Tokenizer())
				// group by the tuple field "0" and sum up tuple field "1"
				.keyBy(value -> value.f0)
				.map(new Stater())
				.print();

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
			String[] tokens = value.toLowerCase().split("\\|");
			return new Tuple2<>(tokens[0], tokens[1]);
		}
	}
	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined
	 * FlatMapFunction. The function takes a line (String) and splits it into multiple pairs in the
	 * form of "(word,1)" ({@code Tuple2<String, Integer>}).
	 */
	public static final class Stater extends RichMapFunction<Tuple2<String, String>, Tuple2<String, String>> {

		private transient ListState<Tuple2<String, String>> list;

		@Override
		public Tuple2<String, String> map(Tuple2<String, String> value) throws Exception {
			list.add(value);
			return value;
		}
		@Override
		public void open(Configuration config) {
			ListStateDescriptor<Tuple2<String, String>> descriptor =
					new ListStateDescriptor<>(
							"state", // the state name
							TypeInformation.of(new TypeHint<Tuple2<String, String>>() {})); // type information
			list = getRuntimeContext().getListState(descriptor);
		}
	}
}
