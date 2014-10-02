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

package com.cloudera.sparkavro;

import java.util.List;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.mapreduce.Job;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;

public class JavaSparkAvroReader {

  public static void main(String [] args) throws IOException {

    SparkConf scc = new SparkConf();
    scc.setAppName("Spark Avro");
    scc.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    scc.set("spark.kryo.registrator", "com.cloudera.sparkavro.AvroRegistrator" );

    JavaSparkContext sc = new JavaSparkContext( scc );

    String inPath = args[0];
    Job    job    = new Job();
    FileInputFormat.setInputPaths(job, inPath);
    AvroJob.setInputKeySchema(job, User.getClassSchema());
    Configuration conf = job.getConfiguration();

    JavaPairRDD records = sc.newAPIHadoopRDD(
        conf, AvroKeyInputFormat.class,
        AvroKey.class, NullWritable.class);

    List< String > lst = records.map(
      new Function< Tuple2, String > () {
        public String call(Tuple2 tup) {
          AvroKey< User > data = (AvroKey< User >) tup._1;
          return data.datum().getName();
        }
      }).collect();

    for( String name: lst ){
      System.out.println( name );
    }
  }
}
