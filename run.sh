rm -f test-data.avro
hdfs dfs -rm /tmp/test-data.avro

java -jar ../avro-tools-1.7.6.jar random --schema-file src/main/resources/user.avsc --count 100 test-data.avro
hdfs dfs -put test-data.avro /tmp/test-data.avro

spark-submit  --master local                            \
  --class com.cloudera.sparkavro.JavaSparkAvroReader    \
  sparkavroapp-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
  /tmp/test-data.avro
