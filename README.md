2015/09/05

Some samples using apache spark interacting with apache cassandra, using scala and maven.

a fork from https://github.com/koeninger/spark-cassandra-example, thank you Cody Koeninger

instead of using sbt, i am using maven and the latest versions of the libraries:

<properties>
		<project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
		<java.version>1.7</java.version>
		<scala.version>2.11.2</scala.version>
		<scala.version.tools>2.11</scala.version.tools>
		<spark-cassandra-connector_2.11.version>1.5.0-M1</spark-cassandra-connector_2.11.version>
		<spark-cassandra-connector-java_2.11.version>1.5.0-M1</spark-cassandra-connector-java_2.11.version>
		<spark-core_2.11.version>1.5.1</spark-core_2.11.version>
		<spark-streaming_2.11.version>1.5.1</spark-streaming_2.11.version>
		<junit.version>4.11</junit.version>
		<scalacheck.version>1.11.4</scalacheck.version>
		<scalatest.version>2.2.0</scalatest.version>
</properties>

To run every sample in eclipse, just Right click, Run as, scala application.


