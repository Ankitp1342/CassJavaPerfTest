# cass-java-perf-test
Cassandra performance test using client java beans


The source code in this project will allow users to plug in their own datamodel on performance testing a Cassandra cluster. The launch paramaters for the project are in resources/vm_args.txt file. The main class for running the test is src/main/java/perftest/PerfTest.java



## Custom-Data-Model

Custom data model can be configured by changing the code in java bean: src/main/java/perftest/bean/ClientBean.java.

