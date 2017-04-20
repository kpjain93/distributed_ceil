# distributed_ceil

### pre-requisites

 * [Spark]  (http://spark.apache.org/)   0.9.0 or later
 * [graphX]  (http://spark.apache.org/docs/latest/graphx-programming-guide.html) 
 * [Gradle] (http://www.gradle.org/) 

### build

If necessary edit the build.gradle file to set your version of spark and graphX. Execute the following command inside kapil_mtp directory.

> gradle clean dist

Check the build/dist folder for kapil_mtp-0.1.jar.


### Running Distributed CEIL

After building the package (See above), you can execute the distributed CEIL algorithm against an edge list using the provided script

```
bin/dceil [options] [<property>=<value>....]

  -i <value> | --input <value>
        input file or path  Required.
  -o <value> | --output <value>
        output path Required
  -m <value> | --master <value>
        spark master, local[N] or spark://host:port default=local
  -h <value> | --sparkhome <value>
        SPARK_HOME Required to run on cluster
  -n <value> | --jobname <value>
        job name
  -p <value> | --parallelism <value>
        sets spark.default.parallelism and minSplits on the edge file. default=based on input partitions
  -x <value> | --minprogress <value>
        Number of vertices that must change communites for the algorithm to consider progress. default=2000
  -y <value> | --progresscounter <value>
        Number of times the algorithm can fail to make progress before exiting. default=1
  -d <value> | --edgedelimiter <value>
        specify input file edge delimiter. default=","
  -j <value> | --jars <value>
        comma seperated list of jars
  -z <value> | --ipaddress <value>
        Set to true to convert ipaddresses to Long ids. Defaults to false
  <property>=<value>....
```

To run a small local example execute:
```
bin/dceil -i examples/input150.txt -o test_output --edgedelimiter "\t" 2> stderr.txt
```

Spark produces a lot of output, so sending stderr to a log file is recommended.  Examine the test_output folder. you should see the structure as shown below:

```
test_output/
├── level_0_vertices
│   ├── _SUCCESS
│   └── part-00000

```

```

### running DCEIL on a cluster

Inside spark directory, run the following command:
 bin/spark-submit --class kapil_mtp.dceil.Main [options] [<property>=<value>....]

To run on a cluster, be sure the output path should be of the form "hdfs://<namenode>/path" and ensure you provide the --master and --sparkhome options. The --jars option is already set by the dceil script itself and need not be applied.

For example, once the master and slave machines are started in Apache Spark, the command like below can be executed inside spark folder:

  ./bin/spark-submit --class kapil_mtp.dceil.Main --master spark://10.200.6.49:7078 --total-executor-cores 128 --executor-cores 8 --executor-memory 10g --driver-memory 10g  /home/ildsmaster2/masternode2/kapil/dceil_no_output/target/dceil-1.0-SNAPSHOT-jar-with-dependencies.jar -i /home/ildsmaster2/masternode2/kapil/com-amazon-ungraph.txt -o hdfs://10.6.45.231:8020/kapil/test_output  -p 64 -d "\t"

### parallelism

To change the level of parallelism use the -p or --parallelism option.  If this option is not set parallelism will be based on the layout of the input data in HDFS.  The number of partitions of the input file sets the level of parallelism.
