## Building an automatic data profiling and reporting solution with Amazon EMR, AWS Glue, and Amazon QuickSight

GitHub code repository for the “Data Profiler For AWS Glue Data Catalog” application described in the AWS Big Data Blog post "[Building an automatic data profiling and reporting solution with Amazon EMR, AWS Glue, and Amazon QuickSight](https://aws.amazon.com/blogs/big-data/build-an-automatic-data-profiling-and-reporting-solution-with-amazon-emr-aws-glue-and-amazon-quicksight/)".

![Architecture](./resources/architecture.png?raw=true "Architecture")

This GitHub  repository contains the code for the Data Profiler for the AWS Glue Data Catalog application described in the blog post.

Data Profiler for AWS Glue Data Catalog is an Apache Spark Scala application that profiles all the tables defined in a database in the Data Catalog using the profiling capabilities of the [Amazon Deequ](https://github.com/awslabs/deequ) library and saves the results in the Data Catalog and an Amazon S3 bucket in a partitioned Parquet format.

For more information about the Amazon Deequ data library, see [Test data quality at scale with Deequ](https://aws.amazon.com/blogs/big-data/test-data-quality-at-scale-with-deequ/).

The Deequ library [does not support tables with nested data](https://github.com/awslabs/deequ/issues/118) (such as JSON). If you want to run the application on a table with nested data, this must be un-nested/flattened or relationalized before profiling. For more information about useful transforms for this task, see [AWS Glue Scala DynamicFrame APIs](https://docs.aws.amazon.com/glue/latest/dg/glue-etl-scala-apis-glue-dynamicframe.html) or [AWS Glue PySpark Transforms Reference](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-transforms.html).

### Application Configuration and Build

You can run the application via `spark-submit` on a transient or permanent Amazon EMR cluster with Spark installed and configured with the Data Catalog settings option Use for Spark table metadata enabled. The minimum version supported of EMR is **emr-5.28.0**.

![Amazon EMR Configuration](./resources/emr.png?raw=true "Amazon EMR Configuration")

Clone this GitHub repository or download the source code and build the uber jar using the [Scala Build Tool](https://www.scala-sbt.org/) (`sbt`).

```sh
$ export SBT_OPTS="-Xms1G -Xmx3G -Xss2M -XX:MaxMetaspaceSize=3G" && sbt assembly
```

By default, the .jar file is created in the following path, relative to the project root directory:

`./target/scala-2.11/data-profiler-for-aws-glue-data-catalog-assembly-1.0.jar`

Copy the jar to the EMR master node and run the application with a command like the one below (only the dbName and region parameters are required, see next section).

```sh
$ spark-submit \
--class awsdataprofiler.DataProfilerForAWSGlueDataCatalog \
--master yarn \
--deploy-mode cluster \
--name data-profiler-for-aws-glue-data-catalog \
/home/hadoop/data-profiler-for-aws-glue-data-catalog-assembly-1.0.jar 
--dbName nyctlcdb \
--region eu-west-1 \
--compExp true \
--statsPrefix DQP \
--s3BucketPrefix deequ-profiler/deequ-profiler-metrics \
--profileUnsupportedTypes true \
--noOfBins 30 \
--quantiles 10
```

### Input Parameters

The following table summarizes the input parameters that the application accepts.

|Name|Type|Required|Default|Description|
|----|----|--------|-------|-----------|
|`--dbName (-d)`|String|Yes|N/A| Data Catalog database name. The database must be defined in the Catalog owned by the same account where the application is executed. |
|`--region (-r)`|String|Yes|N/A| AWS Region endpoint where the Data Catalog database is defined, for example `us-west-1` or `us-east-1`. For more information, see [Regional Endpoints](https://docs.aws.amazon.com/general/latest/gr/rande.html#regional-endpoints). |
|`--compExp (-c)`|Boolean|No|`false`|If `true`, the application also executes “expensive” profiling [analyzers](https://github.com/awslabs/deequ/tree/master/src/main/scala/com/amazon/deequ/analyzers) on Text columns. These are [CountDistinct](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/analyzers/CountDistinct.scala), [Entropy](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/analyzers/Entropy.scala), [Histogram](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/analyzers/Histogram.scala), [UniqueValueRatio](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/analyzers/UniqueValueRatio.scala), and [Uniqueness](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/analyzers/Uniqueness.scala). If `false`, only the following default analyzers are executed: [ApproxCountDistinct](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/analyzers/ApproxCountDistinct.scala), [Completeness](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/analyzers/Completeness.scala), [Distinctness](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/analyzers/Distinctness.scala), [MaxLength](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/analyzers/MaxLength.scala), [MinLength](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/analyzers/MinLength.scala). All analyzers for Numeric columns are always executed.|
|`--statsPrefix (-p)`|String|No|DQP|String prepended to the statistics names in the Data Catalog. The application also adds two underscores (__). This is useful to identify metrics calculated by the application.|
|`--s3BucketPrefix (-s)`|String|No|_blank_|Format must be `s3Buckename/prefix`. If specified, the application writes Parquet files with metrics in the prefixes `db_name=.../table_name=...`|
|`--profileUnsupportedTypes (-u)`|Boolean|No|`false`|By default, the Amazon Deequ library only supports Text and Numeric columns. If this parameter is set to true, the application also profiles columns of type Boolean and Date.|
|`--noOfBins (-b)`|Integer|No|10|When `--compExp (-c)` is `true`, sets the [number of maximum values](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/analyzers/Histogram.scala#L44) to create for the [Histogram](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/analyzers/Histogram.scala) analyzer for String columns.|
|`--quantiles (-q)`|Integer|No|10|Sets the number of [quantiles](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/analyzers/ApproxQuantiles.scala#L34) to calculate for the [ApproxQuantiles](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/analyzers/ApproxQuantiles.scala) analyzer for numeric columns.|

### Usage Example

Please see the [AWS Big Data blog post](https://aws.amazon.com/blogs/big-data/build-an-automatic-data-profiling-and-reporting-solution-with-amazon-emr-aws-glue-and-amazon-quicksight/) associated with this repository to see a detailed walkthrough on how to use this applicaiton.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
