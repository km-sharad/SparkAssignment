This application uses SBT for build.

Prerequisite for building:<br/>
SBT
Java version 17
Spark version 3.5.1
SPARK_HOME is set
JAVA_HOME is set

Instructions for building:
Run `sbt clean package` from the root of the repo

Aplication execution command takes this form:
TimeSeriesAggregator <ts_input_dataset> <ts_output_dataset> <level>
level value: 1 = aggregation by date; 2 = aggregation by date and hour

Instructions for running:
Sample command for daily aggregation:
`$SPARK_HOME/bin/spark-submit --class com.interview.assignment.TimeSeriesAggregator ./target/scala-2.12/assignment_2.12-1.0.jar data_in/input_data.csv data_out_date 1`

Sample command for hourly aggregation:
`$SPARK_HOME/bin/spark-submit --class com.interview.assignment.TimeSeriesAggregator ./target/scala-2.12/assignment_2.12-1.0.jar data_in/input_data.csv data_out_hour 2`
