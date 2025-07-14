This application uses SBT for build.

Prerequisite for building:<br/>
SBT <br/>
Java version 17 <br/>
Spark version 3.5.1 <br/>
SPARK_HOME is set <br/>
JAVA_HOME is set <br/>

Instructions for building: <br/>
Run `sbt clean package` from the root of the repo

Aplication execution command takes this form: <br/>
TimeSeriesAggregator <ts_input_dataset> <ts_output_dataset> <op_level> <br/>
op_level value: 1 = aggregation by date; 2 = aggregation by date and hour

Instructions for running: <br/>
Sample command for daily aggregation: <br/>
`$SPARK_HOME/bin/spark-submit --class com.interview.assignment.TimeSeriesAggregator ./target/scala-2.12/assignment_2.12-1.0.jar data_in/input_data.csv data_out_date 1`

Sample command for hourly aggregation: <br/>
`$SPARK_HOME/bin/spark-submit --class com.interview.assignment.TimeSeriesAggregator ./target/scala-2.12/assignment_2.12-1.0.jar data_in/input_data.csv data_out_hour 2`
