ip=192.168.1.5
project_dir=$HOME/dev/projects/TrainingSprints/datascience-bootcamp/MesosTest
log_dir=$project_dir/log

jar=http://$ip/mesostest_2.11-0.1.jar
jar=http://$ip/spark-examples_2.11-2.4.0.jar

spark_submit=$HOME/dev/software/spark-2.4.0-bin-hadoop2.7/bin/spark-submit

typesafe=com.typesafe:config:1.3.2

mode=cluster
#mode=client

log=$log_dir/log.txt

class=com.knoldus.training.MesosTest
class=org.apache.spark.examples.SparkPi

$spark_submit --class $class --master mesos://$ip:7077 --driver-memory 3G --executor-memory 3G --deploy-mode $mode --supervise --conf spark.master.rest.enabled=true --conf spark.mesos.executor.docker.image=$ip:spark_docker $jar 100 >& $log
#$spark_submit --class $class --master mesos://$ip:7077 --driver-memory 3G --executor-memory 3G --deploy-mode $mode --supervise --conf spark.master.rest.enabled=true $jar >& $log


