ip=192.168.1.5
main_dir=$HOME/dev/projects/TrainingSprints/datascience-bootcamp/LendingClubMl_2
resources_dir=$main_dir/data

#jar=$main_dir/target/scala-2.11/lendingclubml_2.11-0.1.jar
jar=http://$ip/lendingclubml_2.11-0.1.jar

spark_submit=$HOME/dev/software/spark-2.4.0-bin-hadoop2.7/bin/spark-submit

#csv=$resources_dir/loan_truncated.csv
csv=$resources_dir/loan_removed_rows.csv
lr_model=$resources_dir/LogisticRegression_2
lsvm_model=$resources_dir/lsvm_2
dt_model=$resources_dir/dt_2

log=$resources_dir/LendingClubMl_AllFeatures.txt

aardpfark=$HOME/dev/software/aardpfark/target/scala-2.11/aardpfark-assembly-0.1.0-SNAPSHOT.jar
hadrian=/home/jouko/dev/software/hadrian/hadrian/target/hadrian-0.8.4.jar

mode=cluster
#mode=client

rm -r $lr_model
rm -r $lsvm_model
rm -r $dt_model

$spark_submit --class com.knoldus.training.Main --master spark://$ip:7077 --driver-memory 7G --executor-memory 4G --deploy-mode $mode --jars $aardpfark,$hadrian $jar $csv $lr_model $lsvm_model $dt_model >& $log

