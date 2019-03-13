$SPARK_BASE/bin/spark-submit  \
  --master spark://localhost:7077 \
  --deploy-mode client \
  --verbose \
  --conf spark.master.rest.enabled=true \
  --jars file:///Users/iramaraju/dev/projects/training/target/scala-2.11/Training-assembly-0.1.jar\
  --class com.knoldus.training.SafeFileSample file:///Users/iramaraju/dev/projects/training/target/scala-2.11/Training-assembly-0.1.jar
