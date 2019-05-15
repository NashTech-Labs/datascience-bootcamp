bin=/home/jouko/dev/software/marathon-1.7.189-48bfd6000/bin/marathon

ip=`cat IpAddress.txt`

sudo -E env "PATH=$PATH" MESOS_NATIVE_JAVA_LIBRARY=/usr/local/lib/libmesos-1.8.0.so $bin --master zk://$ip:2181/mesos --zk zk://$ip:2181/marathon
#sudo MESOS_NATIVE_JAVA_LIBRARY=/usr/local/lib/libmesos.so $bin --master zk://192.168.1.5:2181/mesos --zk zk://192.168.1.5:2181/marathon
