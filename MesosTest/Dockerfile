FROM ubuntu:latest

RUN apt-get update
RUN apt-get install -y wget
RUN apt-get install -y tar git
RUN apt-get install -y openjdk-8-jdk
RUN apt-get install -y autoconf libtool
RUN apt-get -y install build-essential python-dev python-six python-virtualenv libcurl4-nss-dev libsasl2-dev libsasl2-modules maven libapr1-dev libsvn-dev zlib1g-dev iputils-ping
RUN wget https://archive.apache.org/dist/mesos/1.6.0/mesos-1.6.0.tar.gz
RUN tar -xzf mesos-1.6.0.tar.gz
RUN cd mesos-1.6.0
RUN mkdir build
RUN cd build
RUN mesos-1.6.0/configure
RUN make
RUN make install

