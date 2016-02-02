FROM ubuntu:latest

# Install dependencies
RUN apt-get -y update
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y --force-yes software-properties-common python-software-properties
RUN add-apt-repository -y ppa:webupd8team/java
RUN apt-get -y update
RUN /bin/echo debconf shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections
RUN DEBIAN_FRONTEND=noninteractive apt-get -y install oracle-java8-installer oracle-java8-set-default
#RUN DEBIAN_FRONTEND=noninteractive apt-get install -y wget dnsmasq-base python2.7 vim less iputils-ping
RUN echo "deb http://www.apache.org/dist/cassandra/debian 32x main" | sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list
RUN gpg --keyserver pgp.mit.edu --recv-keys F758CE318D77295D
RUN gpg --export --armor F758CE318D77295D | sudo apt-key add -
RUN gpg --keyserver pgp.mit.edu --recv-keys 2B5C1B00
RUN gpg --export --armor 2B5C1B00 | sudo apt-key add -
RUN gpg --keyserver pgp.mit.edu --recv-keys 0353B12C
RUN gpg --export --armor 0353B12C | sudo apt-key add -
RUN apt-get -y update
RUN apt-get -y install cassandra

EXPOSE 9160
EXPOSE 9042
EXPOSE 7199
