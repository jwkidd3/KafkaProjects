kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 3 --partitions 1 --topic callback-hello-producer --config min.insync.replicas=3