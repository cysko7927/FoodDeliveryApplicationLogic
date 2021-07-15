#!/bin/bash

bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic ShowOrder --partitions 1 --replication-factor 3
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic OrderCreation --partitions 1 --replication-factor 3
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic UpdateQuantityItem --partitions 1 --replication-factor 3
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic ShowItem --partitions 1 --replication-factor 3
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic NotifyCompleteShipping --partitions 1 --replication-factor 3
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic NotifyUser --partitions 1 --replication-factor 3
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic UserRegistration --partitions 1 --replication-factor 3
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic ShippingCreation --partitions 1 --replication-factor 3
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic ShowUserData --partitions 1 --replication-factor 3
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic ShowShippingNotCompleted --partitions 1 --replication-factor 3
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic CompleteShipping --partitions 1 --replication-factor 3
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic UpdateAddressShippingUser --partitions 1 --replication-factor 3



exit
