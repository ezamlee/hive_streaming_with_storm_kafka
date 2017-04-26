# hive_streaming_with_storm_kafka

Step 1: Create Hive Table
*********************************
Loan Application
*****************
Transaction Raw table:
******************************************
CREATE TABLE cu_data_raw.loan_application_transactional_raw(
application_id  string,
term_months string,
first_name string,
last_name string,
address string,
state string,
phone string,
type string,
origination_date string,
loan_decision_type string,
decided string,
loan_approved string,
denial_reason string,
loan_account_status string,
approved string,
payment_method string,
requested_amount string,
funded string,
funded_date string,
rate string,
ecoagroup string,
officer_id string,
denied_date string
)
CLUSTERED BY (loan_decision_type) into 4 buckets
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY","transactional"="true");


Step 2: Create Kafka Topics:
**********************
./kafka-topics.sh --create --zookeeper ZOOKEEPER_HOST:2181 --replication-factor 1 --partition 1 --topic HomeLoanApplication

./kafka-topics.sh --create --zookeeper ZOOKEEPER_HOST:2181 --replication-factor 1 --partition 1 --topic AutoLoanApplication

./kafka-topics.sh --create --zookeeper ZOOKEEPER_HOST:2181 --replication-factor 1 --partition 1 --topic CreditCardLoanApplication

./kafka-topics.sh --create --zookeeper ZOOKEEPER_HOST:2181 --replication-factor 1 --partition 1 --topic PersonalLoanApplication



Step 3: Publish into Kafka Topic:
*****************************
./kafka-console-producer.sh --broker-list KAFKA_BROKER_HOST:6667 --topic HomeLoanApplication < /data/datasource/home.txt

./kafka-console-producer.sh --broker-list KAFKA_BROKER_HOST:6667 --topic AutoLoanApplication < /data/datasource/auto.txt

./kafka-console-producer.sh --broker-list KAFKA_BROKER_HOST:6667 --topic CreditCardLoanApplication < /data/datasource/credit.txt

./kafka-console-producer.sh --broker-list KAFKA_BROKER_HOST:6667 --topic PersonalLoanApplication < /data/datasource/personal.txt


Step 4: Submit Storm Topology:
**********************
storm jar kafka-storm-hive-bolt-0.0.1-SNAPSHOT.jar com.storm.topology.KafkaTopology  /data/storm_test/topologyHome.properties HomeLoanApplicationTopology

storm jar kafka-storm-hive-bolt-0.0.1-SNAPSHOT.jar com.storm.topology.KafkaTopology  /data/storm_test/topologyAuto.properties AutoLoanApplicationTopology

storm jar kafka-storm-hive-bolt-0.0.1-SNAPSHOT.jar com.storm.topology.KafkaTopology  /data/storm_test/topologyCredit.properties CreditCardLoanApplicationTopology

storm jar kafka-storm-hive-bolt-0.0.1-SNAPSHOT.jar com.storm.topology.KafkaTopology  /data/storm_test/topologyPersonal.properties PersonalLoanApplicationTopology






