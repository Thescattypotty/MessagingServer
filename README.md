## Messaging Server

A Java messaging broker that allows you to create topics, publish messages, and consume them through a simple TCP interface.

### Quick Start
#### Build Project :
```
javac *.java
```
#### Run Server : 
```
java MessagingServer
```
Server starts on port 9099


### Usage Examples
#### Using netcat (nc) to connect:
```
nc 127.0.0.1 9099
```

#### Available Commands :

- Create: create <topic_name> <num_partitions>
```
# Create a topic with 2 partitions
create mytopic 2

```
- Publish: publish <topic_name> <partition_index> <message>
```
# Publish a message to partition 0
publish mytopic 0 "Hello World"
```
- Consume: consume <topic_name> <partition_index> <offset>
```
# Read messages from partition 0 starting at offset 0
consume mytopic 0 0
```

#### Storage : 
- Messages are stored persistently in files under the partitions/ directory
- Each partition has its own file named <topic>-partition-<index>.log
