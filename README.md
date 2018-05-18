# SimpleDynamo
This is an android project which implements a Simple Dynamo similar to Amazon Dynamo.
The port configuration used in the app is as follows: Each emulator listens on port 10000, but it will connect to a different port number on the IP address 10.0.2.2 for each emulator, as follows: emulator serial port emulator-5554 11108 emulator-5556 11112 emulator-5558 11116 emulator-5560 11120 emulator-5562 11124.

This application implements a distributed key-value storage system that provides both availability and linearizability. It also performs successful read and write operations even in the presence of failures.

It also implements :
• Data replication
• Data partitioning
• Handle node failures while continuing to provide availability and linearizability

Following assumptions are made in this project:
1. There will be at most one app instance failure.
2. There will be five nodes in the system.
3. Each node knows its position in the ring and about every other node in the ring.
4. There are no virtual nodes in the ring unlike Amazon Dynamo.
5. It does not implement hinted handoff.
6. There are only three replicas stored in two consecutive nodes from where the key belongs to.
