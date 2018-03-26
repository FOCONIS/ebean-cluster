# ebean-cluster

Standard TCP cluster transport for ebean

## How does it work

Each ebean instance establishes a TCP/IP connection to each other ebean instance.

Cluster messages are sent to all other instances.

