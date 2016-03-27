# cs505ex6
### Usage
    main.py [-p <port>] [-h <hostfile>] [-f <max_crashes>]
### Remote HostFile Example
    1 xinu01.cs.purdue.edu
    2 xinu02.cs.purdue.edu
    3 xinu03.cs.purdue.edu
    ...
### Local HostFile Example
    1 localhost 1024
    2 localhost 1025
    3 localhost 1026
    ...
### Extra
    You can use bashstart [<num_of_nodes>] to start a cluster with <num_of_nodes> on local machine. Note that since this runs on local machine, all nodes will print to the same terminal.