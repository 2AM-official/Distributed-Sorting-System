# **Distributed Sorting System**

## Distributed Sort specifications

- ### The basic sort specifications
  This project will read, sort, and write files consisting of zero or
  more records. A record is a 100 byte binary key-value pair, consisting
  of a 10-byte key and a 90-byte value. Each key should be interpreted
  as an unsigned 10-byte (80-bit) integer. Your sort should be in ascending order,
  meaning that the output should have the record with the smallest key first,
  then the second-smallest, etc.

- ### Distributed program specifications
  Complete the `netsort.go` program that will concurrently run on multiple machines. The program
  should broadly have the following architecture/specs :  
  - read the input file,
  - appropriately partition the data,
  - send relevant data to peer servers,
  - receive data from peers,
  - sort the data,
  - write the sorted data to the output file

- ### Input params to `netsort`
  Usage : `netsort <serverId> <inputFilePath> <outputFilePath> <configFilePath>`
  + **serverId** : integer-valued id starting from 0. For example, if there are 4 servers, they would have ids 0, 1, 2, 3.
  + **inputFilePath** : input file path of a particular server of the form `path/to/input-{serverId}.dat`.
  + **outputFilePath** : input file path of a particular server of the form `path/to/output-{serverId}.dat`.
  + **configFilePath** : path to the config file of the form `path/to/server.yaml`

- ### Basic assumptions of the distributed nature of the application
  1. **Number of servers** : We make a simplifying assumption that, at a time, the number of machines running `netsort.go` would be a power of 2.
     For example, there could be 2, 4, 8 ... machines running the same program to sort the input data. 
     Finally, it is the number of servers defined in the config (`servers.yaml`) that determines this. 
  2. **Data partition algorithm** : Given a record with a 10 byte key and assuming 2^N 
     servers, we would use the most significant N bits to map this record to the 
     appropriate server. For example, in a system with 4 servers, 
     if we encounter a record with a key starting with `1101`, it would belong to 
     `serverId : 3`. <br/>
     This partitioning scheme would mean, if we were to concatenate
     the 4 output files in case of 4 servers, in serial order i.e <output file from server 0> ++
     <output file from server 1> ++ <output file from server 2> ++ <output file from server 3>
     , they would appear as a single sorted file according to the keys of the records.
  3. When dealing with client/server architectures, it's almost always required to accept
     multiple client connections and handle them concurrently. You are required to build up
     on that same idea when implementing the `netsort` routine.
  4. For large data transfers, it's usually a good idea to make sure there are no 
     race conditions when accepting data otherwise you may face all sorts of issues related to concurrency.

- ### Dealing with sockets
  - You will be required to use the [net package](https://pkg.go.dev/net) that go provides
    to listen/serve data from peers and also send data to peers.
  - The `config/server.yaml` file is already being read as part of the starter code into
    the `netsort` routine. Use the host/port specs to open socket connections. <br>

---


## Building and running the program | Some basic docker commands
Building the Docker image
(From inside the `root` directory, i.e. wherever your Dockerfile exists)
```
docker build . -t cse224-netsort
```

This step should build an image with a tag of `cse224-netsort:latest`. The docker containers
that run your code will be built using this image. You could check the current docker 
images using 
```
docker image ls
```
Note that there could be an older image that was earlier tagged with `cse224-netsort`
which would, however, now be untagged (with tag value <none>). You can choose to delete it using :
```
docker image rm <IMAGE ID>
```

Once you have the image ready, run the Docker containers using
```
docker-compose up -d
```

This should produce an output such as the following
```
$ docker-compose up -d
Creating starter-code_server3_1 ... done
Creating starter-code_server0_1 ... done
Creating starter-code_server1_1 ... done
Creating starter-code_server2_1 ... done
```

Your containers should be ready/running after this, to check them use
```
docker container ls
```
<br/>

If you don't see any running containers, which would be the case if the `netsort` process
has exited, use the `-a` flag to list all containers
```
docker container ls -a
```

A good way to debug your code in a distributed setting could be using logs. For running/exited containers, use
```
docker logs <CONTAINER_ID | CONTAINER_NAME>
```
to check stdout of your process. For the starter-code you would see the following:
```
$ docker logs starter-code_server0_1                                                                                                                                                                                      ─╯
My server Id: 0
Got the following server configs: {[{0 server0 8080} {1 server1 8080} {2 server2 8080} {3 server3 8080}]}
```

Following commands might turn out to be useful to you, for cleanup of containers etc.

**Stopping a running container**
```
docker stop <CONTAINER_ID | CONTAINER_NAME>
```

**Removing a stopped/exited container**
```
docker rm <CONTAINER_ID | CONTAINER_NAME>
```

---

## Environment requirements and setup
1. Install and setup [Docker desktop](https://docs.docker.com/engine/install/) on your machine.
2. Install and setup [docker-compose](https://docs.docker.com/compose/install/). It comes bundled with `docker-desktop` for `macOS`.  

Make sure that both commands `docker` and `docker-compose` are running on your machine. Essentially, 
the corresponding executable binary paths must have been added to the System Path.
  
---
  
## Verifying your sort implementation
  
There are a few ways to check that your program is correct.  Here are two possibilities, though of course you might think of others.

_Setup_
  
Concatenate all the input files into a file called INPUT

```
$ cp input-0.dat INPUT
$ cat input-1.dat >> INPUT
$ cat input-2.dat >> INPUT
$ cat input-3.dat >> INPUT
```
  
Concatenate all the output files into a file called OUTPUT
  
```
$ cp output-0.dat OUTPUT
$ cat output-1.dat >> OUTPUT
$ cat output-2.dat >> OUTPUT
$ cat output-3.dat >> OUTPUT
```

### Method 1: Using showsort

```
$ bin/showsort INPUT | sort > REF_OUTPUT
$ diff REF_OUTPUT OUTPUT
```


In both cases, you are simply comparing a sort of a concatenation of the input files with a concatenation of the output files.
