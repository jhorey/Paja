# Paja
Paja is an implementation of Paxos for Java. The goal is to implement a "complete" version of
Paxos. This means:

* The implementation should support classic, fast, and general modes
* The implementation should support a simple user-facing API
* The implementation should support a modular communication back-end

# Brief Paxos Overview
Paxos is a distributed, fault-tolerant consensus protocol. The basic idea is that you need multiple
"replicas" to all agree upon a certain order of events even if each replica receives individual events
in a different order (due to concurrent client submissions). In addition, paxos is designed to ensure 
that all replicas agree upon a global order even if a certain number of nodes randomly fail and restart. 

The Paxos algorithm is usually described by a set of functional roles. In our implementation of classic Paxos, "Clients" propose
values to a "proposer". The proposer then sends the proposal to a set of "acceptors". When a quorum of acceptors agree upon the
proposal, the proposal is accepted. 

* Replica: responsible for interacting with the actual state machine
* Proposer: responsible for proposing and accepting new values
* Acceptor: responsible for voting for new values

In "classic" mode, clients usually interact with a single leader proposer. To ensure safety, proposer will wait for a 2/3 majority
before accepting a value. In "fast" mode, clients contact acceptors directly (thus bypassing the proposer). It does this by first
activating a "fast window" that defines how many actions will come directly from clients. This may create "conflicting" proposals,
so to To ensure safety, the proposer will wait for a 3/4 majority before accepting the value in fast mode. Finally, in "general" mode
conflicting proposals are tested to see if they actually conflict (since some values can be applied to the state machine in arbitrary
order). 

# API & Usage
Although Paxos is defined in terms of acceptors, proposers, etc. most implementations (including this one) groups together
a single replica, proposer, and acceptor into a single process ('PaxosNode.java'). Each paxos node is started independently
and listens on a separate port. 

~~~
node = new PaxoNode(configFile);
node.setStateMachine(new MyStateMachineFactory());
node.start();
~~~

Clients interact with paxos nodes via 'PaxosGroup'.
~~~
byte[] myData = generateData();

group = new PaxosGroup(configFile);
group.connect();
group.propose(myData);
~~~

Paja also includes a *diagnoser* ('PaxosDiagnoser.java') that lets users perform diagnosis on paxos nodes. 
The benchmark application (located in 'benchmark/') demonstrates the usage of both the server and client processes. 

The easiest way to test and run Paja is to run the following commands (in the 'benchmark' directory) in two 
separate consoles (one starts three servers, the other starts three concurrent clients). 
~~~
ant servers -Dconfig=config/servers
ant clients -Dconfig=config/client/client0.xml -Dnum=3
~~~

# Current Status & Future Work
This code is in an active state of development and is intended primarily for research purposes. Besides more thoroughly
testing correctness, I intend on improving:

* Performance
* Better fast mode window setting (fast windows are currently set by user applications)

# Copyright and License
This code was originally developed by James Horey while an employee at Oak Ridge National Lab. Consequently
the copyright belongs to Oak Ridge National Laboratory. The code is released under an 
Apache 2.0 license. If you do use this application, please let me know and share your experiences. 