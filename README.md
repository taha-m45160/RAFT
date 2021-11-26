# RAFT Distributed Consensus Algorithm
Implemented RAFT as described in the paper: 

<b>Diego Ongaro and John Ousterhout. 2014. 
Ongaro, D. & Ousterhout, J.. (2014). 
In search of an understandable consensus algorithm. 
USENIX. 305-320.</b>

<ul>
<li>Leader Election</li>
<li>Log Replication</li>
<li>Fault Tolerance with Persistent Storage</li>
</ul>

<h3>Abstract</h3>
<p>Raft is a consensus algorithm for managing a replicated
log. It produces a result equivalent to (multi-)Paxos, and
it is as efficient as Paxos, but its structure is different
from Paxos; this makes Raft more understandable than
Paxos and also provides a better foundation for building
practical systems. In order to enhance understandability,
Raft separates the key elements of consensus, such as
leader election, log replication, and safety, and it enforces
a stronger degree of coherency to reduce the number of
states that must be considered. Results from a user study
demonstrate that Raft is easier for students to learn than
Paxos. Raft also includes a new mechanism for changing
the cluster membership, which uses overlapping majori-
ties to guarantee safety.</p>

<u>The paper can be found at the following link:
https://raft.github.io/raft.pdf</u>
