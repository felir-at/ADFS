#Raft terms
 - **committed**: An entry is considered committed if it is safe for that entry to be applied to state machines (~ it's replicated enough times to followers)
 - **applied**: applyed to the underlying state machine
 - **next index**: the index of the next entry to send to a given follower
 - **match index**: the index of the last entry to which the log entries are the same for  a given follower

#Notes from mailing list:

Where to store lastApplied?
> Second, lastApplied should be as durable as the state machine. If the state machine lives in memory, then lastApplied should reset to 0 on a restart. If the state machine lives on disk, then lastApplied should also be stored on disk.


What to do, when responses are lost for client requests, and client repeats it request:
> As you observe, you have to deal with this kind of issue (lost acks) in pretty much all distributed systems; it's not really specific to Raft. Check out section 6.3 "Implementing Linearizable Semantics" in my thesis for some ideas: https://ramcloud.stanford.edu/~ongaro/thesis.pdf

#My questions about raft

####Should we maintain to separate logs for cluster operation (`Join`, `Leave`, etc) and client operations?

> First, lastApplied must increase by 1 each time an entry is applied so that it reflects the state of the state machine.

I ask this, because it would be intuitive to use the log index, but it's not necessarily continuous. But by separating the log into two different lists the client operation log indexes rest continuous, but we would need to maintain to different set of indexes.

####Can the leader set the commitIndex as lastLogIndex upon election?

You say that
> [A newly elected leader will advance its commitIndex value once it
commits its first entry. (Was that a circular definition? I mean once
it replicates the first entry having its current term to a majority of
the cluster.)](https://groups.google.com/forum/#!topic/raft-dev/KIozjYuq5m0)

But when a leader get elected, it also means that the log entries match up until leader's lastLogIndex (because of the election safety property).
