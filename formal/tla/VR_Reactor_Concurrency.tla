----------------------- MODULE VR_Reactor_Concurrency -----------------------
(***************************************************************************)
(* VoidRush Epoll Reactor Concurrency & Fault Recovery Specification       *)
(*                                                                         *)
(* Models:                                                                 *)
(* 1. Acceptor & least-connections worker assignment                       *)
(* 2. Linux epoll wait with EPOLLET + EPOLLONESHOT arming semantics        *)
(* 3. Generational FD reuse (ABA event misrouting protection)              *)
(* 4. SCOOP worker fault reporting & supervisor purging                    *)
(* 5. Reference-counted broadcast message life-cycle                       *)
(* 6. Eventfd reactor wakeup and graceful shutdown                         *)
(***************************************************************************)

EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS
  MAX_FDS,          \* Bound on file descriptors for TLC (e.g. 3)
  MAX_WORKERS,      \* Bound on worker count for TLC (e.g. 2)
  MAX_GEN           \* Bound on generation counter for TLC (e.g. 3)

FDs == 0..(MAX_FDS - 1)
Workers == 0..(MAX_WORKERS - 1)
NoOwner == -1

VARIABLES
  \* --- Reactor & OS Kernel State ---
  shutdown,         \* BOOLEAN: r->shutdown_flag
  wakePending,      \* BOOLEAN: eventfd counter > 0
  reactorExited,    \* BOOLEAN: reactor wait loop terminated
  osFdOpen,         \* [FDs -> BOOLEAN]: kernel FD open status
  epollArmed,       \* [FDs -> BOOLEAN]: EPOLLONESHOT state (TRUE = armed in epoll)
  epollGen,         \* [FDs -> 0..MAX_GEN]: generation in ev.data.u64

  \* --- In-Flight Event Buffer ---
  inFlightEvents,   \* SUBSET (FDs \X (0..MAX_GEN))

  \* --- Shared Atomic Control Block ---
  fdOwner,          \* [FDs -> Workers \cup {NoOwner}]: r->fd_owner
  workerLoad,       \* [Workers -> Nat]: r->worker_conns
  faultMask,        \* SUBSET Workers: r->child_fault_mask

  \* --- Worker Local State ---
  workerGen,        \* [Workers -> [FDs -> 0..MAX_GEN]]: expected generation
  msgRefCount,      \* Nat: ref_count of broadcast message
  msgFreed,         \* BOOLEAN: whether broadcast buffer is freed
  workerHasMsgRef   \* [Workers -> BOOLEAN]: reference held by worker

vars == <<shutdown, wakePending, reactorExited, osFdOpen, epollArmed, epollGen,
          inFlightEvents, fdOwner, workerLoad, faultMask, workerGen,
          msgRefCount, msgFreed, workerHasMsgRef>>

-----------------------------------------------------------------------------
\* TYPE INVARIANT
TypeOK ==
  /\ shutdown \in BOOLEAN
  /\ wakePending \in BOOLEAN
  /\ reactorExited \in BOOLEAN
  /\ osFdOpen \in [FDs -> BOOLEAN]
  /\ epollArmed \in [FDs -> BOOLEAN]
  /\ epollGen \in [FDs -> 0..MAX_GEN]
  /\ inFlightEvents \subseteq (FDs \X (0..MAX_GEN))
  /\ fdOwner \in [FDs -> Workers \cup {NoOwner}]
  /\ workerLoad \in [Workers -> 0..MAX_FDS]
  /\ faultMask \subseteq Workers
  /\ workerGen \in [Workers -> [FDs -> 0..MAX_GEN]]
  /\ msgRefCount \in 0..MAX_WORKERS
  /\ msgFreed \in BOOLEAN
  /\ workerHasMsgRef \in [Workers -> BOOLEAN]

Init ==
  /\ shutdown = FALSE
  /\ wakePending = FALSE
  /\ reactorExited = FALSE
  /\ osFdOpen = [fd \in FDs |-> FALSE]
  /\ epollArmed = [fd \in FDs |-> FALSE]
  /\ epollGen = [fd \in FDs |-> 0]
  /\ inFlightEvents = {}
  /\ fdOwner = [fd \in FDs |-> NoOwner]
  /\ workerLoad = [w \in Workers |-> 0]
  /\ faultMask = {}
  /\ workerGen = [w \in Workers |-> [fd \in FDs |-> 0]]
  /\ msgRefCount = 0
  /\ msgFreed = TRUE
  /\ workerHasMsgRef = [w \in Workers |-> FALSE]

-----------------------------------------------------------------------------
\* 1. ACCEPT & LEAST-CONNECTIONS WORKER ASSIGNMENT
\* Note: Skips workers in faultMask to avoid assigning traffic to faulted shards
AcceptAndAssign(fd) ==
  /\ ~shutdown
  /\ ~osFdOpen[fd]
  /\ fdOwner[fd] = NoOwner
  /\ epollGen[fd] < MAX_GEN
  /\ LET healthyWorkers == Workers \ faultMask
     IN
       /\ healthyWorkers # {}
       /\ LET best == CHOOSE w \in healthyWorkers :
                        \A u \in healthyWorkers :
                          \/ workerLoad[w] < workerLoad[u]
                          \/ (workerLoad[w] = workerLoad[u] /\ w <= u)
              nextGen == epollGen[fd] + 1
          IN
            /\ osFdOpen' = [osFdOpen EXCEPT ![fd] = TRUE]
            /\ fdOwner' = [fdOwner EXCEPT ![fd] = best]
            /\ workerLoad' = [workerLoad EXCEPT ![best] = workerLoad[best] + 1]
            /\ epollGen' = [epollGen EXCEPT ![fd] = nextGen]
            /\ epollArmed' = [epollArmed EXCEPT ![fd] = TRUE]
            /\ workerGen' = [workerGen EXCEPT ![best][fd] = nextGen]
            /\ UNCHANGED <<shutdown, wakePending, reactorExited, inFlightEvents,
                           faultMask, msgRefCount, msgFreed, workerHasMsgRef>>

\* 2. EPOLL WAIT (Fires on armed FD, disarms via EPOLLONESHOT)
EpollWaitEvent(fd) ==
  /\ ~reactorExited
  /\ osFdOpen[fd]
  /\ epollArmed[fd]
  /\ epollArmed' = [epollArmed EXCEPT ![fd] = FALSE]
  /\ inFlightEvents' = inFlightEvents \cup {<<fd, epollGen[fd]>>}
  /\ UNCHANGED <<shutdown, wakePending, reactorExited, osFdOpen, epollGen,
                 fdOwner, workerLoad, faultMask, workerGen,
                 msgRefCount, msgFreed, workerHasMsgRef>>

\* 3. WORKER HANDLES EVENT & RE-ARMS (vr_reactor_route_fd + vr_reactor_mod)
WorkerHandleAndRearm(fd, gen) ==
  /\ <<fd, gen>> \in inFlightEvents
  /\ LET w == fdOwner[fd]
     IN
       /\ inFlightEvents' = inFlightEvents \ {<<fd, gen>>}
       /\ IF w # NoOwner /\ ~(w \in faultMask) /\ workerGen[w][fd] = gen /\ osFdOpen[fd]
          THEN epollArmed' = [epollArmed EXCEPT ![fd] = TRUE]
          ELSE UNCHANGED epollArmed
       /\ UNCHANGED <<shutdown, wakePending, reactorExited, osFdOpen, epollGen,
                      fdOwner, workerLoad, faultMask, workerGen,
                      msgRefCount, msgFreed, workerHasMsgRef>>

\* 4. WORKER CLOSES FD CLEANLY (vr_reactor_close_fd_for_worker)
\* Atomic teardown: ownership, load decrement, and socket close unified
WorkerCloseFd(w, fd) ==
  /\ ~(w \in faultMask)
  /\ fdOwner[fd] = w
  /\ fdOwner' = [fdOwner EXCEPT ![fd] = NoOwner]
  /\ workerLoad' = [workerLoad EXCEPT ![w] = IF workerLoad[w] > 0 THEN workerLoad[w] - 1 ELSE 0]
  /\ epollArmed' = [epollArmed EXCEPT ![fd] = FALSE]
  /\ osFdOpen' = [osFdOpen EXCEPT ![fd] = FALSE]
  /\ UNCHANGED <<shutdown, wakePending, reactorExited, epollGen, inFlightEvents,
                 faultMask, workerGen, msgRefCount, msgFreed, workerHasMsgRef>>

\* 5. WORKER FAULT & SUPERVISOR PURGE
WorkerFault(w) ==
  /\ ~(w \in faultMask)
  /\ faultMask' = faultMask \cup {w}
  /\ UNCHANGED <<shutdown, wakePending, reactorExited, osFdOpen, epollArmed,
                 epollGen, inFlightEvents, fdOwner, workerLoad, workerGen,
                 msgRefCount, msgFreed, workerHasMsgRef>>

\* Purge cleans up owned FDs AND releases any pending broadcast message ref
SupervisorPurgeWorker(w) ==
  /\ w \in faultMask
  /\ LET ownedFds == {fd \in FDs : fdOwner[fd] = w}
         hadMsgRef == workerHasMsgRef[w]
         newRefCnt == IF hadMsgRef THEN msgRefCount - 1 ELSE msgRefCount
     IN
       /\ fdOwner' = [fd \in FDs |-> IF fd \in ownedFds THEN NoOwner ELSE fdOwner[fd]]
       /\ osFdOpen' = [fd \in FDs |-> IF fd \in ownedFds THEN FALSE ELSE osFdOpen[fd]]
       /\ epollArmed' = [fd \in FDs |-> IF fd \in ownedFds THEN FALSE ELSE epollArmed[fd]]
       /\ workerLoad' = [workerLoad EXCEPT ![w] = 0]
       /\ faultMask' = faultMask \ {w}
       /\ workerHasMsgRef' = [workerHasMsgRef EXCEPT ![w] = FALSE]
       /\ msgRefCount' = newRefCnt
       /\ msgFreed' = (newRefCnt = 0)
       /\ UNCHANGED <<shutdown, wakePending, reactorExited, epollGen, inFlightEvents,
                      workerGen>>

\* 6. SHUTDOWN & EVENTFD WAKEUP
RequestShutdown ==
  /\ ~shutdown
  /\ shutdown' = TRUE
  /\ wakePending' = TRUE
  /\ UNCHANGED <<reactorExited, osFdOpen, epollArmed, epollGen, inFlightEvents,
                 fdOwner, workerLoad, faultMask, workerGen,
                 msgRefCount, msgFreed, workerHasMsgRef>>

ReactorConsumeWakeAndExit ==
  /\ wakePending
  /\ wakePending' = FALSE
  /\ reactorExited' = IF shutdown THEN TRUE ELSE reactorExited
  /\ UNCHANGED <<shutdown, osFdOpen, epollArmed, epollGen, inFlightEvents,
                 fdOwner, workerLoad, faultMask, workerGen,
                 msgRefCount, msgFreed, workerHasMsgRef>>

\* 7. REF-COUNTED BROADCAST MESSAGE (vr_ws_msg_create / vr_ws_msg_unref)
BrokerBroadcastCreate(targetWorkers) ==
  /\ targetWorkers \subseteq (Workers \ faultMask)
  /\ targetWorkers # {}
  /\ msgFreed = TRUE
  /\ msgRefCount' = Cardinality(targetWorkers)
  /\ msgFreed' = FALSE
  /\ workerHasMsgRef' = [w \in Workers |-> w \in targetWorkers]
  /\ UNCHANGED <<shutdown, wakePending, reactorExited, osFdOpen, epollArmed,
                 epollGen, inFlightEvents, fdOwner, workerLoad, faultMask, workerGen>>

WorkerUnrefMsg(w) ==
  /\ ~(w \in faultMask)
  /\ workerHasMsgRef[w]
  /\ workerHasMsgRef' = [workerHasMsgRef EXCEPT ![w] = FALSE]
  /\ msgRefCount' = msgRefCount - 1
  /\ msgFreed' = (msgRefCount - 1 = 0)
  /\ UNCHANGED <<shutdown, wakePending, reactorExited, osFdOpen, epollArmed,
                 epollGen, inFlightEvents, fdOwner, workerLoad, faultMask, workerGen>>

-----------------------------------------------------------------------------
Next ==
  \/ \E fd \in FDs : AcceptAndAssign(fd)
  \/ \E fd \in FDs : EpollWaitEvent(fd)
  \/ \E ev \in inFlightEvents : WorkerHandleAndRearm(ev[1], ev[2])
  \/ \E w \in Workers, fd \in FDs : WorkerCloseFd(w, fd)
  \/ \E w \in Workers : WorkerFault(w)
  \/ \E w \in Workers : SupervisorPurgeWorker(w)
  \/ RequestShutdown
  \/ ReactorConsumeWakeAndExit
  \/ \E sub \in SUBSET Workers : BrokerBroadcastCreate(sub)
  \/ \E w \in Workers : WorkerUnrefMsg(w)

\* Sub-action weak fairness prevents starvation cycles during liveness checking
Spec ==
  /\ Init
  /\ [][Next]_vars
  /\ WF_vars(Next)
  /\ WF_vars(ReactorConsumeWakeAndExit)
  /\ \A w \in Workers : WF_vars(SupervisorPurgeWorker(w))
  /\ \A w \in Workers : WF_vars(WorkerUnrefMsg(w))

-----------------------------------------------------------------------------
\* SAFETY INVARIANTS

\* 1. Any owned FD must be open in the kernel
OwnedFdIsOpen ==
  \A fd \in FDs : fdOwner[fd] # NoOwner => osFdOpen[fd]

\* 2. Armed epoll FDs must be open and owned
ArmedFdIsValid ==
  \A fd \in FDs : epollArmed[fd] => (osFdOpen[fd] /\ fdOwner[fd] # NoOwner)

\* 3. Worker load exactly matches active owned FD cardinality
OwnerConsistentWithLoads ==
  \A w \in Workers :
    workerLoad[w] = Cardinality({fd \in FDs : fdOwner[fd] = w})

\* 4. Shutdown guarantees pending wakeup or reactor loop termination
ShutdownSafety ==
  shutdown => (wakePending \/ reactorExited)

\* 5. Memory safety: no leak or use-after-free on ref-counted messages
RefCountSafety ==
  /\ (msgFreed <=> (msgRefCount = 0))
  /\ msgRefCount = Cardinality({w \in Workers : workerHasMsgRef[w]})

\* 6. ABA Safety: in-flight events only routed if generation matches open FD
NoABAMisrouting ==
  \A ev \in inFlightEvents :
    LET fd == ev[1]
        gen == ev[2]
        w == fdOwner[fd]
    IN (w # NoOwner /\ workerGen[w][fd] = gen) => (osFdOpen[fd] /\ epollGen[fd] = gen)

Safety ==
  /\ TypeOK
  /\ OwnedFdIsOpen
  /\ ArmedFdIsValid
  /\ OwnerConsistentWithLoads
  /\ ShutdownSafety
  /\ RefCountSafety
  /\ NoABAMisrouting

-----------------------------------------------------------------------------
\* LIVENESS PROPERTIES

ShutdownEventuallyExits ==
  shutdown ~> reactorExited

FaultsEventuallyPurged ==
  \A w \in Workers : (w \in faultMask) ~> ~(w \in faultMask)

BroadcastMsgEventuallyFreed ==
  (~msgFreed) ~> msgFreed

=============================================================================
