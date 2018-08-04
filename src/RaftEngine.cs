using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace NRaft
{

    /**
     * Major TODOS:
     * <ul>
     * <li>Smooth Cluster membership changes</li>
     * <li>More Unit Tests & Robust Simulator
     * <li>Idempotent client requests
     * </ul>
     */

    public class RaftEngine<T> : RaftRequests<T> where T : StateMachine<T>
    {

        public static readonly ILogger logger = LoggerFactory.GetLogger<RaftEngine<T>>();

        /**
         * These are the major raft roles we can be in
         */
        public enum Role
        {
            Joining, Observer, Follower, Candidate, Leader, Failed, Leaving
        }

        public volatile bool DEBUG = false;

        private readonly Random random = new Random();
        private readonly Dictionary<int, PeerState> peers = new Dictionary<int, PeerState>();
        private readonly Queue<PendingCommand<T>> pendingCommands = new Queue<PendingCommand<T>>();
        private readonly Log<T> log;
        private readonly RaftRPC<T> rpc;
        private readonly Config config;

        private Role role = Role.Joining;
        private int myPeerId;
        private long currentTerm;
        private int votedFor;
        private int leaderId;
        private long electionTimeout;
        private long firstIndexOfTerm;
        private long lastTermCommitted = 0;

        public class PeerState
        {
            public readonly int peerId;
            public long lastAppendMillis;
            public long nextIndex = 1;
            public long matchIndex;
            public bool appendPending;
            public bool fresh = true;
            public string snapshotTransfer;

            public PeerState(int peerId)
            {
                this.peerId = peerId;
            }

            public override string ToString()
            {
                return $"Peer-{peerId}";
            }
        }

        public RaftEngine(Config config, T stateMachine, RaftRPC<T> rpc)
        {
            this.rpc = rpc;
            this.config = config;
            stateMachine.addListener(onLogEntryApplied);
            this.log = new Log<T>(config, stateMachine);
            this.lastTermCommitted = this.currentTerm = log.getLastTerm();
        }

        public void start(int peerId)
        {
            lock (this)
            {
                setPeerId(peerId);
                this.role = Role.Follower;
                rescheduleElection();
                //  this.electionTimeout += 10000; // TODO add an initial grace period on startup
                launchPeriodicTasksThread();
            }
        }

        public void startAsObserver()
        {
            lock (this)
            {
                this.role = Role.Observer;
                this.electionTimeout = Int64.MaxValue;
            }
        }

        public void stop()
        {
            lock (this)
            {
                clearAllPendingRequests();
                log.stop();
                foreach (PeerState p in peers.Values)
                {
                    logger.LogInformation($" - {p} has matchIndex {p.matchIndex}");
                    if (role == Role.Leader)
                    {
                        // be nice and send a    update to all the peers with our  current commit index before we shutdown
                        long prevLogIndex = p.nextIndex - 1;
                        long prevLogTerm = log.getTerm(prevLogIndex);
                        rpc.sendAppendEntries(p.peerId, currentTerm, myPeerId, prevLogIndex, prevLogTerm, null, log.getCommitIndex(), null);
                    }
                }
                role = Role.Leaving;
                logger.LogInformation("Raft Stopped");
            }
        }

        public override string ToString()
        {
            return $"Raft[{myPeerId}] {role} (Leader is {leaderId}) ";
        }

        public T getStateMachine()
        {
            lock (this)
            {
                return log.getStateMachine();
            }
        }

        public void setPeerId(int peerId)
        {
            lock (this)
            {
                this.myPeerId = peerId;
                peers.Remove(peerId);
            }
        }

        public int getPeerId()
        {
            return myPeerId;
        }

        public int getClusterSize()
        {
            lock (this)
            {
                return 1 + peers.Count;
            }
        }

        public Role getRole()
        {
            lock (this)
            {
                return role;
            }
        }

        public long getCurrentTerm()
        {
            lock (this)
            {
                return currentTerm;
            }
        }

        public Log<T> getLog()
        {
            return log;
        }

        public int getLeader()
        {
            lock (this)
            {
                return leaderId;
            }
        }

        public bool isValidPeer(int peerId)
        {
            return peers.ContainsKey(peerId);
        }

        private void rescheduleElection()
        {
            lock (this)
            {
                this.electionTimeout = DateTimeOffset.Now.ToUnixTimeMilliseconds() + config.getElectionTimeoutFixedMillis()
                      + random.Next(config.getElectionTimeoutRandomMillis());
            }
        }

        private void launchPeriodicTasksThread()
        {
            var t = Task.Run(async () =>
            {
                while (getRole() != Role.Leaving)
                {
                    try
                    {
                        runPeriodicTasks();
                        await Task.Delay(10);
                    }
                    catch (Exception e)
                    {
                        logger.LogError(e.Message);
                    }
                }
            });
        }

        /**
         * Called periodically to do recurring work
         */
        private void runPeriodicTasks()
        {
            lock (this)
            {
                if (!log.IsRunning && role != Role.Leaving)
                {
                    role = Role.Failed;
                }
                switch (role)
                {
                    case Role.Joining:
                        role = Role.Follower;
                        rescheduleElection();
                        break;
                    case Role.Observer:
                        break;
                    case Role.Follower:
                    case Role.Candidate:
                        if (DateTimeOffset.Now.ToUnixTimeMilliseconds() > electionTimeout)
                        {
                            callElection();
                        }
                        updatePendingRequests();
                        break;
                    case Role.Leader:
                        updateCommitIndex();
                        updatePeers();
                        updatePendingRequests();
                        break;
                    case Role.Failed:
                    case Role.Leaving:
                        break;
                }
            }
        }

        private bool isCommittable(long index)
        {
            lock (this)
            {
                int count = 1;
                int needed = 1 + (1 + peers.Count) / 2;

                foreach (PeerState p in peers.Values)
                {
                    if (p.matchIndex >= index)
                    {
                        count++;
                        if (count >= needed)
                            return true;
                    }
                }
                return count >= needed;
            }
        }

        private void updateCommitIndex()
        {
            lock (this)
            {
                Debug.Assert(role == Role.Leader);
                // we can't commit anything until we've replicated something from this term
                if (isCommittable(firstIndexOfTerm))
                {
                    // we can commit any entry a majority of peers have replicated
                    long index = log.getLastIndex();
                    foreach (PeerState peer in peers.Values)
                    {
                        index = Math.Min(index, peer.matchIndex);
                    }
                    index = Math.Max(index, log.getCommitIndex());
                    while (index <= log.getLastIndex() && isCommittable(index))
                    {

                        // Logging for PT #102932910
                        var e = log.getEntry(index);
                        if (e != null && lastTermCommitted != e.term)
                        {
                            logger.LogInformation($"Committed new term {e.term}");
                            foreach (var kv in peers)
                            {
                                var p = kv.Value;
                                logger.LogInformation($" - {p} has matchIndex {p.matchIndex} >= {firstIndexOfTerm} ({p.matchIndex >= firstIndexOfTerm})");
                            }
                            lastTermCommitted = e.term;
                        }
                        log.setCommitIndex(index);
                        index++;
                    }
                }
            }
        }

        private void callElection()
        {
            lock (this)
            {
                role = Role.Candidate;
                log.updateStateMachine();

                if (role == Role.Leaving)
                {
                    logger.LogError("Can't call election when leaving");
                    return;
                }

                int votesNeeded = (1 + peers.Count) / 2;
                var votes = 1;
                ++currentTerm;
                leaderId = 0;
                votedFor = myPeerId;
                logger.LogInformation($"{this} is calling an election (term {currentTerm})");
                if (peers.Count > 0)
                {
                    foreach (PeerState peer in peers.Values)
                    {
                        peer.nextIndex = 1;
                        peer.matchIndex = 0;
                        rpc.sendRequestVote(config.getClusterName(), peer.peerId, currentTerm, myPeerId, log.getLastIndex(), log.getLastTerm(),
                              (term, voteGranted) =>
                              {
                                  lock (this)
                                  {
                                      if (!stepDown(term))
                                      {
                                          if (term == currentTerm && role == Role.Candidate)
                                          {
                                              if (voteGranted)
                                              {
                                                  votes++;
                                              }
                                              if (votes > votesNeeded)
                                              {
                                                  becomeLeader();
                                              }
                                          }
                                      }
                                  }
                              });
                    }
                }
                else
                {
                    becomeLeader();
                }
                rescheduleElection();
            }
        }

        public void handleVoteRequest(String clusterName, long term, int candidateId, long lastLogIndex, long lastLogTerm,
              VoteResponseHandler handler)
        {
            lock (this)
            {

                if (config.getClusterName() != clusterName || !isValidPeer(candidateId))
                {
                    return;
                }

                if (term > currentTerm)
                {
                    stepDown(term);
                }
                if (term >= currentTerm && (votedFor == 0 || votedFor == candidateId) && lastLogIndex >= log.getLastIndex()
                      && lastLogTerm >= log.getLastTerm())
                {
                    votedFor = candidateId;
                    rescheduleElection();

                    logger.LogInformation($"{this} I'm voting YES for {candidateId} (term {currentTerm})");
                    handler(currentTerm, true);
                }
                else
                {
                    logger.LogInformation($"{this} I'm voting NO for {candidateId} (term {currentTerm})");
                    handler(currentTerm, false);
                }
            }
        }

        private bool stepDown(long term)
        {
            lock (this)
            {
                if (term > currentTerm)
                {
                    currentTerm = term;
                    votedFor = 0;
                    if (role == Role.Candidate || role == Role.Leader)
                    {
                        logger.LogInformation($"{this} is stepping down to follower (term {currentTerm})");
                        role = Role.Follower;
                        clearAllPendingRequests();
                    }
                    rescheduleElection();
                    return true;
                }
                return false;
            }
        }

        public void becomeLeader()
        {
            lock (this)
            {

                logger.LogInformation($"{this} is becoming the leader (term {currentTerm})");
                role = Role.Leader;
                leaderId = myPeerId;
                firstIndexOfTerm = log.getLastIndex() + 1;
                foreach (PeerState peer in peers.Values)
                {
                    peer.matchIndex = 0;
                    peer.nextIndex = log.getLastIndex() + 1;
                    peer.appendPending = false;
                    peer.snapshotTransfer = null;
                    peer.fresh = true;
                    Debug.Assert(peer.nextIndex != 0);
                }

                // Force a new term command to mark the occasion and hasten
                // commitment of any older entries in our log from the 
                // previous term
                executeCommand(new NewTermCommand<T>(myPeerId, currentTerm), null);

                updatePeers();
            }
        }

        /**
         * As leader, we need to make sure we continually keep our peers up to date, and when no entries are made, to send a heart beat so that
         * they do not call an election
         */
        private void updatePeers()
        {
            lock (this)
            {
                Debug.Assert(role == Role.Leader);
                foreach (var peer in peers.Values) updatePeer(peer);
            }
        }

        private void updatePeer(PeerState peer)
        {
            lock (this)
            {
                long now = DateTimeOffset.Now.ToUnixTimeMilliseconds();
                if (peer.appendPending && now > peer.lastAppendMillis + 5000)
                {
                    peer.appendPending = false; // time out the last append
                }
                if (!peer.appendPending && (peer.nextIndex < log.getLastIndex() || now > peer.lastAppendMillis + config.getHeartbeatMillis()))
                {
                    Debug.Assert(peer.nextIndex > 0);

                    // for a fresh peer we'll start with an empty list of entries so we can learn what index the node is already on in it's log
                    // fetch entries from log to send to the peer
                    Entry<T>[] entries = (!peer.fresh && peer.snapshotTransfer == null)
                      ? log.getEntries(peer.nextIndex, config.getMaxEntriesPerRequest()) : null;

                    // if this peer needs entries we no longer have, then send them a snapshot
                    if (!peer.fresh && peer.nextIndex > 0 && peer.nextIndex < log.getFirstIndex() && entries == null)
                    {
                        logger.LogInformation($"{this} peer {peer.peerId} needs a snapshot");
                        installSnapshot(peer);
                    }
                    else
                    {
                        long prevLogIndex = peer.nextIndex - 1;
                        long prevLogTerm = log.getTerm(prevLogIndex);

                        // debugging a weird situation:
                        if ((entries == null || entries.Length == 0) && peer.nextIndex < log.getCommitIndex())
                        {
                            logger.LogWarning($"Empty entries for peer {peer} (fresh={peer.fresh},snap={peer.snapshotTransfer}): peerIndex={peer.nextIndex}, firstIndex={log.getFirstIndex()}, lastIndex={log.getLastIndex()} : {entries}");
                            var e = log.getEntry(peer.nextIndex);
                            logger.LogWarning($"{peer.nextIndex} = {e}");
                        }

                        logger.LogTrace($"{this} is sending append entries to {peer.peerId}");
                        peer.lastAppendMillis = now;
                        peer.appendPending = true;
                        peer.snapshotTransfer = null;
                        rpc.sendAppendEntries(peer.peerId, currentTerm, myPeerId, prevLogIndex, prevLogTerm, entries, log.getCommitIndex(),
                              (term, success, lastLogIndex) =>
                              {
                                  lock (this)
                                  {
                                      peer.appendPending = false;
                                      if (role == Role.Leader)
                                      {
                                          if (!stepDown(term))
                                          {
                                              peer.fresh = false;
                                              if (success)
                                              {
                                                  if (entries != null)
                                                  {
                                                      peer.matchIndex = entries[entries.Length - 1].index;
                                                      peer.nextIndex = peer.matchIndex + 1;
                                                      Debug.Assert(peer.nextIndex != 0);
                                                  }
                                                  else
                                                  {
                                                      peer.nextIndex = Math.Max(lastLogIndex + 1, 1);
                                                  }
                                                  updatePeer(peer);
                                              }
                                              else
                                              {
                                                  //Debug.Assert(peer.nextIndex > 1 : "peer.nextIndex = " + peer.nextIndex;
                                                  if (peer.nextIndex > lastLogIndex)
                                                  {
                                                      peer.nextIndex = Math.Max(lastLogIndex + 1, 1);
                                                  }
                                                  else if (peer.nextIndex > 1)
                                                  {
                                                      peer.nextIndex--;
                                                  }
                                              }
                                          }
                                      }
                                  }
                              });
                    }
                }
            }
        }

        public void handleAppendEntriesRequest(long term, int leaderId, long prevLogIndex, long prevLogTerm, Entry<T>[] entries,
             long leaderCommit, AppendEntriesResponseHandler handler)
        {
            lock (this)
            {
                if (!log.IsRunning)
                {
                    return;
                }

                logger.LogTrace($"{this} append entries from {leaderId}: from <{prevLogTerm}:{prevLogIndex}>");
                if (term >= currentTerm)
                {
                    bool rescheduled = false;
                    if (term > currentTerm || this.leaderId != leaderId)
                    {
                        this.leaderId = leaderId;
                        rescheduled = stepDown(term);
                        role = Role.Follower;
                    }
                    
                    if(!rescheduled)
                        rescheduleElection();

                    if (log.isConsistentWith(prevLogIndex, prevLogTerm))
                    {
                        if (entries != null)
                        {
                            foreach (Entry<T> e in entries)
                            {
                                if (!log.append(e))
                                {
                                    logger.LogWarning($"{this} is failing append entries from {leaderId}: {e}");
                                    handler(currentTerm, false, log.getLastIndex());
                                    return;
                                }
                            }
                        }

                        log.setCommitIndex(Math.Min(leaderCommit, log.getLastIndex()));

                        logger.LogTrace($"{this} is fine with append entries from {leaderId}");
                        handler(currentTerm, true, log.getLastIndex());
                        return;
                    }
                    else
                    {
                        logger.LogWarning($"{this} is failing with inconsistent append entries from {leaderId}");
                        logger.LogWarning($"Leader prevLogTerm={prevLogTerm}, prevLogIndex={prevLogIndex}");
                        logger.LogWarning($"Follower firstTerm={log.getFirstTerm()}, firstIndex={log.getFirstIndex()}");
                        logger.LogWarning($"Follower lastTerm={log.getLastTerm()}, lastIndex={log.getLastIndex()}");

                        if (prevLogIndex > log.getCommitIndex())
                        {
                            log.wipeConflictedEntries(prevLogIndex);
                        }
                        else
                        {
                            stop();
                        }

                    }
                }

                logger.LogTrace($"{this} is rejecting append entries from {leaderId}");
                handler(currentTerm, false, log.getLastIndex());
            }
        }

        private void installSnapshot(PeerState peer)
        {
            lock (this)
            {

                if (peer.snapshotTransfer == null)
                {
                    peer.snapshotTransfer = Path.Combine(log.getLogDirectory(), "raft.snapshot");
                    installSnapshot(peer, 0);
                }
            }
        }

        private void installSnapshot(PeerState peer, int part)
        {
            lock (this)
            {

                // we don't have log entries this old on record, so we need to send them a viable snapshot instead
                // we need to be able to send this snapshot before we delete log entries after the snapshot, or
                // we won't be able to catch them up. We also need to make sure we don't delete the snapshot file
                // we're sending. 
                if (peer.snapshotTransfer != null)
                {
                    logger.LogInformation($"Installing Snapshot to {peer} Part #{part}");
                    long snapshotIndex = StateMachine.getSnapshotIndex(peer.snapshotTransfer);
                    if (snapshotIndex > 0)
                    {
                        int partSize = config.getSnapshotPartSize();
                        long len = peer.snapshotTransfer.Length;
                        var data = RaftUtil.getFilePart(peer.snapshotTransfer, part * partSize,
                         (int)Math.Min(partSize, len - part * partSize));

                        rpc.sendInstallSnapshot(peer.peerId, currentTerm, myPeerId, len, partSize, part, data,

                      (bool success) =>
                        {
                            lock (this)
                            {
                                if (success)
                                {
                                    if ((part + 1) * partSize < len)
                                    {
                                        // send the next part
                                        installSnapshot(peer, part + 1);
                                    }
                                    else
                                    {
                                        logger.LogInformation($"InstallSnapshot done {peer.nextIndex} => {snapshotIndex} for {peer}");
                                        peer.snapshotTransfer = null;
                                        peer.nextIndex = snapshotIndex + 1;
                                    }
                                }
                                else
                                {
                                    logger.LogError($"{this} Failed to install snapshot on {peer}");
                                    peer.snapshotTransfer = null;
                                }
                            }
                        });
                    }
                }
            }
        }

        public void handleInstallSnapshotRequest(long term, long index, long length, int partSize, int part, byte[] data,
                 InstallSnapshotResponseHandler handler)
        {
            logger.LogInformation($"handleInstallSnapshot: length={length} part={part}");
            rescheduleElection();

            var file = new FileInfo(Path.Combine(log.getLogDirectory(), "raft.installing.snapshot"));
            if (file.Exists && part == 0)
            {
                file.Delete();
            }

            if (part == 0 || file.Exists)
            {
                if (file.Length == partSize * part)
                {
                    try
                    {
                        using (var stream = file.OpenWrite())
                        {
                            using (var raf = new BinaryWriter(stream))
                            {
                                raf.Seek(partSize * part, SeekOrigin.Begin);
                                raf.Write(data);

                                if (stream.Length == length)
                                {
                                    File.Move(file.FullName, Path.Combine(log.getLogDirectory(), "raft.snapshot"));
                                    log.loadSnapshot();
                                    rescheduleElection();
                                }

                                handler(true);
                                return;
                            }
                        }
                    }
                    catch (IOException e)
                    {
                        logger.LogError(e.Message);
                    }
                }
            }
            handler(false);
        }

        public void handleClientRequest(Command<T> command, ClientResponseHandler<T> handler)
        {
            lock (this)
            {
                executeCommand(command, handler);
            }
        }

        public bool executeCommand(Command<T> command, ClientResponseHandler<T> handler = null)
        {
            lock (this)
            {
                if (role == Role.Leader)
                {
                    logger.LogInformation($"{this} is executing a command");
                    Entry<T> e = log.append(currentTerm, command);
                    if (e != null)
                    {
                        if (handler != null)
                        {
                            lock (pendingCommands)
                            {
                                pendingCommands.Enqueue(new PendingCommand<T>(e, handler));
                            }
                        }
                        return true;
                    }
                    if (handler != null)
                    {
                        handler(null);
                    }
                }
                return false;
            }
        }

        public void executeAfterCommandProcessed(Entry<T> e, ClientResponseHandler<T> handler)
        {
            if (e.index <= log.getStateMachineIndex())
            {
                handler(e);
            }
            else
            {
                lock (pendingCommands)
                {
                    pendingCommands.Enqueue(new PendingCommand<T>(e, handler));
                }
            }
        }

        /**
         * Pop all the pending command requests from our list that are now safely replicated to the majority and applied to our state machine
         */
        private void updatePendingRequests()
        {

            lock (pendingCommands)
            {
                while (pendingCommands.Count > 0)
                {
                    //  logger.LogInformation("Updating All Pending Requests {} > {} ", pendingCommands.Count, log.getCommitIndex());
                    PendingCommand<T> item = pendingCommands.Dequeue();
                    if (item.entry.index <= log.getStateMachineIndex())
                    {
                        logger.LogDebug($"Returning Pending Command Response To Client {item.entry}");
                        item.handler(item.entry);
                    }
                    else
                    {
                        pendingCommands.Enqueue(item); // ????
                        return;
                    }
                }
            }
        }

        private void clearAllPendingRequests()
        {
            lock (this)
            {
                logger.LogInformation("Clearing All Pending Requests");
                lock (pendingCommands)
                {
                    foreach (var item in pendingCommands)
                        item.handler(null);
                    pendingCommands.Clear();
                }
            }
        }

        public void addPeer(int peerId)
        {
            lock (this)
            {
                if (peerId != 0 && peerId != this.myPeerId)
                {
                    peers.Add(peerId, new PeerState(peerId));
                }
            }
        }

        public void onLogEntryApplied(Entry<T> entry)
        {
            //       Command<T> command = entry.getCommand();
            //      if (command.getCommandType() == StateMachine.COMMAND_ID_ADD_PEER) {
            //             AddPeerCommand<T> addPeerCommand = ((AddPeerCommand<T>) command);
            //         if (addPeerCommand.bootstrap) {
            //            logger.LogInformation(" ********************** BOOTSTRAP **********************", addPeerCommand.peerId);
            //            peers.clear();
            //         }
            //         if (addPeerCommand.peerId != this.myPeerId) {
            //            logger.LogInformation(" ********************** AddPeer #{} ********************** ", addPeerCommand.peerId);
            //            peers.put(addPeerCommand.peerId, new Peer(addPeerCommand.peerId));
            //         }
            //      } else if (command.getCommandType() == StateMachine.COMMAND_ID_DEL_PEER) {
            //             DelPeerCommand<T> delPeerCommand = ((DelPeerCommand<T>) command);
            //         logger.LogInformation(" ********************** DelPeer #{} ********************** ", delPeerCommand.peerId);
            //         peers.remove(delPeerCommand.peerId);
            //      }
        }

        public IEnumerable<PeerState> getPeers()
        {
            return peers.Values;
        }
    }
}

