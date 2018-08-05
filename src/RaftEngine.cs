using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
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

    public class RaftEngine : RaftRequests 
    {

        public static readonly ILogger logger = LoggerFactory.GetLogger<RaftEngine>();

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
        private readonly Queue<PendingCommand> pendingCommands = new Queue<PendingCommand>();
        internal readonly Log Log;
        private readonly RaftRPC rpc;
        private readonly Config config;

        private Role role = Role.Joining;
        private int myPeerId;
        private long currentTerm;
        private int votedFor;
        private int leaderId;
        private long electionTimeout;
        private long firstIndexOfTerm;
        private long lastTermCommitted = 0;
        private IStateMachine stateMachineManager;
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

        public RaftEngine(Config config, IStateMachine stateMachineManager, RaftRPC rpc)
        {
            this.stateMachineManager = stateMachineManager;
            this.rpc = rpc;
            this.config = config;
            var stateMachine = new StateManager(stateMachineManager);
            stateMachine.AddListener(OnLogEntryApplied);
            this.Log = new Log(config, stateMachine);
            this.lastTermCommitted = this.currentTerm = Log.LastTerm;
            SetPeerId(config.PeerId);
        }

        public void Start()
        {
            lock (this)
            {
                this.role = Role.Follower;
                RescheduleElection();
                this.electionTimeout += 1000; // TODO add an initial grace period on startup
                LaunchPeriodicTasksThread();
            }
        }

        public void StartAsObserver()
        {
            lock (this)
            {
                this.role = Role.Observer;
                this.electionTimeout = Int64.MaxValue;
            }
        }

        public void Stop()
        {
            lock (this)
            {
                ClearAllPendingRequests();
                Log.Stop();
                foreach (PeerState p in peers.Values)
                {
                    logger.LogInformation($" - {p} has matchIndex {p.matchIndex}");
                    if (role == Role.Leader)
                    {
                        // be nice and send a update to all the peers with our  current commit index before we shutdown
                        long prevLogIndex = p.nextIndex - 1;
                        long prevLogTerm = Log.GetTerm(prevLogIndex);
                        rpc.SendAppendEntries(p.peerId, currentTerm, myPeerId, prevLogIndex, prevLogTerm, null, Log.CommitIndex, null);
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

        public T GetStateMachineManager<T>() where T: IStateMachine
        {
                return (T)stateMachineManager;
            }

        public void SetPeerId(int peerId)
        {
            lock (this)
            {
                this.myPeerId = peerId;
                peers.Remove(peerId);
            }
        }

        public int PeerId
        {
            get
            {
                return myPeerId;
            }
        }

        public int ClusterSize
        {
            get
            {
                lock (this)
                {
                    return 1 + peers.Count;
                }
            }
        }

        public long CurrentTerm
        {
            get
            {
                return Interlocked.Read(ref currentTerm);
            }
        }

        public int LeaderId
        {
            get
            {
                return leaderId;
            }
        }

        public bool IsValidPeer(int peerId)
        {
            return peers.ContainsKey(peerId);
        }

        private void RescheduleElection()
        {
            lock (this)
            {
                this.electionTimeout = DateTimeOffset.Now.ToUnixTimeMilliseconds() + config.ElectionTimeoutFixedMillis
                      + random.Next(config.ElectionTimeoutRandomMillis);
            }
        }

        private void LaunchPeriodicTasksThread()
        {
            var t = Task.Run((Func<Task>)(async () =>
            {
                while (role != RaftEngine.Role.Leaving)
                {
                    try
                    {
                        RunPeriodicTasks();
                        await Task.Delay(10);
                    }
                    catch (Exception e)
                    {
                        logger.LogError(e.Message);
                    }
                }
            }));
        }

        /**
         * Called periodically to do recurring work
         */
        private void RunPeriodicTasks()
        {
            lock (this)
            {
                if (!Log.IsRunning && role != Role.Leaving)
                {
                    role = Role.Failed;
                    logger.LogError($"{this} failed.");
                }
                switch (role)
                {
                    case Role.Joining:
                        role = Role.Follower;
                        RescheduleElection();
                        break;
                    case Role.Observer:
                        break;
                    case Role.Follower:
                    case Role.Candidate:
                        if (DateTimeOffset.Now.ToUnixTimeMilliseconds() > electionTimeout)
                        {
                            ProcessElection();
                        }
                        UpdatePendingRequests();
                        break;
                    case Role.Leader:
                        UpdateCommitIndex();
                        UpdatePeers();
                        UpdatePendingRequests();
                        break;
                    case Role.Failed:
                    case Role.Leaving:
                        break;
                }
            }
        }

        private bool IsCommittable(long index)
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

        private void UpdateCommitIndex()
        {
            lock (this)
            {
                Debug.Assert(role == Role.Leader);
                // we can't commit anything until we've replicated something from this term
                if (IsCommittable(firstIndexOfTerm))
                {
                    // we can commit any entry a majority of peers have replicated
                    long index = Log.LastIndex;
                    foreach (PeerState peer in peers.Values)
                    {
                        index = Math.Min(index, peer.matchIndex);
                    }
                    index = Math.Max(index, Log.CommitIndex);
                    while (index <= Log.LastIndex && IsCommittable(index))
                    {

                        // Logging for PT #102932910
                        var e = Log.GetEntry(index);
                        if (e != null && lastTermCommitted != e.Term)
                        {
                            logger.LogInformation($"Committed new term {e.Term}");
                            foreach (var kv in peers)
                            {
                                var p = kv.Value;
                                logger.LogInformation($" - {p} has matchIndex {p.matchIndex} >= {firstIndexOfTerm} ({p.matchIndex >= firstIndexOfTerm})");
                            }
                            lastTermCommitted = e.Term;
                        }
                        Log.CommitIndex = index;
                        index++;
                    }
                }
            }
        }

        private void ProcessElection()
        {
            lock (this)
            {
                role = Role.Candidate;
                Log.UpdateStateMachine();

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
                        rpc.SendRequestVote(config.ClusterName, peer.peerId, currentTerm, myPeerId, Log.LastIndex, Log.LastTerm,
                              (term, voteGranted) =>
                              {
                                  lock (this)
                                  {
                                      if (!StepDown(term))
                                      {
                                          if (term == currentTerm && role == Role.Candidate)
                                          {
                                              if (voteGranted)
                                              {
                                                  votes++;
                                              }
                                              if (votes > votesNeeded)
                                              {
                                                  BecomeLeader();
                                              }
                                          }
                                      }
                                  }
                              });
                    }
                }
                else
                {
                    BecomeLeader();
                }
                RescheduleElection();
            }
        }

        public void HandleVoteRequest(String clusterName, long term, int candidateId, long lastLogIndex, long lastLogTerm,VoteResponseHandler handler)
        {
            lock (this)
            {

                if (config.ClusterName != clusterName || !IsValidPeer(candidateId))
                {
                    return;
                }

                if (term > currentTerm)
                {
                    StepDown(term);
                }
                if (term >= currentTerm && (votedFor == 0 || votedFor == candidateId) && lastLogIndex >= Log.LastIndex
                      && lastLogTerm >= Log.LastTerm)
                {
                    votedFor = candidateId;
                    RescheduleElection();

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

        private bool StepDown(long term)
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
                        ClearAllPendingRequests();
                    }
                    RescheduleElection();
                    return true;
                }
                return false;
            }
        }

        public void BecomeLeader()
        {
            lock (this)
            {

                logger.LogInformation($"{this} is becoming the leader (term {currentTerm})");
                role = Role.Leader;
                leaderId = myPeerId;
                firstIndexOfTerm = Log.LastIndex + 1;
                foreach (PeerState peer in peers.Values)
                {
                    peer.matchIndex = 0;
                    peer.nextIndex = Log.LastIndex + 1;
                    peer.appendPending = false;
                    peer.snapshotTransfer = null;
                    peer.fresh = true;
                    Debug.Assert(peer.nextIndex != 0);
                }

                // Force a new term command to mark the occasion and hasten
                // commitment of any older entries in our log from the 
                // previous term
                ExecuteCommand(new NewTermCommand(myPeerId, currentTerm), null);

                UpdatePeers();
            }
        }

        /**
         * As leader, we need to make sure we continually keep our peers up to date, and when no entries are made, to send a heart beat so that
         * they do not call an election
         */
        private void UpdatePeers()
        {
            lock (this)
            {
                Debug.Assert(role == Role.Leader);
                foreach (var peer in peers.Values) UpdatePeer(peer);
            }
        }

        private void UpdatePeer(PeerState peer)
        {
            lock (this)
            {
                long now = DateTimeOffset.Now.ToUnixTimeMilliseconds();
                if (peer.appendPending && now > peer.lastAppendMillis + 5000)
                {
                    peer.appendPending = false; // time out the last append
                }
                if (!peer.appendPending && (peer.nextIndex < Log.LastIndex || now > peer.lastAppendMillis + config.HeartbeatMillis))
                {
                    Debug.Assert(peer.nextIndex > 0);

                    // for a fresh peer we'll start with an empty list of entries so we can learn what index the node is already on in it's log
                    // fetch entries from log to send to the peer
                    Entry[] entries = (!peer.fresh && peer.snapshotTransfer == null)
                      ? Log.GetEntries(peer.nextIndex, config.MaxEntriesPerRequest) : null;

                    // if this peer needs entries we no longer have, then send them a snapshot
                    if (!peer.fresh && peer.nextIndex > 0 && peer.nextIndex < Log.FirstIndex && entries == null)
                    {
                        logger.LogInformation($"{this} peer {peer.peerId} needs a snapshot");
                        InstallSnapshot(peer);
                    }
                    else
                    {
                        long prevLogIndex = peer.nextIndex - 1;
                        long prevLogTerm = Log.GetTerm(prevLogIndex);

                        // debugging a weird situation:
                        if ((entries == null || entries.Length == 0) && peer.nextIndex < Log.CommitIndex)
                        {
                            logger.LogWarning($"Empty entries for peer {peer} (fresh={peer.fresh},snap={peer.snapshotTransfer}): peerIndex={peer.nextIndex}, firstIndex={Log.FirstIndex}, lastIndex={Log.LastIndex} : {entries}");
                            var e = Log.GetEntry(peer.nextIndex);
                            logger.LogWarning($"{peer.nextIndex} = {e}");
                        }

                        logger.LogTrace($"{this} is sending append entries to {peer.peerId}");
                        peer.lastAppendMillis = now;
                        peer.appendPending = true;
                        peer.snapshotTransfer = null;
                        rpc.SendAppendEntries(peer.peerId, currentTerm, myPeerId, prevLogIndex, prevLogTerm, entries, Log.CommitIndex,
                              (term, success, lastLogIndex) =>
                              {
                                  lock (this)
                                  {
                                      peer.appendPending = false;
                                      if (role == Role.Leader)
                                      {
                                          if (!StepDown(term))
                                          {
                                              peer.fresh = false;
                                              if (success)
                                              {
                                                  if (entries != null)
                                                  {
                                                      peer.matchIndex = entries[entries.Length - 1].Index;
                                                      peer.nextIndex = peer.matchIndex + 1;
                                                      Debug.Assert(peer.nextIndex != 0);
                                                  }
                                                  else
                                                  {
                                                      peer.nextIndex = Math.Max(lastLogIndex + 1, 1);
                                                  }
                                                  UpdatePeer(peer);
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

        public void HandleAppendEntriesRequest(long term, int leaderId, long prevLogIndex, long prevLogTerm, IEnumerable<Entry> entries,
             long leaderCommit, AppendEntriesResponseHandler handler)
        {
            lock (this)
            {
                if (!Log.IsRunning)
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
                        rescheduled = StepDown(term);
                        role = Role.Follower;
                    }
                    
                    if(!rescheduled)
                        RescheduleElection();

                    if (Log.IsConsistentWith(prevLogIndex, prevLogTerm))
                    {
                        if (entries != null)
                        {
                            foreach (Entry e in entries)
                            {
                                if (!Log.Append(e))
                                {
                                    logger.LogWarning($"{this} is failing append entries from {leaderId}: {e}");
                                    handler(currentTerm, false, Log.LastIndex);
                                    return;
                                }
                            }
                        }

                        Log.CommitIndex = Math.Min(leaderCommit, Log.LastIndex);

                        logger.LogTrace($"{this} is fine with append entries from {leaderId}");
                        handler(currentTerm, true, Log.LastIndex);
                        return;
                    }
                    else
                    {
                        logger.LogWarning($"{this} is failing with inconsistent append entries from {leaderId}");
                        logger.LogWarning($"Leader prevLogTerm={prevLogTerm}, prevLogIndex={prevLogIndex}");
                        logger.LogWarning($"Follower firstTerm={Log.FirstTerm}, firstIndex={Log.FirstIndex}");
                        logger.LogWarning($"Follower lastTerm={Log.LastTerm}, lastIndex={Log.LastIndex}");

                        if (prevLogIndex > Log.CommitIndex)
                        {
                            Log.WipeConflictedEntries(prevLogIndex);
                        }
                        else
                        {
                            Stop();
                        }

                    }
                }

                logger.LogTrace($"{this} is rejecting append entries from {leaderId}");
                handler(currentTerm, false, Log.LastIndex);
            }
        }

        private void InstallSnapshot(PeerState peer)
        {
            lock (this)
            {

                if (peer.snapshotTransfer == null)
                {
                    peer.snapshotTransfer = Path.Combine(Log.LogDirectoryName, "raft.snapshot");
                    InstallSnapshot(peer, 0);
                }
            }
        }

        private void InstallSnapshot(PeerState peer, int part)
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
                    long snapshotIndex = StateManager.GetSnapshotIndex(peer.snapshotTransfer);
                    if (snapshotIndex > 0)
                    {
                        int partSize = config.SnapshotPartSize;
                        long len = peer.snapshotTransfer.Length;
                        var data = RaftUtil.GetFilePart(peer.snapshotTransfer, part * partSize,
                         (int)Math.Min(partSize, len - part * partSize));

                        rpc.SendInstallSnapshot(peer.peerId, currentTerm, myPeerId, len, partSize, part, data,

                      (bool success) =>
                        {
                            lock (this)
                            {
                                if (success)
                                {
                                    if ((part + 1) * partSize < len)
                                    {
                                        // send the next part
                                        InstallSnapshot(peer, part + 1);
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

        public void HandleInstallSnapshotRequest(long term, long index, long length, int partSize, int part, byte[] data,InstallSnapshotResponseHandler handler)
        {
            logger.LogInformation($"handleInstallSnapshot: length={length} part={part}");
            RescheduleElection();

            var file = new FileInfo(Path.Combine(Log.LogDirectoryName, "raft.installing.snapshot"));
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
                                    File.Move(file.FullName, Path.Combine(Log.LogDirectoryName, "raft.snapshot"));
                                    Log.LoadSnapshot();
                                    RescheduleElection();
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

        public void HandleClientRequest(ICommand command, ClientResponseHandler handler)
        {
            lock (this)
            {
                ExecuteCommand(command, handler);
            }
        }

        public bool ExecuteCommand(ICommand command, ClientResponseHandler handler = null)
        {
            lock (this)
            {
                if (role == Role.Leader)
                {
                    logger.LogInformation($"{this} is executing a command");
                    Entry e = Log.Append(currentTerm, command);
                    if (e != null)
                    {
                        if (handler != null)
                        {
                            lock (pendingCommands)
                            {
                                pendingCommands.Enqueue(new PendingCommand(e, handler));
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

        public void ExecuteAfterCommandProcessed(Entry e, ClientResponseHandler handler)
        {
            if (e.Index <= Log.StateMachineIndex)
            {
                handler(e);
            }
            else
            {
                lock (pendingCommands)
                {
                    pendingCommands.Enqueue(new PendingCommand(e, handler));
                }
            }
        }

        /**
         * Pop all the pending command requests from our list that are now safely replicated to the majority and applied to our state machine
         */
        private void UpdatePendingRequests()
        {

            lock (pendingCommands)
            {
                while (pendingCommands.Count > 0)
                {
                    //  logger.LogInformation("Updating All Pending Requests {} > {} ", pendingCommands.Count, log.getCommitIndex());
                    PendingCommand item = pendingCommands.Dequeue();
                    if (item.entry.Index <= Log.StateMachineIndex)
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

        private void ClearAllPendingRequests()
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

        public void AddPeer(int peerId)
        {
            lock (this)
            {
                if (peerId != 0 && peerId != this.myPeerId)
                {
                    peers.Add(peerId, new PeerState(peerId));
                }
            }
        }

        public void OnLogEntryApplied(Entry entry)
        {
            //       Command command = entry.getCommand();
            //      if (command.getCommandType() == StateMachine.COMMAND_ID_ADD_PEER) {
            //             AddPeerCommand addPeerCommand = ((AddPeerCommand) command);
            //         if (addPeerCommand.bootstrap) {
            //            logger.LogInformation(" ********************** BOOTSTRAP **********************", addPeerCommand.peerId);
            //            peers.clear();
            //         }
            //         if (addPeerCommand.peerId != this.myPeerId) {
            //            logger.LogInformation(" ********************** AddPeer #{} ********************** ", addPeerCommand.peerId);
            //            peers.put(addPeerCommand.peerId, new Peer(addPeerCommand.peerId));
            //         }
            //      } else if (command.getCommandType() == StateMachine.COMMAND_ID_DEL_PEER) {
            //             DelPeerCommand delPeerCommand = ((DelPeerCommand) command);
            //         logger.LogInformation(" ********************** DelPeer #{} ********************** ", delPeerCommand.peerId);
            //         peers.remove(delPeerCommand.peerId);
            //      }
        }
    }
}

