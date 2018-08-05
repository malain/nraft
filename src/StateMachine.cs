using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Microsoft.Extensions.Logging;

namespace NRaft
{
    internal partial class StateManager {
        public static int SNAPSHOT_FILE_VERSION = 1;
        public static int COMMAND_ID_ADD_PEER = -1;
        public static int COMMAND_ID_DEL_PEER = -2;
        public static int COMMAND_ID_NEW_TERM = -3;
        public static int COMMAND_ID_HEALTH_CHECK = -4;

        public static long getSnapshotIndex(string path)
        {
            try
            {
                using (var reader = new BinaryReader(File.OpenRead(path)))
                {
                    int version = reader.ReadInt16();
                    //Debug.Assert (version <= SNAPSHOT_FILE_VERSION);
                    long term = reader.ReadInt64();
                    long index = reader.ReadInt64();
                    return index;
                }
            }
            catch (IOException)
            {
               // logger.error(e.Message, e);
                return 0;
            }
        }
    }

    public class PeerInfo
    {
        public int peerId;
        public string host;
        public int port;

        public PeerInfo(System.IO.BinaryReader reader)
        {
            peerId = reader.ReadInt32();
            host = reader.ReadString();
            port = reader.ReadInt32();
        }

        public PeerInfo(int peerId, string host, int port)
        {
            this.peerId = peerId;
            this.host = host;
            this.port = port;
        }

        public void write(System.IO.BinaryWriter writer)
        {
            writer.Write(peerId);
            writer.Write(host);
            writer.Write(port);
        }

        public override string ToString()
        {
            return $"Peer-{peerId}({host}:{port})";
        }
    }

    public interface IStateMachine {

        void saveState(System.IO.BinaryWriter writer);

        void loadState(System.IO.BinaryReader reader);
        void registerCommand(ICommandManager manager);
    }

    /**
     * The state machine applies commands to update state.
     * 
     * It contains the state we want to coordinate across a distributed cluster.
     * 
     */
    internal partial class StateManager 
    {
        public static readonly ILogger logger = LoggerFactory.GetLogger<StateManager>();

        public enum SnapshotMode
        {
            /**
             * Blocking mode is memory efficient, but blocks all changes while writing the snapshot. Only suitable for small state machines that
             * can write out very quickly
             */
            Blocking,

            /**
             * Dedicated mode maintains an entire secondary copy of the state machine in memory for snapshots. This allows easy non-blocking
             * snapshots, at the expense of using more memory to hold the second state machine, and the processing time to apply commands twice.
             */
            Dedicated,

            /**
             * If your state machine can support copy-on-writes, this is the most efficient mode for non-blocking snapshots
             */
            CopyOnWrite
        }

        private List<Action<Entry>> listeners = new List<Action<Entry>>();

        // State
        private long index;
        private long term;
        private long checksum = 0;
        private long count = 0;
        private long prevIndex;
        private long prevTerm;
        private Dictionary<int, PeerInfo> peers = new Dictionary<int, PeerInfo>();

        /**
         * The timestamp of when we last applied a command
         */
        private long lastCommandAppliedMillis;

        public IStateMachine StateMachine { get; private set; }

        public StateManager(IStateMachine stateMachine)
        {
            this.StateMachine = stateMachine;
        }

        public SnapshotMode getSnapshotMode()
        {
            return SnapshotMode.Blocking;
        }

        public void writeSnapshot(string path, long prevTerm)
        {
            using (var writer = new System.IO.BinaryWriter(File.OpenWrite(path)))
            { // TODO zip
                writer.Write(StateManager.SNAPSHOT_FILE_VERSION);
                writer.Write(term);
                writer.Write(index);
                writer.Write(prevTerm);
                writer.Write(count);
                writer.Write(checksum);
                writer.Write(peers.Count);
                foreach (var peer in peers.Values)
                {
                    peer.write(writer);
                }

                StateMachine.saveState(writer);
            }
        }

        public void readSnapshot(string path)
        {
            using (var reader = new BinaryReader(File.OpenRead(path)))
            {
                int fileVersion = reader.ReadInt32();
                if (fileVersion > StateManager.SNAPSHOT_FILE_VERSION)
                {
                    throw new IOException("Incompatible Snapshot Format: " + fileVersion + " > " + StateManager.SNAPSHOT_FILE_VERSION);
                }
                term = reader.ReadInt64();
                index = reader.ReadInt64();
                prevIndex = index - 1;
                prevTerm = reader.ReadInt64();
                count = reader.ReadInt64();
                checksum = reader.ReadInt64();
                peers.Clear();
                int numPeers = reader.ReadInt32();
                for (int i = 0; i < numPeers; i++)
                {
                    PeerInfo p = new PeerInfo(reader);
                    peers.Add(p.peerId, p);
                }
                StateMachine.loadState(reader);
            }
        }

        /**
         * Return the time we last applied a command
         */
        public long getLastCommandAppliedMillis()
        {
            return lastCommandAppliedMillis;
        }

        public long getIndex()
        {
            return index;
        }

        public long getTerm()
        {
            return term;
        }

        public long getPrevIndex()
        {
            return prevIndex;
        }

        public long getPrevTerm()
        {
            return prevTerm;
        }

        internal void apply(Entry entry)
        {
            //Debug.Assert (this.index + 1 == entry.index) : (this.index + 1) + "!=" + entry.index;
            Debug.Assert(this.term <= entry.Term);
            entry.Command.applyTo(this.StateMachine);
            this.index = entry.Index;
            this.term = entry.Term;
            lastCommandAppliedMillis = DateTime.Now.Millisecond;
            fireEntryAppliedEvent(entry);
        }

        private void fireEntryAppliedEvent(Entry entry)
        {
            lock (listeners)
            {
                foreach (var listener in listeners)
                {
                    try
                    {
                        listener(entry);
                    }
                    catch (Exception t)
                    {
                        logger.LogError(t.Message);
                    }
                }
            }
        }

        public void addListener(Action<Entry> listener)
        {
            lock (listeners)
            {
                this.listeners.Add(listener);
            }
        }

        public PeerInfo addPeer(string host, int port, bool bootstrap)
        {
            if (bootstrap)
            {
                peers.Clear();
            }
            int peerId = 1;
            // find first available peerId
            while (peers.ContainsKey(peerId))
            {
                peerId++;
            }
            PeerInfo p = new PeerInfo(peerId, host, port);
            peers.Add(peerId, p);
            return p;
        }

        public void delPeer(int peerId)
        {
            peers.Remove(peerId);
        }

        public IEnumerable<PeerInfo> getPeers()
        {
            return peers.Values;
        }

        public void applyHealthCheck(long val)
        {
            checksum ^= (val * index * ++count);
            //logger.info("CHECKSUM {} = {}:{}", val, checksum, count);
        }

        public long getChecksum()
        {
            return checksum;
        }
    }
}