using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace NRaft
{
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

        public void Serialize(System.IO.BinaryWriter writer)
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

    /**
     * Raft Configuration. All the magic numbers for turning performance to your specific needs.
     */
    public class Config : IStateMachine
    {
        private Dictionary<int, PeerInfo> peers = new Dictionary<int, PeerInfo>();

        public IEnumerable<PeerInfo> Peers { get { lock (peers) { return peers.Values.ToArray(); } } }

        public static readonly int DEFAULT_ELECTION_TIMEOUT_FIXED_MILLIS = 1500;
        public static readonly int DEFAULT_ELECTION_TIMEOUT_RANDOM_MILLIS = 2500;
        public static readonly int DEFAULT_HEARTBEAT_MILLIS = 250;
        public static readonly int DEFAULT_MAX_ENTRIES_PER_REQUEST = 250;
        public static readonly int DEFAULT_PART_SIZE = 1024 * 256;
        public static readonly int NUM_ENTRIES_PER_LOGFILE = 0x2000;
        public static readonly int NUM_ENTRIES_PER_SNAPSHOT = 0x10000;
        public static readonly bool DELETE_OLD_FILES = true;

        /**
          * Get the fixed timeout length in millis before we will call an election.
          */
        public int ElectionTimeoutFixedMillis { get; private set; } = DEFAULT_ELECTION_TIMEOUT_FIXED_MILLIS;
        /**
        * Get the randomized timeout length in millis before we will call an election.
        */
        public int ElectionTimeoutRandomMillis { get; private set; } = DEFAULT_ELECTION_TIMEOUT_RANDOM_MILLIS;
        /**
        * Get the heart beat interval. The leader will send a hear tbeat this often, even if there are no new entries.
        */
        public int HeartbeatMillis { get; private set; } = DEFAULT_HEARTBEAT_MILLIS;
        /**
        * Get the maximum number of log entries per AppendEntries RPC
        */
        public int MaxEntriesPerRequest { get; private set; } = DEFAULT_MAX_ENTRIES_PER_REQUEST;
        /**
        * Get the maximum bytes sent per InstallSnapshot RPC.
        */
        public int SnapshotPartSize { get; private set; } = DEFAULT_PART_SIZE;
        /**
        * Get the number of log entries we will store per log file
        */
        public int EntriesPerFile { get; private set; } = NUM_ENTRIES_PER_LOGFILE;
        /**
         * Get the number of log entries we will process between taking snapshots.
         */
        public int EntriesPerSnapshot { get; private set; } = NUM_ENTRIES_PER_SNAPSHOT;
        /**
        * Get if we delete old log files to recover disk space, or keep a permanent log of all entries.
        */
        public bool DeleteOldFiles { get; private set; } = DELETE_OLD_FILES;
        /**
         * The directory where we will read and write raft data files
         */
        public string LogDirectory { get; private set; } = "raft-logs";
        /**
         * The name of our cluster which provides a fail-safe to prevent nodes from accidentally joining the wrong raft cluster.
         */
        public string ClusterName { get; private set; }
        /*
         * Current peer id 
         */
        public int PeerId { get; private set; }

        public PeerInfo GetPeer(int peerId) {
            return peers[peerId];
        }

        public Config(int peerId)
        {
            PeerId = peerId;
        }

        /**
         * Set the fixed timeout length in millis before we will call an election.
         * 
         * This should be significantly greater than the RPC latency and heartbeat timeout.
         */
        public Config WithElectionTimeoutFixedMillis(int electionTimeoutFixedMillis)
        {
            this.ElectionTimeoutFixedMillis = electionTimeoutFixedMillis;
            return this;
        }

        /**
         * Set the randomized timeout length in millis before we will call an election. Larger values reduce the chances of failed elections, but
         * increase the time it may take to call an election after the leader fails.
         */
        public Config WithElectionTimeoutRandomMillis(int electionTimeoutRandomMillis)
        {
            this.ElectionTimeoutRandomMillis = electionTimeoutRandomMillis;
            return this;
        }

        /**
         * Set the heart beat interval.
         */
        public Config WithHeartbeatMillis(int heartbeatMillis)
        {
            this.HeartbeatMillis = heartbeatMillis;
            return this;
        }

        /**
         * Set the maximum number of log entries per AppendEntries RPC. This should be tuned for your particular RPC implementation and typical
         * Command data size and rates.
         */
        public Config WithMaxEntriesPerRequest(int maxEntriesPerRequest)
        {
            this.MaxEntriesPerRequest = maxEntriesPerRequest;
            return this;
        }

        /**
         * Set the directory where we store snapshots and log files
         */
        public Config WithLogDir(string logDir)
        {
            this.LogDirectory = logDir;
            return this;
        }

        /**
         * Get our configured cluster name. Our peers must all have matching cluster names to vote in elections. This prevents misconfigured
         * nodes from accidentally joining or voting in the wrong cluster.
         */
        public Config WithClusterName(string clusterName)
        {
            this.ClusterName = clusterName;
            return this;
        }

        /**
         * Set the maximum bytes sent per InstallSnapshot RPC. This should be tuned appropriately for your RPC implementation and network
         * conditions.
         */
        public Config WithSnapshotPartSize(int snapshotPartSize)
        {
            this.SnapshotPartSize = snapshotPartSize;
            return this;
        }

        /**
         * Set the number of log entries we will store per log file
         */
        public Config WithEntriesPerFile(int entriesPerFile)
        {
            this.EntriesPerFile = entriesPerFile;
            return this;
        }

        /**
         * Set if we will delete older log files to recover disk space, or keep a permanent log of all entries.
         */
        public Config WithDeleteOldFiles(bool deleteOldFiles)
        {
            this.DeleteOldFiles = deleteOldFiles;
            return this;
        }

        /**
         * Set the number of log entries we will process between taking snapshots.
         */
        public Config WithEntriesPerSnapshot(int entriesPerSnapshot)
        {
            this.EntriesPerSnapshot = entriesPerSnapshot;
            return this;
        }

        public PeerInfo AddPeer(string host, int port, bool bootstrap, int peerId=0)
        {
            lock (peers)
            {
                if (bootstrap)
                {
                    peers.Clear();
                }
                if (peerId == 0)
                {
                    // find first available peerId
                    while (peers.ContainsKey(peerId))
                    {
                        peerId++;
                    }
                }
                PeerInfo p = new PeerInfo(peerId, host, port);
                peers.Add(peerId, p);
                return p;
            }
        }

        public PeerInfo AddPeer(int peerId, string host = "localhost", int port = 30140)
        {
            if(peerId == 0)
                throw new System.ArgumentOutOfRangeException("peerId must be greater than 0.");

            PeerInfo p = new PeerInfo(peerId, host, port);
            peers.Add(peerId, p);
            return p;
        }

        public void DeletePeer(int peerId)
        {
            lock (peers)
            {
                peers.Remove(peerId);
            }
        }

        void IStateMachine.SaveState(BinaryWriter writer)
        {
            writer.Write(this.ClusterName);
            writer.Write(this.DeleteOldFiles);
            writer.Write(this.ElectionTimeoutFixedMillis);
            writer.Write(this.ElectionTimeoutRandomMillis);
            writer.Write(this.EntriesPerFile);
            writer.Write(this.EntriesPerSnapshot);
            writer.Write(this.HeartbeatMillis);
            writer.Write(this.LogDirectory);
            writer.Write(this.MaxEntriesPerRequest);
            writer.Write(this.PeerId);
            writer.Write(this.SnapshotPartSize);
            writer.Write(peers.Count);
            foreach(var peer in Peers) {
                peer.Serialize(writer);
            }
        }

        void IStateMachine.LoadState(BinaryReader reader)
        {
            this.ClusterName = reader.ReadString();
            this.DeleteOldFiles = reader.ReadBoolean();
            this.ElectionTimeoutFixedMillis = reader.ReadInt32();
            this.ElectionTimeoutRandomMillis = reader.ReadInt32();
            this.EntriesPerFile = reader.ReadInt32();
            this.EntriesPerSnapshot = reader.ReadInt32();
            this.HeartbeatMillis = reader.ReadInt32();
            this.LogDirectory = reader.ReadString();
            this.MaxEntriesPerRequest = reader.ReadInt32();
            this.PeerId = reader.ReadInt32();
            this.SnapshotPartSize = reader.ReadInt32();
            var cx = reader.ReadInt32();
            peers.Clear();
            for (var i = 0; i < cx;i++)
            {
                var peer = new PeerInfo(reader);
                peers.Add(peer.peerId, peer );
        }
        }

        public void RegisterCommands(ICommandManager manager)
        {
            manager.RegisterCommand<AddPeerCommand>();
            manager.RegisterCommand<DelPeerCommand>();
            manager.RegisterCommand<NewTermCommand>();
        }

        public static Config FromFile(string configFileName) {
            if(!File.Exists(configFileName))
                return null;

            var cfg = new Config(0);
            using(var reader = new BinaryReader(File.OpenRead(configFileName))) {
                ((IStateMachine)cfg).LoadState(reader);
            }
            return cfg;
        }

        public void Save(string configFileName) {
            Directory.CreateDirectory(Path.GetDirectoryName(configFileName));
            using (var writer = new BinaryWriter(new FileStream(configFileName, FileMode.Create)))
            {
                ((IStateMachine)this).SaveState(writer);
            }
        }
    }
}