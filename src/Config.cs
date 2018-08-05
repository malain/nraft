namespace NRaft
{

    /**
     * Raft Configuration. All the magic numbers for turning performance to your specific needs.
     */
    public class Config
    {
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
    }
}