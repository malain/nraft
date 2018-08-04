using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace NRaft
{
    /**
     * A raft log is the backbone of the raft algorithm. It stores an ordered list of commands that have been agreed upon by consensus, as well
     * as the tentative list of future commands yet to be confirmed.
     * 
     * 
     * <ul>
     * <li>TODO: Add a proper file lock so we can ensure only one raft process to a raft-dir</li>
     * <li>TODO: Make config constants configurable
     * </ul>
     * 
     */
    public class Log<T> where T : StateMachine<T>
    {

        public static readonly ILogger logger = LoggerFactory.GetLogger<Log<T>>();

        public static readonly int LOG_FILE_VERSION = 3;

        /**
         * The log's in-memory buffer of log entries
         */
        private readonly List<Entry<T>> entries = new List<Entry<T>>();

        private readonly Config config;
        private LRUCache<T> entryFileCache = new LRUCache<T>();

        /**
         * Our current journal file's output stream
         */
        private BinaryWriter writer;
        private CancellationTokenSource cancel = new CancellationTokenSource();

        // We keep some key index & term variables that may or 
        // may not be in our buffer and are accessed frequently:

        private long snapshotIndex = 0;
        private long snapshotTerm = 0;
        private long firstIndex = 0;
        private long firstTerm = 0;
        private long lastIndex = 0;
        private long lastTerm = 0;
        private long commitIndex = 0;

        /**
         * The state machine we are coordinating via raft
         */
        private T stateMachine;
        public bool IsRunning => cancel != null && !cancel.IsCancellationRequested;
        public Log(Config config, T stateMachine)
        {
            this.stateMachine = stateMachine;
            this.config = config;
            Directory.CreateDirectory(this.config.getLogDir());

            // obtain the raft logs lock file
          //  obtainFileLock();

            // restore our state to the last snapshot
            loadSnapshot();

            // load all subsequent entries in our log
            replayLogs();

            // apply entries to our state machine
            updateStateMachine();

            // fire up our thread for writing log files 
            Task.Run(writeLoop);
        }

        public T getStateMachine()
        {
            return  stateMachine;
        }

        /**
         * Attempts to append the log entry to our log.
         * 
         * @return true if the entry was successfully appended to the log, or was already in our log to begin with
         */
        public bool append(Entry<T> entry)
        {
            lock (this)
            {
                // Debug.Assert entry != null;
                // check if the entry is already in our log
                if (entry.index <= lastIndex)
                {
                    //Debug.Assert entry.index >= commitIndex : entry.index + " >= " + commitIndex;
                    if (getTerm(entry.index) != entry.term)
                    {
                        logger.LogWarning($"Log is conflicted at {entry} : {getTerm(entry.index)}");
                        wipeConflictedEntries(entry.index);
                    }
                    else
                    {
                        return true; // we already have this entry
                    }
                }


                // validate that this is an acceptable entry to append next
                if (entry.index == lastIndex + 1 && entry.term >= lastTerm)
                {
                    //logger.LogInformation("### APPENDING {} {} - {}", entry.term, entry.index, entry.command);

                    // append to log
                    entries.Add(entry);

                    // update our indexes
                    if (firstIndex == 0)
                    {
                        Debug.Assert(entries.Count == 1);
                        firstIndex = entry.index;
                        firstTerm = entry.term;

                        logger.LogInformation($"Setting First Index = {firstIndex} ({entry.index})");
                    }
                    lastIndex = entry.index;
                    lastTerm = entry.term;

                    return true;
                }

                return false;
            }
        }

        /**
         * Append a new command to the log. Should only be called by a Leader
         */
        public Entry<T> append(long term, Command<T> command)
        {
            lock (this)
            {
                Entry<T> e = new Entry<T>(term, lastIndex + 1, command);
                if (append(e))
                {
                    return e;
                }
                else
                {
                    return null;
                }
            }
        }

        /**
         * Get an entry from our log, by index
         */
        public Entry<T> getEntry(long index)
        {
            lock (this)
            {
                if (index > 0 && index <= lastIndex)
                {
                    if (index >= firstIndex && entries.Count > 0)
                    {
                        Debug.Assert(index - firstIndex < int.MaxValue);
                        Debug.Assert(firstIndex == entries[0].index);
                        //Debug.Assert( (index - firstIndex) < entries.Count : "index=" + index + ", first=" + firstIndex;
                        Entry<T> e = entries[(int)(index - firstIndex)];
                        Debug.Assert(e.index == index);
                        return e;
                    }
                    else
                    {
                        return getEntryFromDisk(index);
                    }
                }
                return null; // we don't have it!
            }
        }

        /**
         * Fetch entries from fromIndex, up to maxEntries. Returns all or none.
         */
        public Entry<T>[] getEntries(long fromIndex, int maxEntries)
        {
            if (fromIndex > lastIndex)
            {
                return null;
            }

            var list = new Entry<T>[(int)Math.Min(maxEntries, (lastIndex - fromIndex) + 1)];
            for (int i = 0; i < list.Length; i++)
            {
                list[i] = getEntry(fromIndex + i);
                if (list[i] == null)
                {
                    logger.LogWarning($"Could not find log entry {fromIndex + i}");
                    return null;
                }
            }
            return list;
        }

        /**
         * Get the term for an entry in our log
         */
        public long getTerm(long index)
        {
            if (index == 0)
            {
                return 0;
            }
            if (index == stateMachine.getPrevIndex())
            {
                return stateMachine.getPrevTerm();
            }
            if (index == stateMachine.getIndex())
            {
                return stateMachine.getTerm();
            }
            if (index == snapshotIndex)
            {
                return snapshotTerm;
            }
            Entry<T> e = getEntry(index);
            if (e == null)
            {
                logger.LogError($"Could not find entry in log for {index}");
            }
            return e.term;
        }

        /**
         * Deletes all uncommitted entries after a certain index
         */
        public void wipeConflictedEntries(long index)
        {
            lock (this)
            {
                Debug.Assert(index > snapshotIndex);
                if (index <= commitIndex)
                {
                    stop();
                    throw new Exception("Can't restore conflicted index already written to disk: " + index);
                }

                // we have a conflict -- we need to throw away all entries from our log from this point on
                while (lastIndex >= index)
                {
                    var e = entries[(int)(lastIndex-- - firstIndex)]; // TODO
                    entries.Remove(e);
                }
                if (lastIndex > 0)
                {
                    lastTerm = getTerm(lastIndex);
                }
                else
                {
                    lastTerm = 0;
                }
            }
        }

        public List<Entry<T>> getEntries()
        {
            return entries;
        }

        public string getLogDirectory()
        {
            return config.getLogDir();
        }

        public long getFirstIndex()
        {
            return Interlocked.Read(ref firstIndex);
        }

        public long getFirstTerm()
        {
            return Interlocked.Read(ref firstTerm);
        }

        public long getLastIndex()
        {
            return Interlocked.Read(ref lastIndex);
        }

        public long getLastTerm()
        {
            return Interlocked.Read(ref lastTerm);
        }

        public long getCommitIndex()
        {
            return Interlocked.Read(ref commitIndex);
        }

        public void setCommitIndex(long index)
        {
            Interlocked.Exchange(ref commitIndex, index);
        }

        public long getStateMachineIndex()
        {
            return stateMachine.getIndex(); // TODO synchronized
        }

        /**
         * See if our log is consistent with the purported leader
         * 
         * @return false if log doesn't contain an entry at index whose term matches
         */
        public bool isConsistentWith(long index, long term)
        {
            if (index == 0 && term == 0 || index > lastIndex)
            {
                return true;
            }
            if (index == snapshotIndex && term == snapshotTerm)
            {
                return true;
            }
            Entry<T> entry = getEntry(index);
            if (entry == null)
            {
                if (index == stateMachine.getPrevIndex())
                {
                    return term == stateMachine.getPrevTerm();
                }
            }

            return (entry != null && entry.term == term);
        }

        public void stop()
        {
            lock (this)
            {
                logger.LogInformation("Stopping log...");
                cancel.Cancel();
                try
                {
                    updateStateMachine();
                    if (writer != null)
                    {
                        writer.Flush();
                        writer.Close();
                        writer = null;
                    }
                    logger.LogInformation($"commitIndex = {commitIndex}, lastIndex = {lastIndex}" );
                }
                catch (Exception t)
                {
                    logger.LogError(t.Message);
                }
            }
        }

        private async Task writeLoop()
        {
            while (!cancel.IsCancellationRequested)
            {
                try
                {
                    updateStateMachine();
                    compact();
                    if (writer != null)
                    {
                        writer.Flush();
                    }
                   // lock (this)
                    {
                        await Task.Delay(100);
                    }
                }
                catch (Exception t)
                {
                    logger.LogError(t.Message);
                    stop();
                }
            }
        }

        // private void obtainFileLock()
        // {
        //     var lockFile = Path.Combine(getLogDirectory(), "lock");
        //     var stream = new File(lockFile);
        //     lock = stream.getChannel().tryLock();
        //     if (lock == null || !lock.isValid())
        //             {
        //                 throw new IOException("File lock held by another process: " + lockFile);
        //             }
        //     // purposefully kept open for lifetime of jvm
        // }

        /**
         * Get the canonical file name for this index
         * 
         * @throws IOException
         */
        private string getFile(long index, bool forReading)
        {
            long firstIndexInFile = (index / config.getEntriesPerFile()) * config.getEntriesPerFile();
            var file = Path.Combine(getLogDirectory(), firstIndexInFile.ToString("X16") + ".log");
            if (forReading)
            {
                // if the config's entriesPerFile has changed, we need to scan files to find the right one

                // if the file is cached, we can do a quick check
                lock (entryFileCache)
                {
                    List<Entry<T>> list = entryFileCache.Get(Path.GetFullPath(file));
                    if (list != null && list.Count > 0)
                    {
                        if (list[0].index <= index && list[list.Count - 1].index >= index)
                        {
                            return file;
                        }
                    }
                }

                string bestFile = null;
                long bestIndex = 0;
                foreach (var f in Directory.GetFiles(getLogDirectory()))
                {
                    string fn = Path.GetFileName(f);
                    var m = Regex.Match(fn, "([A-F0-9]{16})\\.log");
                    if ( m.Success)
                    {
                        long i = Convert.ToInt64(m.Groups[1].Value, 16);
                        if (i <= index && i > bestIndex)
                        {
                            bestFile = f;
                            bestIndex = i;
                        }
                    }
                }
                if (bestFile != null)
                {
                    //logger.LogInformation("Best guess for file containing {} is {}", index, bestFile);
                    file = bestFile;
                }
            }
            return file;
        }

        private void ensureCorrectLogFile(long index)
        {
            lock (this)
            {
                if (index % config.getEntriesPerFile() == 0)
                {
                    if (writer != null)
                    {
                        writer.Flush();
                        writer.Close();
                        writer = null;
                    }
                }
                if (writer == null)
                {
                    string file = getFile(index, false);
                    if (File.Exists(file))
                    {
                        File.Move(file, Path.Combine(getLogDirectory(), "old." + Path.GetFileName(file)));
                    }
                    logger.LogInformation($"Raft Log File : {file}");
                    writer = new BinaryWriter(File.OpenWrite(file)); // TODO Buffered
                    writer.Write(LOG_FILE_VERSION);
                }
            }
        }

        /**
         * Applies committed log entries to our state machine until it is at the given index
         */
        internal void updateStateMachine()
        {
            lock (this)
            {
                try
                {
                    lock (stateMachine)
                    {
                        while (commitIndex > stateMachine.getIndex())
                        {
                            Entry<T> e = getEntry(stateMachine.getIndex() + 1);
                            Debug.Assert(e != null);
                            Debug.Assert(e.index == stateMachine.getIndex() + 1);
                            stateMachine.apply(e);
                            ensureCorrectLogFile(e.index);
                            e.write(writer);
                            if (e.command.getCommandType() == StateMachine.COMMAND_ID_NEW_TERM)
                            {
                                logger.LogInformation($"Writing new term {e}");
                            }
                            if ((e.index % config.getEntriesPerSnapshot()) == 0)
                            {
                                saveSnapshot();
                            }
                        }
                    }
                }
                catch (IOException e)
                {
                    logger.LogError(e.Message);
                    cancel?.Cancel(); // revisit this, but should probably halt
                }
            }
        }

        public void loadSnapshot()
        {
            lock (this)
            {
                var file = Path.Combine(getLogDirectory(), "raft.snapshot");
                if (File.Exists(file))
                {
                    logger.LogInformation($"Loading snapshot {file} ");
                    stateMachine.readSnapshot(file);
                    logger.LogInformation($"Loaded snapshot @ {stateMachine.getTerm()}:{stateMachine.getIndex()}");
                    commitIndex = snapshotIndex = lastIndex = stateMachine.getIndex();
                    snapshotTerm = lastTerm = stateMachine.getTerm();
                    firstIndex = 0;
                    firstTerm = 0;
                    entries.Clear();
                    entryFileCache.Clear();
                }
            }
        }

        /**
         * Read and apply all available entries in the log from disk
         * 
         * @throws FileNotFoundException
         */
        private void replayLogs()
        {
            lock (this)
            {
                Entry<T> entry = null;
                do
                {
                    entry = getEntryFromDisk(stateMachine.getIndex() + 1);
                    if (entry != null)
                    {
                        stateMachine.apply(entry);
                    }
                } while (entry != null);

                // get the most recent file of entries
                List<Entry<T>> list = loadLogFile(getFile(stateMachine.getIndex(), true));
                if (list != null && list.Count > 0)
                {
                    Debug.Assert(entries.Count == 0);
                    entries.AddRange(list);
                    firstIndex = entries[0].index;
                    firstTerm = entries[0].term;
                    lastIndex = entries[entries.Count - 1].index;
                    lastTerm = entries[entries.Count - 1].term;
                    // TODO: rename existing file in case of failure
                    // re-write writer the last file
                    writer = new BinaryWriter(File.OpenWrite(getFile(firstIndex, true)));
                    writer.Write(LOG_FILE_VERSION);
                    foreach (Entry<T> e in list)
                    {
                        e.write(writer);
                    }
                    writer.Flush();
                    commitIndex = lastIndex;
                    logger.LogInformation($"Log First Index = {firstIndex}, Last Index = {lastIndex}" );
                }

                lock (entryFileCache)
                {
                    entryFileCache.Clear();
                }
            }
        }

        /**
         * An LRU cache of entries loaded from disk
         */
        // TODO
        //    private Map<String, List<Entry<T>>> entryFileCache = new LinkedHashMap<String, List<Entry<T>>>(3, 0.75f, true) {
        //       protected bool removeEldestEntry(Map.Entry<String, List<Entry<T>>> eldest) {
        //          return size() > 2;
        //       }
        //    };

        private Entry<T> getEntryFromDisk(long index)
        {
            var file = getFile(index, true);
            if (File.Exists(file))
            {
                List<Entry<T>> list = loadLogFile(file);
                if (list != null && list.Count > 0)
                {
                    int i = (int)(index - list[0].index);
                    if (i >= 0 && i < list.Count)
                    {
                        Debug.Assert(list[i].index == index);
                        return list[i];
                    }
                }
            }
            else
            {
                logger.LogInformation($"Could not find file {file}");
            }
            return null;
        }

        public List<Entry<T>> loadLogFile(string file)
        {
            lock (entryFileCache)
            {
                List<Entry<T>> list = entryFileCache.Get(Path.GetFullPath(file)); 
                if (list == null)
                {
                    list = new List<Entry<T>>();
                    if (File.Exists(file))
                    {
                        logger.LogInformation($"Loading Log File {file}");
                        try
                        {
                            using (var reader = new BinaryReader(File.OpenRead(file)))
                            {
                                int version = reader.ReadInt32();
                                Debug.Assert(version <= LOG_FILE_VERSION);
                                Entry<T> last = null;
                                while (reader.BaseStream.Position != reader.BaseStream.Length)
                                {
                                    Entry<T> e = new Entry<T>(reader, version, stateMachine);
                                    if (last != null)
                                    {
                                        if (e.index != last.index + 1)
                                        {
                                            logger.LogError($"Log File {file} is inconsistent. {last} followed by {e}");
                                        }

                                        Debug.Assert(e.term >= last.term);
                                        Debug.Assert(e.index == last.index + 1);
                                    }
                                    list.Add(e);
                                    last = e;
                                }
                            }
                        }
                        catch (Exception)
                        {
                            logger.LogError($"Read {list.Count} from {file}");
                        }
                    }
                    entryFileCache.Add(file, list);
                }
                return list;
            }
        }

        /**
         * Discards entries from our buffer that we no longer need to store in memory
         */
        private void compact()
        {
            lock (this)
            {
                if (entries.Count > config.getEntriesPerFile() * 2)
                {

                    if (firstIndex > commitIndex || firstIndex > stateMachine.getIndex() || firstIndex > lastIndex - config.getEntriesPerFile())
                    {
                        return;
                    }

                    logger.LogInformation($"Compacting log size = {entries.Count}");
                    List<Entry<T>> entriesToKeep = new List<Entry<T>>();
                    foreach (Entry<T> e in entries)
                    {
                        if (e.index > commitIndex || e.index > stateMachine.getIndex() || e.index > lastIndex - config.getEntriesPerFile())
                        {
                            entriesToKeep.Add(e);
                        }
                    }
                    entries.Clear();
                    entries.AddRange(entriesToKeep);
                    Entry<T> first = entries.First();
                    firstIndex = first.index;
                    firstTerm = first.term;
                    logger.LogInformation($"Compacted log new size = {entries.Count}, firstIndex = {firstIndex}" );
                }
            }
        }

        private void archiveOldLogFiles()
        {
            if (config.getDeleteOldFiles())
            {
                var archiveDir = Path.Combine(getLogDirectory(), "archived");
                Directory.CreateDirectory(archiveDir);

                long index = commitIndex - (config.getEntriesPerSnapshot() * 4);
                while (index >= 0)
                {
                    logger.LogInformation($" Checking ::  {index.ToString("X16")}" );
                    var file = getFile(index, true);
                    if (File.Exists(file))
                    {
                        logger.LogInformation($"Archiving old log file {file}");
                        File.Move(file, Path.Combine(archiveDir, Path.GetFileName(file)));
                        // TODO: Archive into larger log files
                    }
                    else
                    {
                        break; // done archiving
                    }
                    index -= config.getEntriesPerFile();
                }

                var p = new Regex("raft\\.([0-9A-F]{16})\\.snapshot");
                foreach (var file in Directory.GetFiles(getLogDirectory()))
                {
                    var m = p.Matches( Path.GetFileName(file));
                    if (m.Count > 0)
                    {
                        long snapIndex = Convert.ToInt64(m[0].Groups[1].Value, 16);
                        logger.LogInformation($"{index.ToString("X16")} Checking {file}: {snapIndex.ToString("X16")}");
                        if (snapIndex < index)
                        {
                            if (snapIndex % (config.getEntriesPerSnapshot() * 16) == 0)
                            {
                                logger.LogInformation($"Archiving old snapshot file {file}");
                                File.Move(file, Path.Combine(archiveDir, Path.GetFileName(file)));
                            }
                            else
                            {
                                // otherwise delete the older ones
                                logger.LogInformation($"Deleting old snapshot file {file}");
                                File.Delete(file);
                            }
                        }
                    }
                }
            }
        }

        /**
         * Currently is a pause-the-world snapshot
         */
        public long saveSnapshot()
        {
            // currently pauses the world to save a snapshot
            lock (stateMachine)
            {
                var openFile = Path.Combine(getLogDirectory(), "open.snapshot");
                logger.LogInformation($"Saving snapshot @ {stateMachine.getIndex().ToString("X16")}");
                stateMachine.writeSnapshot(openFile, getTerm(stateMachine.getPrevIndex()));
                var file = Path.Combine(getLogDirectory(), "raft.snapshot");
                if (File.Exists(file))
                {
                    long oldIndex = StateMachine.getSnapshotIndex(file);
                    File.Move(file, Path.Combine(getLogDirectory(), $"raft.{oldIndex.ToString("X16")}.snapshot"));
                }
                File.Move(openFile, file);
                archiveOldLogFiles();
                return stateMachine.getIndex();
            }
        }

    }
}