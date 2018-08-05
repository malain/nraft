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
    
    public interface ICommandManager
    {
        void RegisterCommand<T>() where T : ICommand;
    }

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
    internal class Log : ICommandManager
    {
        private Dictionary<int, Func<ICommand>> commandfactories = new Dictionary<int, Func<ICommand>>();

        public static readonly ILogger logger = LoggerFactory.GetLogger<Log>();

        public static readonly int LOG_FILE_VERSION = 3;

        /**
         * The log's in-memory buffer of log entries
         */
        private readonly List<Entry> entries = new List<Entry>();

        private readonly Config config;
        private LRUCache entryFileCache = new LRUCache();

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
        private StateManager stateManager;
        public bool IsRunning => cancel != null && !cancel.IsCancellationRequested;
        public Log(Config config, StateManager stateManager)
        {
            config.RegisterCommands(this);
            stateManager.RegisterCommands(this);

            this.stateManager = stateManager;
            this.config = config;
            Directory.CreateDirectory(this.config.LogDirectory);

            // obtain the raft logs lock file
            //  obtainFileLock();

            // restore our state to the last snapshot
            LoadSnapshot();

            // load all subsequent entries in our log
            ReplayLogs();

            // apply entries to our state machine
            UpdateStateMachine();

            // fire up our thread for writing log files 
            Task.Run(WriteLoop);
        }

        public StateManager StateManager
        {
            get
            {
                return stateManager;
            }
        }

        /**
         * Attempts to append the log entry to our log.
         * 
         * @return true if the entry was successfully appended to the log, or was already in our log to begin with
         */
        public bool Append(Entry entry)
        {
            lock (this)
            {
                // Debug.Assert entry != null;
                // check if the entry is already in our log
                if (entry.Index <= lastIndex)
                {
                    //Debug.Assert entry.index >= commitIndex : entry.index + " >= " + commitIndex;
                    if (GetTerm(entry.Index) != entry.Term)
                    {
                        logger.LogWarning($"Log is conflicted at {entry} : {GetTerm(entry.Index)}");
                        WipeConflictedEntries(entry.Index);
                    }
                    else
                    {
                        return true; // we already have this entry
                    }
                }


                // validate that this is an acceptable entry to append next
                if (entry.Index == lastIndex + 1 && entry.Term >= lastTerm)
                {
                    //logger.LogInformation("### APPENDING {} {} - {}", entry.term, entry.index, entry.command);

                    // append to log
                    entries.Add(entry);

                    // update our indexes
                    if (firstIndex == 0)
                    {
                        Debug.Assert(entries.Count == 1);
                        firstIndex = entry.Index;
                        firstTerm = entry.Term;

                        logger.LogInformation($"Setting First Index = {firstIndex} ({entry.Index})");
                    }
                    lastIndex = entry.Index;
                    lastTerm = entry.Term;

                    return true;
                }

                return false;
            }
        }

        /**
         * Append a new command to the log. Should only be called by a Leader
         */
        public Entry Append(long term, ICommand command)
        {
            lock (this)
            {
                Entry e = new Entry(term, lastIndex + 1, command);
                if (Append(e))
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
        public Entry GetEntry(long index)
        {
            lock (this)
            {
                if (index > 0 && index <= lastIndex)
                {
                    if (index >= firstIndex && entries.Count > 0)
                    {
                        Debug.Assert(index - firstIndex < int.MaxValue);
                        Debug.Assert(firstIndex == entries[0].Index);
                        //Debug.Assert( (index - firstIndex) < entries.Count : "index=" + index + ", first=" + firstIndex;
                        Entry e = entries[(int)(index - firstIndex)];
                        Debug.Assert(e.Index == index);
                        return e;
                    }
                    else
                    {
                        return GetEntryFromDisk(index);
                    }
                }
                return null; // we don't have it!
            }
        }

        /**
         * Fetch entries from fromIndex, up to maxEntries. Returns all or none.
         */
        public Entry[] GetEntries(long fromIndex, int maxEntries)
        {
            if (fromIndex > lastIndex)
            {
                return null;
            }

            var list = new Entry[(int)Math.Min(maxEntries, (lastIndex - fromIndex) + 1)];
            for (int i = 0; i < list.Length; i++)
            {
                list[i] = GetEntry(fromIndex + i);
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
        public long GetTerm(long index)
        {
            if (index == 0)
            {
                return 0;
            }
            if (index == stateManager.PrevIndex)
            {
                return stateManager.PrevTerm;
            }
            if (index == stateManager.Index)
            {
                return stateManager.Term;
            }
            if (index == snapshotIndex)
            {
                return snapshotTerm;
            }
            Entry e = GetEntry(index);
            if (e == null)
            {
                logger.LogError($"Could not find entry in log for {index}");
            }
            return e.Term;
        }

        /**
         * Deletes all uncommitted entries after a certain index
         */
        public void WipeConflictedEntries(long index)
        {
            lock (this)
            {
                Debug.Assert(index > snapshotIndex);
                if (index <= commitIndex)
                {
                    Stop();
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
                    lastTerm = GetTerm(lastIndex);
                }
                else
                {
                    lastTerm = 0;
                }
            }
        }

        public string LogDirectoryName
        {
            get
            {
                return config.LogDirectory;
            }
        }

        public long FirstIndex
        {
            get
            {
                return Interlocked.Read(ref firstIndex);
            }
        }

        public long FirstTerm
        {
            get
            {
                return Interlocked.Read(ref firstTerm);
            }
        }

        public long LastIndex
        {
            get
            {
                return Interlocked.Read(ref lastIndex);
            }
        }

        public long LastTerm
        {
            get
            {
                return Interlocked.Read(ref lastTerm);
            }
        }

        public long CommitIndex
        {
            get
            {
                return Interlocked.Read(ref commitIndex);
            }
            set
            {
                Interlocked.Exchange(ref commitIndex, value);
            }
        }

        public long StateMachineIndex
        {
            get
            {
                return stateManager.Index; // TODO synchronized
            }
        }

        /**
         * See if our log is consistent with the purported leader
         * 
         * @return false if log doesn't contain an entry at index whose term matches
         */
        public bool IsConsistentWith(long index, long term)
        {
            if (index == 0 && term == 0 || index > lastIndex)
            {
                return true;
            }
            if (index == snapshotIndex && term == snapshotTerm)
            {
                return true;
            }
            Entry entry = GetEntry(index);
            if (entry == null)
            {
                if (index == stateManager.PrevIndex)
                {
                    return term == stateManager.PrevTerm;
                }
            }

            return (entry != null && entry.Term == term);
        }

        public void Stop()
        {
            lock (this)
            {
                logger.LogInformation("Stopping log...");
                cancel.Cancel();
                try
                {
                    UpdateStateMachine();
                    if (writer != null)
                    {
                        writer.Flush();
                        writer.Close();
                        writer = null;
                    }
                    logger.LogInformation($"commitIndex = {commitIndex}, lastIndex = {lastIndex}");
                }
                catch (Exception t)
                {
                    logger.LogError(t.Message);
                }
            }
        }

        private async Task WriteLoop()
        {
            while (!cancel.IsCancellationRequested)
            {
                try
                {
                    UpdateStateMachine();
                    Compact();
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
                    Stop();
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
        private string GetFileName(long index, bool forReading)
        {
            long firstIndexInFile = (index / config.EntriesPerFile) * config.EntriesPerFile;
            var file = Path.Combine(LogDirectoryName, firstIndexInFile.ToString("X16") + ".log");
            if (forReading)
            {
                // if the config's entriesPerFile has changed, we need to scan files to find the right one

                // if the file is cached, we can do a quick check
                lock (entryFileCache)
                {
                    List<Entry> list = entryFileCache.Get(Path.GetFullPath(file));
                    if (list != null && list.Count > 0)
                    {
                        if (list[0].Index <= index && list[list.Count - 1].Index >= index)
                        {
                            return file;
                        }
                    }
                }

                string bestFile = null;
                long bestIndex = 0;
                foreach (var f in Directory.GetFiles(LogDirectoryName))
                {
                    string fn = Path.GetFileName(f);
                    var m = Regex.Match(fn, "([A-F0-9]{16})\\.log");
                    if (m.Success)
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

        private void EnsureCorrectLogFile(long index)
        {
            lock (this)
            {
                if (index % config.EntriesPerFile == 0)
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
                    string file = GetFileName(index, false);
                    if (File.Exists(file))
                    {
                        File.Move(file, Path.Combine(LogDirectoryName, "old." + Path.GetFileName(file)));
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
        internal void UpdateStateMachine()
        {
            lock (this)
            {
                try
                {
                    lock (stateManager)
                    {
                        while (commitIndex > stateManager.Index)
                        {
                            Entry e = GetEntry(stateManager.Index + 1);
                            Debug.Assert(e != null);
                            Debug.Assert(e.Index == stateManager.Index + 1);
                            stateManager.Apply(e);
                            EnsureCorrectLogFile(e.Index);
                            e.Serialize(writer);
                            if (e.Command is NewTermCommand)
                            {
                                logger.LogInformation($"Writing new term {e}");
                            }
                            if ((e.Index % config.EntriesPerSnapshot) == 0)
                            {
                                SaveSnapshot();
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

        public void LoadSnapshot()
        {
            lock (this)
            {
                var file = Path.Combine(LogDirectoryName, "raft.snapshot");
                if (File.Exists(file))
                {
                    logger.LogInformation($"Loading snapshot {file} ");
                    stateManager.ReadSnapshot(file);
                    logger.LogInformation($"Loaded snapshot @ {stateManager.Term}:{stateManager.Index}");
                    commitIndex = snapshotIndex = lastIndex = stateManager.Index;
                    snapshotTerm = lastTerm = stateManager.Term;
                    firstIndex = 0;
                    firstTerm = 0;
                    entries.Clear();
                    entryFileCache.Clear();
                }
            }
        }

        public ICommand CreateCommand(int id)
        {
            if (!commandfactories.TryGetValue(id, out Func<ICommand> factory))
            {
                throw new Exception("Could not find command factory for command type " + id);
            }

            return factory();
        }

        public void RegisterCommand<TCommand>() where TCommand : ICommand
        {
            Func<ICommand> factory = () => (TCommand)Activator.CreateInstance<TCommand>();
            try
            {
                var cmd = factory();
                var id = cmd.CommandId;
                commandfactories.Add(id, factory);
            }
            catch
            {
                throw new Exception($"Command {typeof(TCommand).FullName} must have a public default constructor.");
            }
        }

        /**
         * Read and apply all available entries in the log from disk
         * 
         * @throws FileNotFoundException
         */
        private void ReplayLogs()
        {
            lock (this)
            {
                Entry entry = null;
                do
                {
                    entry = GetEntryFromDisk(stateManager.Index + 1);
                    if (entry != null)
                    {
                        stateManager.Apply(entry);
                    }
                } while (entry != null);

                // get the most recent file of entries
                List<Entry> list = LoadLogFile(GetFileName(stateManager.Index, true));
                if (list != null && list.Count > 0)
                {
                    Debug.Assert(entries.Count == 0);
                    entries.AddRange(list);
                    firstIndex = entries[0].Index;
                    firstTerm = entries[0].Term;
                    lastIndex = entries[entries.Count - 1].Index;
                    lastTerm = entries[entries.Count - 1].Term;
                    // TODO: rename existing file in case of failure
                    // re-write writer the last file
                    writer = new BinaryWriter(File.OpenWrite(GetFileName(firstIndex, true)));
                    writer.Write(LOG_FILE_VERSION);
                    foreach (Entry e in list)
                    {
                        e.Serialize(writer);
                    }
                    writer.Flush();
                    commitIndex = lastIndex;
                    logger.LogInformation($"Log First Index = {firstIndex}, Last Index = {lastIndex}");
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
        //    private Map<String, List<Entry>> entryFileCache = new LinkedHashMap<String, List<Entry>>(3, 0.75f, true) {
        //       protected bool removeEldestEntry(Map.Entry<String, List<Entry>> eldest) {
        //          return size() > 2;
        //       }
        //    };

        private Entry GetEntryFromDisk(long index)
        {
            var file = GetFileName(index, true);
            if (File.Exists(file))
            {
                List<Entry> list = LoadLogFile(file);
                if (list != null && list.Count > 0)
                {
                    int i = (int)(index - list[0].Index);
                    if (i >= 0 && i < list.Count)
                    {
                        Debug.Assert(list[i].Index == index);
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

        public List<Entry> LoadLogFile(string file)
        {
            lock (entryFileCache)
            {
                List<Entry> list = entryFileCache.Get(Path.GetFullPath(file));
                if (list == null)
                {
                    list = new List<Entry>();
                    if (File.Exists(file))
                    {
                        logger.LogInformation($"Loading Log File {file}");
                        try
                        {
                            using (var reader = new BinaryReader(File.OpenRead(file)))
                            {
                                int version = reader.ReadInt32();
                                Debug.Assert(version <= LOG_FILE_VERSION);

                                Entry last = null;
                                while (reader.BaseStream.Position != reader.BaseStream.Length)
                                {
                                    Entry e = new Entry(reader, version, this);
                                    if (last != null)
                                    {
                                        if (e.Index != last.Index + 1)
                                        {
                                            logger.LogError($"Log File {file} is inconsistent. {last} followed by {e}");
                                        }

                                        Debug.Assert(e.Term >= last.Term);
                                        Debug.Assert(e.Index == last.Index + 1);
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
        private void Compact()
        {
            lock (this)
            {
                if (entries.Count > config.EntriesPerFile * 2)
                {

                    if (firstIndex > commitIndex || firstIndex > stateManager.Index || firstIndex > lastIndex - config.EntriesPerFile)
                    {
                        return;
                    }

                    logger.LogInformation($"Compacting log size = {entries.Count}");
                    List<Entry> entriesToKeep = new List<Entry>();
                    foreach (Entry e in entries)
                    {
                        if (e.Index > commitIndex || e.Index > stateManager.Index || e.Index > lastIndex - config.EntriesPerFile)
                        {
                            entriesToKeep.Add(e);
                        }
                    }
                    entries.Clear();
                    entries.AddRange(entriesToKeep);

                    Entry first = entries.First();
                    firstIndex = first.Index;
                    firstTerm = first.Term;
                    logger.LogInformation($"Compacted log new size = {entries.Count}, firstIndex = {firstIndex}");
                }
            }
        }

        private void ArchiveOldLogFiles()
        {
            if (config.DeleteOldFiles)
            {
                var archiveDir = Path.Combine(LogDirectoryName, "archived");
                Directory.CreateDirectory(archiveDir);

                long index = commitIndex - (config.EntriesPerSnapshot * 4);
                while (index >= 0)
                {
                    logger.LogInformation($" Checking ::  {index.ToString("X16")}");
                    var file = GetFileName(index, true);
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
                    index -= config.EntriesPerFile;
                }

                var p = new Regex("raft\\.([0-9A-F]{16})\\.snapshot");
                foreach (var file in Directory.GetFiles(LogDirectoryName))
                {
                    var m = p.Matches(Path.GetFileName(file));
                    if (m.Count > 0)
                    {
                        long snapIndex = Convert.ToInt64(m[0].Groups[1].Value, 16);
                        logger.LogInformation($"{index.ToString("X16")} Checking {file}: {snapIndex.ToString("X16")}");
                        if (snapIndex < index)
                        {
                            if (snapIndex % (config.EntriesPerSnapshot * 16) == 0)
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
        public long SaveSnapshot()
        {
            // currently pauses the world to save a snapshot
            lock (stateManager)
            {
                var openFile = Path.Combine(LogDirectoryName, "open.snapshot");
                logger.LogInformation($"Saving snapshot @ {stateManager.Index.ToString("X16")}");
                stateManager.WriteSnapshot(openFile, GetTerm(stateManager.PrevIndex));
                var file = Path.Combine(LogDirectoryName, "raft.snapshot");
                if (File.Exists(file))
                {
                    long oldIndex = NRaft.StateManager.GetSnapshotIndex(file);
                    File.Move(file, Path.Combine(LogDirectoryName, $"raft.{oldIndex.ToString("X16")}.snapshot"));
                }
                File.Move(openFile, file);
                ArchiveOldLogFiles();
                return stateManager.Index;
            }
        }
    }
}
