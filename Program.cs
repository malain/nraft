using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace NRaft
{
    public class Program
    {
        private static int NUM_PEERS = 3;
        private static Dictionary<int, RaftEngine<TestStateMachine>> rafts = new Dictionary<int, RaftEngine<TestStateMachine>>();
        private static string[] logDirs = new string[NUM_PEERS];
        private static Random rnd = new Random();
        private static string keys = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

        private static void Dump(RaftEngine<TestStateMachine> node)
        {
            Console.WriteLine($"State machine for {node} ---------------");
            node.getStateMachine().Dump();
        }
        public static async Task TestSnapshots()
        {
            var logDir = "logs/snapshot";

            if (Directory.Exists(logDir))
                Directory.Delete(logDir, true);
            Directory.CreateDirectory(logDir);

            TestStateMachine state = new TestStateMachine();
            Config config = new Config().setLogDir(logDir);
            config.setEntriesPerFile(16);
            config.setEntriesPerSnapshot(32);
            var log = new Log<TestStateMachine>(config, state);

            // write a bunch of entries
            for (int i = 0; i < 100; i++)
            {
                log.append(1, MakeNewCommand());
            }

            // wait for commits to write 
            log.setCommitIndex(log.getLastIndex());
            while (log.getStateMachine().getIndex() < log.getLastIndex())
            {
                await Task.Delay(100);
            }

            var checksum = state.getCheckSum();
            log.stop();

            // load new log from snapshot & files
            state = new TestStateMachine();
            log = new Log<TestStateMachine>(config, state);

            Debug.Assert(checksum == state.getCheckSum());
            Debug.Assert(96 == log.getFirstIndex());
            Debug.Assert(100 == log.getLastIndex());
        }
        
        public static async Task TestLog()
        {
            var logDir = "logs/logs";

            if (Directory.Exists(logDir))
                Directory.Delete(logDir, true);
            Directory.CreateDirectory(logDir);
            
            TestStateMachine state = new TestStateMachine();
            Config config = new Config().setLogDir(logDir);

            // create a log
            var log = new Log<TestStateMachine>(config, state);

            // write a bunch of entries
            for (int i = 0; i < 10; i++)
            {
                log.append(1, MakeNewCommand());
            }

            Debug.Assert(1 == log.getFirstIndex());
            Debug.Assert(10 == log.getLastIndex());

            // test getting all of the entries by index and edges
            Debug.Assert(log.getEntry(0) == null);
            for (int i = 1; i <= 10; i++)
            {
                Entry<TestStateMachine> e = log.getEntry(i);
                Debug.Assert(e != null);
                Debug.Assert(i == e.index);
            }
            Debug.Assert(log.getEntry(11) == null);

            // make sure we can append a higher term
            Debug.Assert(log.append(new Entry<TestStateMachine>(2, 11, MakeNewCommand())) == true);
            Debug.Assert(log.getEntry(11) != null);

            // make sure we cannot append a lower term
            Debug.Assert(log.append(new Entry<TestStateMachine>(1, 12, MakeNewCommand())) == false);
            Debug.Assert(log.getEntry(12) == null);

            log.setCommitIndex(log.getLastIndex());
            while (log.getStateMachine().getIndex() < log.getLastIndex())
            {
                await Task.Delay(100);
            }
            var checksum = state.getCheckSum();
            //logger.info("State = {}", state);
            log.stop();

            await Task.Delay(1000);

            // create a new log

            state = new TestStateMachine();
            log = new Log<TestStateMachine>(config, state);
            Debug.Assert(1 == log.getFirstIndex());
            Debug.Assert(11 == log.getLastIndex());

            log.setCommitIndex(log.getLastIndex());
            while (log.getStateMachine().getIndex() < log.getLastIndex())
            {
                await Task.Delay(100);
            }
            Debug.Assert(checksum == state.getCheckSum());
            // logger.info("State = {}", state);

            // write a bunch of entries
            for (int i = 0; i < 10; i++)
            {
                log.append(3, MakeNewCommand());
            }
            Debug.Assert(1 == log.getFirstIndex());
            Debug.Assert(21 == log.getLastIndex());
        }

        private static TestCommand MakeNewCommand()
        {
            var key = keys[rnd.Next(25) + 1];
            var cmd = new TestCommand(key.ToString(), rnd.Next(100).ToString());
            return cmd;
        }

        private static void TestCluster()
        {
            for (int i = 0; i < NUM_PEERS; i++)
            {
                logDirs[i] = Path.Combine("logs/test-" + (i + 1));
                Directory.CreateDirectory(logDirs[i]);
            }

            for (int i = 1; i <= NUM_PEERS; i++)
            {
                Config cfg = new Config().setLogDir(logDirs[i - 1]).setClusterName("TEST");
                RaftEngine<TestStateMachine> raft = new RaftEngine<TestStateMachine>(cfg, new TestStateMachine(), new RPC(rafts));
                raft.setPeerId(i);
                for (int j = 1; j <= NUM_PEERS; j++)
                {
                    if (j != i)
                    {
                        raft.addPeer(j);
                    }
                }
                Dump(raft);
                rafts.Add(i, raft);
            }

            Task.Run(async () =>
            {
                foreach (var raft in rafts.Values)
                {
                    raft.start(raft.getPeerId());
                    await Task.Delay(100);
                }

                await Task.Delay(3000);
                while (true)
                {
                    await Task.Delay(1000 + rnd.Next(20) * 500);
                    foreach (var r in rafts.Values)
                    {
                        r.executeCommand(MakeNewCommand(), null);
                    }
                }
            });
            Console.ReadKey();
            foreach (var r in rafts.Values)
            {
                r.stop();
                Dump(r);
            }
        }

        public static void Main(string[] args)
        {
            TestLog().GetAwaiter().GetResult();

            // CreateWebHostBuilder(args).Build().Run();
        }

        // public static IWebHostBuilder CreateWebHostBuilder(string[] args) =>
        //     WebHost.CreateDefaultBuilder(args)
        //         .UseStartup<Startup>();
    }
}
