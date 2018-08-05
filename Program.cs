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
using NRaft;

namespace NRaftTest
{
    public class Program
    {
        private static int NUM_PEERS = 3;
        private static Dictionary<int, RaftEngine> rafts = new Dictionary<int, RaftEngine>();
        private static string[] logDirs = new string[NUM_PEERS];
        private static Random rnd = new Random();
        private static string keys = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

        private static void Dump(RaftEngine node)
        {
            Console.WriteLine($"State machine for {node} ---------------");
            node.GetStateMachineManager<TestStateMachine>().Dump();
        }

        public static async Task TestSnapshots()
        {
            var logDir = "logs/snapshot";

            if (Directory.Exists(logDir))
                Directory.Delete(logDir, true);
            Directory.CreateDirectory(logDir);

            var manager = new TestStateMachine();
            var state =  new StateManager(manager);
            Config config = new Config().setLogDir(logDir);
            config.setEntriesPerFile(16);
            config.setEntriesPerSnapshot(32);
            var log = new Log(config, state);

            // write a bunch of entries
            for (int i = 0; i < 100; i++)
            {
                log.Append(1, MakeNewCommand());
            }

            // wait for commits to write 
            log.CommitIndex = log.LastIndex;
            while (log.StateManager.Index < log.LastIndex)
            {
                await Task.Delay(100);
            }

            var checksum = manager.getCheckSum();
            log.Stop();

            // load new log from snapshot & files
            manager = new TestStateMachine();
            state = new StateManager(manager);
                        log = new Log(config, state);

            Debug.Assert(checksum == manager.getCheckSum());
            Debug.Assert(96 == log.FirstIndex);
            Debug.Assert(100 == log.LastIndex);
        }

        public static async Task TestLog()
        {
            var logDir = "logs/logs";

            if (Directory.Exists(logDir))
                Directory.Delete(logDir, true);
            Directory.CreateDirectory(logDir);

            var manager = new TestStateMachine();
            var state = new StateManager(manager);
            Config config = new Config().setLogDir(logDir);

            // create a log
            var log = new Log(config, state);

            // write a bunch of entries
            for (int i = 0; i < 10; i++)
            {
                log.Append(1, MakeNewCommand());
            }

            Debug.Assert(1 == log.FirstIndex);
            Debug.Assert(10 == log.LastIndex);

            // test getting all of the entries by index and edges
            Debug.Assert(log.GetEntry(0) == null);
            for (int i = 1; i <= 10; i++)
            {
                Entry e = log.GetEntry(i);
                Debug.Assert(e != null);
                Debug.Assert(i == e.Index);
            }
            Debug.Assert(log.GetEntry(11) == null);

            // make sure we can append a higher term
            Debug.Assert(log.Append(new Entry(2, 11, MakeNewCommand())) == true);
            Debug.Assert(log.GetEntry(11) != null);

            // make sure we cannot append a lower term
            Debug.Assert(log.Append(new Entry(1, 12, MakeNewCommand())) == false);
            Debug.Assert(log.GetEntry(12) == null);

            log.CommitIndex = log.LastIndex;
            while (log.StateManager.Index < log.LastIndex)
            {
                await Task.Delay(100);
            }
            var checksum = manager.getCheckSum();
            //logger.info("State = {}", state);
            log.Stop();

            await Task.Delay(1000);

            // create a new log

            manager = new TestStateMachine();
            state = new StateManager(manager);      
                  log = new Log(config, state);
            Debug.Assert(1 == log.FirstIndex);
            Debug.Assert(11 == log.LastIndex);

            log.CommitIndex = log.LastIndex;
            while (log.StateManager.Index < log.LastIndex)
            {
                await Task.Delay(100);
            }
            Debug.Assert(checksum == manager.getCheckSum());
            // logger.info("State = {}", state);

            // write a bunch of entries
            for (int i = 0; i < 10; i++)
            {
                log.Append(3, MakeNewCommand());
            }
            Debug.Assert(1 == log.FirstIndex);
            Debug.Assert(21 == log.LastIndex);
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
                RaftEngine raft = new RaftEngine(cfg, new TestStateMachine(), new RPC(rafts));
                raft.SetPeerId(i);
                for (int j = 1; j <= NUM_PEERS; j++)
                {
                    if (j != i)
                    {
                        raft.AddPeer(j);
                    }
                }
                Dump(raft);
                rafts.Add(i, raft);
            }

            Task.Run(async () =>
            {
                foreach (var raft in rafts.Values)
                {
                    raft.Start(raft.PeerId);
                    await Task.Delay(100);
                }

                await Task.Delay(3000);
                while (true)
                {
                    await Task.Delay(1000 + rnd.Next(20) * 500);
                    foreach (var r in rafts.Values)
                    {
                        r.ExecuteCommand(MakeNewCommand(), null);
                    }
                }
            });
            Console.ReadKey();
            foreach (var r in rafts.Values)
            {
                r.Stop();
                Dump(r);
            }
        }

        public static void Main(string[] args)
        {
            TestLog().GetAwaiter().GetResult();
            TestSnapshots().GetAwaiter().GetResult();
            // CreateWebHostBuilder(args).Build().Run();
        }

        // public static IWebHostBuilder CreateWebHostBuilder(string[] args) =>
        //     WebHost.CreateDefaultBuilder(args)
        //         .UseStartup<Startup>();
    }
}
