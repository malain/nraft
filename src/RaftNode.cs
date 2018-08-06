using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;

namespace NRaft
{
    public interface IRaftListener {
        Task<ResponseMessage> HandleMessage(RequestMessage message);
    }

    public class RaftNode<T> : IRaftListener, IRaftRPC where T : IStateMachine, new()
    {
        public Config Configuration { get; private set; }
        private RaftEngine engine;
        private IRpcSender sender;

        public RaftNode(Config cfg, IRpcSender sender = null)
        {
            this.sender = sender ?? new HttpRpcClient();
            Configuration = cfg;
            var stateMachine = new T();
            engine = new RaftEngine(Configuration, stateMachine, this);
        }

        public IDisposable Start()
        {
            engine.Start();
            return new DisposableAction(() => engine.Stop());
        }

        public void UseMiddleware(Microsoft.AspNetCore.Builder.IApplicationBuilder app) {
            app.UseMiddleware<RaftMiddleware>(this);
        }

        public void Stop()
        {
            engine.Stop();
        }

        public Task<ResponseMessage> HandleMessage(RequestMessage message)
        {
            var tcs = new TaskCompletionSource<ResponseMessage>();

            switch (message.MessageType)
            {
                case RequestMessage.APPEND_ENTRIES:
                    var entries = new List<Entry>();
                    using (var stream = new MemoryStream(Convert.FromBase64String(message.Data)))
                    {
                        using (var reader = new BinaryReader(stream))
                        {
                            while (stream.Length != stream.Position)
                            {
                                entries.Add(new Entry(reader, Log.LOG_FILE_VERSION, engine.Log));
                            }
                        }
                    }
                    engine.HandleAppendEntriesRequest(message.Term, message.LeaderId, message.PrevLogIndex, message.PrevLogTerm, entries, message.LeaderCommit,
                    (term, success, lastLogIndex) =>
                    {
                        var response = new ResponseMessage { Term = term, LastLogIndex = lastLogIndex, Success = success };
                        tcs.SetResult(response);
                    });
                    break;

                case RequestMessage.INSTALL_SNAPSHOT:
                    engine.HandleInstallSnapshotRequest(message.Term, message.Index, message.Length, message.PartSize, message.Part, Convert.FromBase64String(message.Data), (success) =>
                  {
                      var response = new ResponseMessage { Success = success };
                      tcs.SetResult(response);
                  });
                    break;

                case RequestMessage.REQUEST_VOTE:
                    engine.HandleVoteRequest(message.ClusterName, message.Term, message.CandidateId, message.LastLogIndex, message.LastLogTerm,
                    (term, voteGranted) =>
                    {
                        var response = new ResponseMessage { Term = term, VoteGranted = voteGranted };
                        tcs.SetResult(response);
                    });
                    break;

                default:
                    throw new System.Exception("Unknow message type");
            }

            return tcs.Task;
        }

        void IRaftRPC.SendRequestVote(string clusterName, int peerId, long term, int candidateId, long lastLogIndex, long lastLogTerm, VoteResponseHandler handler)
        {
            var peer = Configuration.GetPeer(peerId);
            var msg = new RequestMessage(clusterName, peerId, term, candidateId, lastLogIndex, lastLogTerm);
            Task.Run(async () =>
            {
                var response = await sender.SendMessage(peer, msg) as ResponseMessage;
                if(response!=null)
                handler(response.Term, response.VoteGranted);
            });
        }

        void IRaftRPC.SendAppendEntries(int peerId, long term, int leaderId, long prevLogIndex, long prevLogTerm, Entry[] entries, long leaderCommit, AppendEntriesResponseHandler handler)
        {
            var peer = Configuration.GetPeer(peerId);

            string data = "[]";
            if (entries != null)
            {
                using (var stream = new MemoryStream())
                {
                    using (var writer = new BinaryWriter(stream))
                    {
                        foreach (var entry in entries)
                        {
                            entry.Serialize(writer);
                        }
                    }
                    data = Convert.ToBase64String(stream.ToArray());
                }
            }
            var msg = new RequestMessage(peerId, term, leaderId, prevLogIndex, prevLogTerm, data, leaderCommit);
            Task.Run(async () =>
            {
                var response = await sender.SendMessage(peer, msg) as ResponseMessage;
                if(response!=null)
                    handler(response.Term, response.Success, response.LastLogIndex);
            });
        }

        void IRaftRPC.SendInstallSnapshot(int peerId, long term, long index, long length, int partSize, int part, byte[] data, InstallSnapshotResponseHandler handler)
        {
            var peer = Configuration.GetPeer(peerId);

            var msg = new RequestMessage(peerId, term, index, length, partSize, part, Convert.ToBase64String(data));
            Task.Run(async () =>
            {
                var response = await sender.SendMessage(peer, msg) as ResponseMessage;
                if(response!=null)
                handler(response.Success);
            });
        }
    }
}