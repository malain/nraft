using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace NRaft
{
    public class RaftNode : RaftRPC
    {
        public Config Configuration { get; private set; }
        private RaftEngine engine;
        private IRpcSender sender;

        public RaftNode(Config cfg, IStateMachine stateMachine, IRpcSender sender)
        {
            this.sender = sender;
            Configuration = cfg;
            engine = new RaftEngine(Configuration, stateMachine, this);
        }

        public Task<ResponseMessage> HandleMessage(RequestMessage message)
        {
            var tcs = new TaskCompletionSource<ResponseMessage>();

            switch (message.MessageType)
            {
                case AppendEntriesRequest.MSG_ID:
                    var apr = (AppendEntriesRequest)message;
                    var entries = new List<Entry>();
                    using (var stream = new MemoryStream(apr.Data))
                    {
                        using (var reader = new BinaryReader(stream))
                        {
                            while (stream.Length != stream.Position)
                            {
                                entries.Add(new Entry(reader, Log.LOG_FILE_VERSION, engine.Log));
                            }
                        }
                    }

                    engine.HandleAppendEntriesRequest(apr.Term, apr.LeaderId, apr.PrevLogIndex, apr.PrevLogTerm, entries, apr.LeaderCommit,
                    (term, success, lastLogIndex) =>
                    {
                        var response = new AppendEntriesResponse { Term = term, LastLogIndex = lastLogIndex, Success = success };
                        tcs.SetResult(response);
                    });
                    break;
                case InstallSnapshotRequest.MSG_ID:
                    var isr = (InstallSnapshotRequest)message;
                    engine.HandleInstallSnapshotRequest(isr.Term, isr.Index, isr.Length, isr.PartSize, isr.Part, isr.Data, (success) =>
                    {
                        var response = new InstallSnapshotResponse { Success = success };
                        tcs.SetResult(response);
                    });
                    break;
                case RequestVoteRequest.MSG_ID:
                    var rqr = (RequestVoteRequest)message;
                    engine.HandleVoteRequest(rqr.ClusterName, rqr.Term, rqr.CandidateId, rqr.LastLogIndex, rqr.LastLogTerm,
                    (term, voteGranted) =>
                    {
                        var response = new RequestVoteResponse { Term = term, VoteGranted = voteGranted };
                        tcs.SetResult(response);
                    });
                    break;
                default:
                    throw new System.Exception("Unknow message type");
            }

            return tcs.Task;
        }

        void RaftRPC.SendRequestVote(string clusterName, int peerId, long term, int candidateId, long lastLogIndex, long lastLogTerm, VoteResponseHandler handler)
        {
            var msg = new RequestVoteRequest(clusterName, peerId, term, candidateId, lastLogIndex, lastLogTerm);
            Task.Run(async () =>
            {
                var response = await sender.SendMessage(msg) as RequestVoteResponse;
                handler(response.Term, response.VoteGranted);
            });
        }

        void RaftRPC.SendAppendEntries(int peerId, long term, int leaderId, long prevLogIndex, long prevLogTerm, Entry[] entries, long leaderCommit, AppendEntriesResponseHandler handler)
        {
            byte[] data = null;
            var msg = new AppendEntriesRequest(peerId, term, leaderId, prevLogIndex, prevLogTerm, data, leaderCommit);
            Task.Run(async () =>
            {
                var response = await sender.SendMessage(msg) as AppendEntriesResponse;
                handler(response.Term, response.Success, response.LastLogIndex);
            });
        }

        void RaftRPC.SendInstallSnapshot(int peerId, long term, long index, long length, int partSize, int part, byte[] data, InstallSnapshotResponseHandler handler)
        {
            var msg = new InstallSnapshotRequest(peerId, term, index, length, partSize, part, data);
            Task.Run(async () =>
            {
                var response = await sender.SendMessage(msg) as InstallSnapshotResponse;
                handler(response.Success);
            });
        }
    }
}