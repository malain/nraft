using System.Collections.Generic;
using System.Threading.Tasks;

namespace NRaft
{
    public interface IRpcSender
    {
        Task<ResponseMessage> SendMessage(PeerInfo peer, RequestMessage msg);
    }

    ///////// Request Handlers ///////// 

    public interface IRaftRequests
    {
        void HandleVoteRequest(string clusterName, long term, int candidateId, long lastLogIndex, long lastLogTerm, VoteResponseHandler handler);

        void HandleAppendEntriesRequest(long term, int leaderId, long prevLogIndex, long prevLogTerm, IEnumerable<Entry> entries, long leaderCommit, AppendEntriesResponseHandler handler);

        void HandleInstallSnapshotRequest(long term, long index, long length, int partSize, int part, byte[] data, InstallSnapshotResponseHandler handler);

        void HandleClientRequest(ICommand command, ClientResponseHandler handler);
    }

    /**
     * Delegates all the asynchronous RPC implementation for raft to a third party.
     */
    public interface IRaftRPC
    {
        ///////// Request Senders ///////// 
        void SendRequestVote(string clusterName, int peerId, long term, int candidateId, long lastLogIndex, long lastLogTerm, VoteResponseHandler handler);

        void SendAppendEntries(int peerId, long term, int leaderId, long prevLogIndex, long prevLogTerm, Entry[] entries, long leaderCommit, AppendEntriesResponseHandler handler);

        void SendInstallSnapshot(int peerId, long term, long index, long length, int partSize, int part, byte[] data, InstallSnapshotResponseHandler handler);
    }

    ///////// Response Handlers ///////// 

    public delegate void VoteResponseHandler(long term, bool voteGranted);

    public delegate void AppendEntriesResponseHandler(long term, bool success, long lastLogIndex);

    public delegate void InstallSnapshotResponseHandler(bool success);

    public delegate void ClientResponseHandler(Entry entry);
}
