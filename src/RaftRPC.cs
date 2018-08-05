using System.Threading.Tasks;

namespace NRaft
{

    ///////// Request Handlers ///////// 

    public interface RaftRequests
    {
        void HandleVoteRequest(string clusterName, long term, int candidateId, long lastLogIndex, long lastLogTerm,
             VoteResponseHandler handler);

        void HandleAppendEntriesRequest(long term, int leaderId, long prevLogIndex, long prevLogTerm, Entry[] entries,
             long leaderCommit, AppendEntriesResponseHandler handler);

        void HandleInstallSnapshotRequest(long term, long index, long length, int partSize, int part, byte[] data,
             InstallSnapshotResponseHandler handler);

        void HandleClientRequest(Command command, ClientResponseHandler handler);
    }

    /**
     * Delegates all the asynchronous RPC implementation for raft to a third party.
     */
    public interface RaftRPC
    {
        ///////// Request Senders ///////// 
        void SendRequestVote(string clusterName, int peerId, long term, int candidateId, long lastLogIndex, long lastLogTerm,
             VoteResponseHandler handler);

        void SendAppendEntries(int peerId, long term, int leaderId, long prevLogIndex, long prevLogTerm, Entry[] entries,
             long leaderCommit, AppendEntriesResponseHandler handler);

        void SendInstallSnapshot(int peerId, long term, long index, long length, int partSize, int part, byte[] data,
             InstallSnapshotResponseHandler handler);

        void SendIssueCommand(int peerId, Command command, ClientResponseHandler handler);
    }

    ///////// Response Handlers ///////// 

    public delegate void VoteResponseHandler(long term, bool voteGranted);
    
    public delegate void AppendEntriesResponseHandler(long term, bool success, long lastLogIndex);

    public delegate void InstallSnapshotResponseHandler(bool success);

    public delegate void ClientResponseHandler(Entry entry);

    public delegate void ClientResponseHandler<C>(C command) where C : Command;
}
