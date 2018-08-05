namespace NRaft
{
    public class AppendEntriesRequest : RequestMessage
    {
        public const int MSG_ID = 2;

        private int peerId;
        private long term;
        private int leaderId;
        private long prevLogIndex;
        private long prevLogTerm;
        private string data;
        private long leaderCommit;

        public AppendEntriesRequest(int peerId, long term, int leaderId, long prevLogIndex, long prevLogTerm, string data, long leaderCommit)
        {
            this.MessageType = MSG_ID;
            this.PeerId = peerId;
            this.Term = term;
            this.LeaderId = leaderId;
            this.PrevLogIndex = prevLogIndex;
            this.PrevLogTerm = prevLogTerm;
            this.Data = data;
            this.LeaderCommit = leaderCommit;
        }

        public int PeerId { get => peerId; set => peerId = value; }
        public long Term { get => term; set => term = value; }
        public int LeaderId { get => leaderId; set => leaderId = value; }
        public long PrevLogIndex { get => prevLogIndex; set => prevLogIndex = value; }
        public long PrevLogTerm { get => prevLogTerm; set => prevLogTerm = value; }
        public string Data { get => data; set => data = value; }
        public long LeaderCommit { get => leaderCommit; set => leaderCommit = value; }
    }
}