namespace NRaft {
    public class RequestMessage {
        public const int REQUEST_VOTE = 1;
        public const int APPEND_ENTRIES = 2;
        public const int INSTALL_SNAPSHOT = 3;

        public int MessageType;

        public string ClusterName;
        public int PeerId;
        public long Term;
        public int CandidateId;
        public long LastLogIndex;
        public long LastLogTerm;
        public int LeaderId;
        public long PrevLogIndex;
        public long PrevLogTerm;
        public string Data;
        public long LeaderCommit;
        public long Index;
        public long Length;
        public int PartSize;
        public int Part;

        public RequestMessage() {}
        
        public RequestMessage(string clusterName, int peerId, long term, int candidateId, long lastLogIndex, long lastLogTerm)
        {
            this.MessageType = REQUEST_VOTE;
            this.ClusterName = clusterName;
            this.PeerId = peerId;
            this.Term = term;
            this.CandidateId = candidateId;
            this.LastLogIndex = lastLogIndex;
            this.LastLogTerm = lastLogTerm;
        }

        public RequestMessage(int peerId, long term, int leaderId, long prevLogIndex, long prevLogTerm, string data, long leaderCommit)
        {
            this.MessageType = APPEND_ENTRIES;
            this.PeerId = peerId;
            this.Term = term;
            this.LeaderId = leaderId;
            this.PrevLogIndex = prevLogIndex;
            this.PrevLogTerm = prevLogTerm;
            this.Data = data;
            this.LeaderCommit = leaderCommit;
        }

                public RequestMessage(int peerId, long term, long index, long length, int partSize, int part, string data)
        {
            this.MessageType = INSTALL_SNAPSHOT;
            this.PeerId = peerId;
            this.Term = term;
            this.Index = index;
            this.Length = length;
            this.PartSize = partSize;
            this.Part = part;
            this.Data = data;
        }
    }

    public class ResponseMessage {
        public long Term { get; set; }
        public bool VoteGranted { get; set; }
        public bool Success { get; set; }
        public long LastLogIndex { get; set; }
    }
}