namespace NRaft {
    public abstract class RequestMessage {
        public int MessageType;
    }

    public class RequestVoteRequest : RequestMessage
    {
        public const int MSG_ID = 1;

        private string clusterName;
        private int peerId;
        private long term;
        private int candidateId;
        private long lastLogIndex;
        private long lastLogTerm;

        public RequestVoteRequest(string clusterName, int peerId, long term, int candidateId, long lastLogIndex, long lastLogTerm)
        {
            this.MessageType = MSG_ID;
            this.ClusterName = clusterName;
            this.PeerId = peerId;
            this.Term = term;
            this.CandidateId = candidateId;
            this.LastLogIndex = lastLogIndex;
            this.LastLogTerm = lastLogTerm;
        }

        public string ClusterName { get => clusterName; set => clusterName = value; }
        public long Term { get => term; set => term = value; }
        public int CandidateId { get => candidateId; set => candidateId = value; }
        public long LastLogIndex { get => lastLogIndex; set => lastLogIndex = value; }
        public long LastLogTerm { get => lastLogTerm; set => lastLogTerm = value; }
        public int PeerId { get => peerId; set => peerId = value; }
    }

    public class ResponseMessage {

    }

    public class RequestVoteResponse : ResponseMessage
    {
        public long Term { get; set; }
        public bool VoteGranted { get; set; }
    }
}