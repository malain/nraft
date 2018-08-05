namespace NRaft
{
    public class AppendEntriesResponse : ResponseMessage
    {
        public long Term { get; set; }
        public bool Success { get; set; }
        public long LastLogIndex { get; set; }
    }
}