namespace NRaft
{
    internal class InstallSnapshotRequest : RequestMessage
    {
        public const int MSG_ID = 3;

        private int peerId;
        private long term;
        private long index;
        private long length;
        private int partSize;
        private int part;
        private string data;

        public InstallSnapshotRequest(int peerId, long term, long index, long length, int partSize, int part, string data)
        {
            this.MessageType = MSG_ID;
            this.PeerId = peerId;
            this.Term = term;
            this.Index = index;
            this.Length = length;
            this.PartSize = partSize;
            this.Part = part;
            this.Data = data;
        }

        public int PeerId { get => peerId; set => peerId = value; }
        public long Term { get => term; set => term = value; }
        public long Index { get => index; set => index = value; }
        public long Length { get => length; set => length = value; }
        public int PartSize { get => partSize; set => partSize = value; }
        public int Part { get => part; set => part = value; }
        public string Data { get => data; set => data = value; }
    }
}