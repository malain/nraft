namespace NRaft
{
    public class InstallSnapshotResponse : ResponseMessage
    {
        public bool Success { get; internal set; }
    }
}