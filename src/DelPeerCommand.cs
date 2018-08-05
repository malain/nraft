using System.IO;

namespace NRaft
{

    /**
     * Formally removes a raft peer from the cluster. The peer is considered to have left the of quorum after this command is committed.
     */
    public class DelPeerCommand : Command
    {
        public int CommandId => StateManager.COMMAND_ID_DEL_PEER;

        public int peerId;

        public DelPeerCommand() { }

        public DelPeerCommand(int peerId)
        {
            this.peerId = peerId;
        }

        public void applyTo(object state)
        {
            ((StateManager)state).delPeer(peerId);
        }

        public void write(BinaryWriter writer)
        {
            writer.Write(peerId);

        }

        public void read(BinaryReader reader, int fileVersion)
        {
            peerId = reader.ReadInt32();
        }
    }
}
