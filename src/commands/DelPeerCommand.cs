using System.IO;

namespace NRaft
{

    /**
     * Formally removes a raft peer from the cluster. The peer is considered to have left the of quorum after this command is committed.
     */
    public class DelPeerCommand : Command<Config>
    {
        public int CommandId => StateManager.COMMAND_ID_DEL_PEER;

        public int peerId;

        public DelPeerCommand() { }

        public DelPeerCommand(int peerId)
        {
            this.peerId = peerId;
        }

        public void ApplyTo(Config state)
        {
            state.DeletePeer(peerId);
        }

        public void Serialize(BinaryWriter writer)
        {
            writer.Write(peerId);

        }

        public void Deserialize(BinaryReader reader, int fileVersion)
        {
            peerId = reader.ReadInt32();
        }
    }
}
