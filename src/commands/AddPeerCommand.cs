using System.IO;

namespace NRaft
{

    /**
     * Formally add a raft peer to the cluster. The peer is considered part of quorum after this command is committed.
     */
    public class AddPeerCommand : Command<Config>
    {
        public string address;
        public int peerId;
        public bool bootstrap;

        public int CommandId => StateManager.COMMAND_ID_ADD_PEER;

        public AddPeerCommand() { }

        public AddPeerCommand(int peerId, string address, bool bootstrap=false)
        {
            this.peerId = peerId;
            this.address = address;
            this.bootstrap = bootstrap;
        }

        public void ApplyTo(Config state)
        {
            peerId = state.AddPeer(address, bootstrap, peerId).peerId;
        }

        public void Serialize(BinaryWriter writer)
        {
            writer.Write(peerId);
            writer.Write(address);
            writer.Write(bootstrap);
        }

        public void Deserialize(BinaryReader reader, int fileVersion)
        {
            peerId = reader.ReadInt32();
            address = reader.ReadString();
            bootstrap = reader.ReadBoolean();
        }
    }
}
