using System.IO;

namespace NRaft
{

    /**
     * Formally add a raft peer to the cluster. The peer is considered part of quorum after this command is committed.
     */
    public class AddPeerCommand : Command
    {
        public string host;
        public int port;
        public int peerId;
        public bool bootstrap;

        public int CommandId => StateManager.COMMAND_ID_ADD_PEER;

        public AddPeerCommand() { }

        public AddPeerCommand(string host, int port, bool bootstrap=false)
        {
            this.host = host;
            this.port = port;
            this.bootstrap = bootstrap;
        }

        public void applyTo(object state)
        {
            peerId = ((StateManager)state).addPeer(host, port, bootstrap).peerId;
        }

        public void write(BinaryWriter writer)
        {
            writer.Write(peerId);
            writer.Write(host);
            writer.Write(port);
            writer.Write(bootstrap);
        }

        public void read(BinaryReader reader, int fileVersion)
        {
            peerId = reader.ReadInt32();
            host = reader.ReadString();
            port = reader.ReadInt32();
            bootstrap = reader.ReadBoolean();
        }
    }
}
