using System.IO;

namespace NRaft
{

    /**
     * A somewhat dummy command to mark the start of a new term. The new leader always publishes this as the first command of their new term,
     * which helps facilitate advancing the commitIndex, as we cannot commit any older log entries until we've replicated something in the new
     * term to a majority.
     */
    public class NewTermCommand : Command<IStateMachine>
    {
        public int CommandId => StateManager.COMMAND_ID_NEW_TERM;

        private long term;
        private int peerId;

        public NewTermCommand() { }

        public NewTermCommand(int peerId, long term)
        {
            this.peerId = peerId;
            this.term = term;
        }

        public void ApplyTo(IStateMachine state) { }

        public void Serialize(BinaryWriter writer)
        {
            writer.Write(term);
            writer.Write(peerId);
        }

        public void Deserialize(BinaryReader reader, int fileVersion)
        {
            term = reader.ReadInt64();
            peerId = reader.ReadInt32();
        }
    }
}
