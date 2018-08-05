namespace NRaft
{
    public interface ICommand {
        /**
         * Writes this command to an output stream
         */
        void Serialize(System.IO.BinaryWriter writer);

        /**
         * Read this command to from an input stream
         */
        void Deserialize(System.IO.BinaryReader reader, int fileVersion);

        int CommandId { get; }
    }

    /**
     * A command deterministically updates a state machine
     */
    public interface Command<T> : ICommand where T: IStateMachine
    {
        /**
* Based on this command, deterministically update the state machine
*/
        void ApplyTo(T state);

    }
}