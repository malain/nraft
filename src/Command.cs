namespace NRaft
{
    /**
     * A command deterministically updates a state machine
     */
    public interface Command
    {
        int CommandId { get; }

        /**
* Based on this command, deterministically update the state machine
*/
        void ApplyTo(object state);

        /**
         * Writes this command to an output stream
         */
        void Serialize(System.IO.BinaryWriter writer);

        /**
         * Read this command to from an input stream
         */
        void Deserialize(System.IO.BinaryReader reader, int fileVersion);
    }
}