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
        void applyTo(object state);

        /**
         * Writes this command to an output stream
         */
        void write(System.IO.BinaryWriter writer);

        /**
         * Read this command to from an input stream
         */
        void read(System.IO.BinaryReader reader, int fileVersion);
    }
}