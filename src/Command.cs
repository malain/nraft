namespace NRaft
{
    /**
     * A command deterministically updates a state machine
     */
    public interface Command<T> 
    {

        /**
         * Based on this command, deterministically update the state machine
         */
        void applyTo(T state);

        /**
         * Writes this command to an output stream
         */
        void write(System.IO.BinaryWriter writer);

        /**
         * Read this command to from an input stream
         */
        void read(System.IO.BinaryReader reader, int fileVersion);

        /**
         * Get a unique and stable integer id for this command type.
         * 
         * Negative numbers are reserved for internal commands.
         */
        int getCommandType();
    }
}