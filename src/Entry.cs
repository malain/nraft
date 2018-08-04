using System;
using System.Diagnostics;
using System.IO;

namespace NRaft {

    public class Entry<T>
    {
        public readonly long term;
        public readonly long index;
        public readonly Command<T> command;

        public Entry(long term, long index, Command<T> command)
        {
            this.term = term;
            this.index = index;
            this.command = command;
        }

        public long getTerm()
        {
            return term;
        }

        public long getIndex()
        {
            return index;
        }

        public Command<T> getCommand()
        {
            return command;
        }

        /**
         * Read this command to from an input stream
         */
        public Entry(BinaryReader reader, int fileVersion, T state)
        {
            term = reader.ReadInt64();
            index = reader.ReadInt64();
            var typeId = reader.ReadInt32();
            command = state.makeCommandById(typeId);
            if (command == null)
            {
                throw new IOException("Could not create command type " + typeId);
            }
            command.read(reader, fileVersion);
        }

        /**
         * Writes this entry to an output stream
         */
        public void write(BinaryWriter writer)
        {
            writer.Write(term);
            writer.Write(index);
            Debug.Assert(command.getCommandType() != 0);
            writer.Write(command.getCommandType());
            command.write(writer);
        }

        public override string ToString()
        {
            return $"Entry<{term}:{index}>";
        }
    }
}
