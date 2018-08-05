using System;
using System.Diagnostics;
using System.IO;

namespace NRaft {

    public class Entry
    {
        private readonly long term;
        private readonly long index;
        private readonly Command command;

        public long Term => term;

        public long Index => index;

        public Command Command => command;

        public Entry(long term, long index, Command command)
        {
            this.term = term;
            this.index = index;
            this.command = command;
        }

        /**
         * Read this command to from an input stream
         */
        internal Entry(BinaryReader reader, int fileVersion, Log log)
        {
            term = reader.ReadInt64();
            index = reader.ReadInt64();
            var typeId = reader.ReadInt32();
            command = log.makeCommandById(typeId);
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
            writer.Write(Term);
            writer.Write(Index);
            writer.Write(command.CommandId);
            Command.write(writer);
        }

        public override string ToString()
        {
            return $"Entry<{Term}:{Index}>";
        }
    }
}
