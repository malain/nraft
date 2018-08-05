using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;

namespace NRaft {

    public class Entry
    {
        private readonly long term;
        private readonly long index;
        private readonly ICommand command;

        public long Term => term;

        public long Index => index;

        public ICommand Command => command;

        private MethodInfo applyMethod;

        public Entry(long term, long index, ICommand command)
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
            command = log.CreateCommand(typeId);
            if (command == null)
            {
                throw new IOException("Could not create command type " + typeId);
            }
            command.Deserialize(reader, fileVersion);
        }

        /**
         * Writes this entry to an output stream
         */
        public void Serialize(BinaryWriter writer)
        {
            writer.Write(Term);
            writer.Write(Index);
            writer.Write(command.CommandId);
            Command.Serialize(writer);
        }

        public override string ToString()
        {
            return $"Entry<{Term}:{Index}>";
        }

        internal void InvokeApplyTo(IStateMachine stateMachine)
        {
            if( applyMethod == null)
                applyMethod = Command.GetType().GetMethod("ApplyTo");
                
            applyMethod.Invoke(Command, new object[] { stateMachine });
        }
    }
}
