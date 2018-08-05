using System;
using System.Diagnostics;
using System.IO;
using System.Linq.Expressions;
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

        private Action<ICommand, StateManager> applyMethod;

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

        internal void InvokeApplyTo(StateManager manager)
        {
            if (applyMethod == null)
            {
                var method = Command.GetType().GetMethod("ApplyTo");
                var stateMachineType = method.GetParameters()[0].ParameterType;
                var pCommand = Expression.Parameter(typeof(ICommand));
                var pStateManager = Expression.Parameter(typeof(StateManager));

                Expression instance = pStateManager;
                if(stateMachineType != typeof(IHealthCheckStateMachine)) {
                    instance = Expression.Property(pStateManager, typeof(StateManager).GetProperty("StateMachine"));
                }

                var call = Expression.Call(
                    Expression.Convert(pCommand, Command.GetType()),
                    method,
                    Expression.Convert(instance, stateMachineType));
                applyMethod = Expression.Lambda(Expression.Block(call), pCommand, pStateManager).Compile() as Action<ICommand, StateManager>;
            }

            applyMethod(Command, manager );
        }
    }
}
