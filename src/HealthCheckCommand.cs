using System;
using System.IO;

namespace NRaft
{


    public class HealthCheckCommand : Command<IStateMachine>
    {
        public static readonly int COMMAND_ID = StateMachine.COMMAND_ID_HEALTH_CHECK;

        private static Random random = new Random();

        private long val;


        public HealthCheckCommand()
        {
            val = random.Next();
        }

        public void applyTo(IStateMachine state)
        {
            state.applyHealthCheck(val);
        }

        public void write(BinaryWriter writer) { writer.Write(val); }

        public void read(BinaryReader reader, int fileVersion) { this.val = reader.ReadInt64(); }

        public int getCommandType()
        {
            return COMMAND_ID;
        }
    }
}
