using System;
using System.IO;

namespace NRaft
{


    public class HealthCheckCommand : Command<IInternalStateMachine>
    {
        public int CommandId => StateManager.COMMAND_ID_HEALTH_CHECK;

        private static Random random = new Random();

        private long val;


        public HealthCheckCommand()
        {
            val = random.Next();
        }

        public void ApplyTo(IInternalStateMachine state)
        {
            state.ApplyHealthCheck(val);
        }

        public void Serialize(BinaryWriter writer) { writer.Write(val); }

        public void Deserialize(BinaryReader reader, int fileVersion) { this.val = reader.ReadInt64(); }
    }
}
