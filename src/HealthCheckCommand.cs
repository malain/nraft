using System;
using System.IO;

namespace NRaft
{


    public class HealthCheckCommand : Command
    {
        public int CommandId => StateManager.COMMAND_ID_HEALTH_CHECK;

        private static Random random = new Random();

        private long val;


        public HealthCheckCommand()
        {
            val = random.Next();
        }

        public void applyTo(object state)
        {
            ((StateManager)state).applyHealthCheck(val);
        }

        public void write(BinaryWriter writer) { writer.Write(val); }

        public void read(BinaryReader reader, int fileVersion) { this.val = reader.ReadInt64(); }
    }
}
