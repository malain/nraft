using System;
using System.IO;

namespace NRaft
{

    public class PendingCommand : IComparable<PendingCommand> 
    {
        public Entry entry;
        public ClientResponseHandler handler;

        public PendingCommand(Entry entry, ClientResponseHandler handler)
        {
            this.entry = entry;
            this.handler = handler;
        }

        public int CompareTo(PendingCommand other)
        {
            var index = entry.Index;
            return (int)(index - other.entry.Index);
        }
    }
}