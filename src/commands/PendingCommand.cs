using System;
using System.IO;

namespace NRaft
{

    public class PendingCommand : IComparable<PendingCommand> 
    {
        public Entry entry { get; private set; }
        public ClientResponseHandler handler { get; private set; }

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