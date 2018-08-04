using System;
using System.IO;

namespace NRaft
{

    public class PendingCommand<T> : IComparable<PendingCommand<T>> 
    {
        public Entry<T> entry;
        public ClientResponseHandler<T> handler;

        public PendingCommand(Entry<T> entry, ClientResponseHandler<T> handler)
        {
            this.entry = entry;
            this.handler = handler;
        }

        public int CompareTo(PendingCommand<T> other)
        {
            var index = entry.index;
            return (int)(index - other.entry.index);
        }
    }
}