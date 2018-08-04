using System;
using System.Collections.Generic;

namespace NRaft {
    internal class LRUCache<T> where T : StateMachine<T>
    {
        private Dictionary<string, Node> values = new Dictionary<string, Node>();
        private Node first;
        private Node last;
        private int maxSize = 100;

        class Node {
            public string Key;
            public Node Next;
            public List<Entry<T>> Value;
        }

        internal List<Entry<T>> Get(string file)
        {
            values.TryGetValue(file, out Node val);
            return val?.Value;
        }

        internal void Clear()
        {
            values.Clear();
        }

        internal void Add(string file, List<Entry<T>> list)
        {
            lock (values)
            {
                if( values.ContainsKey(file))
                    return;
                    
                var node = new Node {Key=file, Value = list };
                if(last!=null)
                    last.Next = node;
                last = node;
                values.Add(file, node);

                if(first == null)
                    first = node;
                
                if( values.Count > maxSize) {
                    RemoveOldest();
                }
            }
        }

        private void RemoveOldest()
        {
            var tmp = first;
            first = first.Next;
            values.Remove(tmp.Key);
        }
    }
}