using System.IO;
using Microsoft.Extensions.Logging;

namespace NRaft
{

    public class RaftUtil
    {

        public static readonly ILogger logger = LoggerFactory.GetLogger<RaftUtil>();

        public static byte[] getFilePart(string file, int offset, int len)
        {
            byte[] data = new byte[len];
            try
            {
                using (var stream = File.OpenRead(file))
                {
                    using (var raf = new BinaryReader(stream))
                    {
                        stream.Seek(offset, SeekOrigin.Begin);
                        data = raf.ReadBytes(len);
                        return data;
                    }
                }
            }
            catch (IOException e)
            {
                logger.LogError(e.Message);
            }
            return null;
        }

        public static byte[] toBytes(long value)
        {
            return new byte[] { (byte) (value >> 56), (byte) (value >> 48), (byte) (value >> 40), (byte) (value >> 32),
            (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8), (byte) value };
        }

        public static long toLong(byte[] data)
        {
            return (((long)data[0] << 56) + ((long)(data[1] & 255) << 48) + ((long)(data[2] & 255) << 40) + ((long)(data[3] & 255) << 32)
                  + ((long)(data[4] & 255) << 24) + ((data[5] & 255) << 16) + ((data[6] & 255) << 8) + ((data[7] & 255) << 0));
        }
    }
}