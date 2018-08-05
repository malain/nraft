using System.IO;
using Microsoft.Extensions.Logging;

namespace NRaft
{

    public class RaftUtil
    {

        public static readonly ILogger logger = LoggerFactory.GetLogger<RaftUtil>();

        public static byte[] GetFilePart(string file, int offset, int len)
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
    }
}