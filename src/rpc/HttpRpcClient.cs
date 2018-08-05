using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace NRaft {
    public class HttpRpcClient : IRpcSender
    {
        private HttpClient client;

        public async Task<ResponseMessage> SendMessage(PeerInfo peer, RequestMessage msg)
        {
            var request = new HttpRequestMessage(HttpMethod.Post, $"http://{peer.host}:{peer.port}/api/cluster");
            request.Content = new StringContent(Newtonsoft.Json.JsonConvert.SerializeObject(msg), Encoding.UTF8, "application/json");
            var response = await client.SendAsync(request);
            var content = await response.Content.ReadAsStringAsync();
            return Newtonsoft.Json.JsonConvert.DeserializeObject<ResponseMessage>(content);
        }
    }
}