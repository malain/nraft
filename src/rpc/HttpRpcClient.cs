using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Newtonsoft.Json;

namespace NRaft
{
    public class RaftMiddleware
    {
        private readonly RequestDelegate _next;
        private readonly IRaftListener _rpc;

        public RaftMiddleware(RequestDelegate next, IRaftListener rpc)
        {
            _next = next;
            _rpc = rpc;
        }

        public async Task Invoke(HttpContext context)
        {
            if (context.Request.Path.Value.StartsWith("/cluster"))
            {
                await _next(context);
                return;
            }

            RequestMessage message = null;
            using (var reader = new StreamReader(context.Request.Body, Encoding.UTF8))
            {
                var str = await reader.ReadToEndAsync();
                message = JsonConvert.DeserializeObject<RequestMessage>(str);
                var response = await _rpc.HandleMessage(message);
                context.Response.ContentType = "application/json";
                context.Response.StatusCode = 200;
                await context.Response.WriteAsync(JsonConvert.SerializeObject(response));
            }
        }
    }

    public class HttpRpcClient : IRpcSender
    {
        private HttpClient client = new HttpClient();

        public async Task<ResponseMessage> SendMessage(PeerInfo peer, RequestMessage msg)
        {
            var request = new HttpRequestMessage(HttpMethod.Post, peer.address + "/cluster");
            request.Content = new StringContent(Newtonsoft.Json.JsonConvert.SerializeObject(msg), Encoding.UTF8, "application/json");
            var response = await client.SendAsync(request);
            if (!response.IsSuccessStatusCode)
                return null;

            var content = await response.Content.ReadAsStringAsync();
            return Newtonsoft.Json.JsonConvert.DeserializeObject<ResponseMessage>(content);
        }
    }

    public class LocalRpcClient : IRpcSender
    {
        private 
        public async Task<ResponseMessage> SendMessage(PeerInfo peer, RequestMessage msg)
        {
            var request = new HttpRequestMessage(HttpMethod.Post, peer.address + "/cluster");
            request.Content = new StringContent(Newtonsoft.Json.JsonConvert.SerializeObject(msg), Encoding.UTF8, "application/json");
            var response = await client.SendAsync(request);
            if (!response.IsSuccessStatusCode)
                return null;

            var content = await response.Content.ReadAsStringAsync();
            return Newtonsoft.Json.JsonConvert.DeserializeObject<ResponseMessage>(content);
        }
    }
}
