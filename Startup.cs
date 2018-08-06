using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;

namespace NRaft
{
    public class Startup
    {
        public static int peerId;

        // This method gets called by the runtime. Use this method to add services to the container.
        // For more information on how to configure your application, visit https://go.microsoft.com/fwlink/?LinkID=398940
        public void ConfigureServices(IServiceCollection services)
        {
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env)
        {                    
            Config cfg = new Config(peerId)
                    .WithPeer(1, "http://localhost:9999/")
                    .WithPeer(2, "http://localhost:9998/")
                    .WithClusterName("TEST");

                for (int j = 0; j < 1; j++)
                {
                 //   cfg.AddPeer(j + 1);
                }

            var node = new RaftNode<TestStateMachine>(cfg);
            node.UseMiddleware(app);

            node.Start();
        }
    }
}
