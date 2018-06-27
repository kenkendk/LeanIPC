using System;
using System.IO;
using System.Threading.Tasks;
using LeanIPC;

namespace Unittest
{
    public class ClientServerTester : IDisposable
    {
        public readonly Stream S1;
        public readonly Stream S2;

        public readonly InterProcessConnection Server;
        public readonly InterProcessConnection Client;

        public readonly Task ServerTask;
        public readonly Task ClientTask;


        public ClientServerTester(RequestHandler servercallback, RequestHandler clientcallback)
        {
            S1 = new BlockingPipeStream("serverInput/clientOutput");
            S2 = new BlockingPipeStream("clientInput/serverOutput");

            Server = new InterProcessConnection(S1, S2);
            Client = new InterProcessConnection(S2, S1);

            ServerTask = Task.Run(() => Server.RunMainLoopAsync(servercallback, false));
            ClientTask = Task.Run(() => Client.RunMainLoopAsync(clientcallback, true));
        }

        public void Dispose()
        {
            var cs = Client.ShutdownAsync();
            var ss = Server.ShutdownAsync();

            while (!cs.Wait(5000))
                Console.WriteLine("  *** Waiting for client shutdown call");
            while (!ss.Wait(5000))
                Console.WriteLine("  *** Waiting for server shutdown call");

            var allt = Task.WhenAll(
                cs,
                ss,
                ServerTask,
                ClientTask
            );

            while(!allt.Wait(5000))
            {
                Console.WriteLine();                
            }

        }

        public async Task<T> GuardedOperationAsync<T>(Func<Task<T>> method)
        {
            var runTask = method();
            var firstDone = await Task.WhenAny(ServerTask, ClientTask, runTask);
            if (firstDone != runTask)
            {
                // Fire the exception here if any
                await firstDone;

                // Or report this
                throw new Exception("Task did not complete, but client or server stopped");
            }

            return await runTask;
        }

    }
}
