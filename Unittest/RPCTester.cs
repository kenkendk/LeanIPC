using System;
using System.Threading.Tasks;
using LeanIPC;
using NUnit.Framework;

namespace Unittest
{
    [TestFixture]
    public class RPCTester
    {
        private static Task<RequestHandlerResponse> FailMethod(ParsedMessage message)
        {
            throw new Exception("Did not expect a user message");    
        }

        [Test()]
        public void RemoteInvoke()
        {
            using (var setup = new ClientServerTester(FailMethod, FailMethod))
            {
                var client = new RPCPeer(setup.Client);
                var server = new RPCPeer(setup.Server, new Type[] { typeof(System.IO.Directory) }, (m, a) => m.Name == nameof(System.IO.Directory.GetCurrentDirectory));

                var result = client.InvokeRemoteMethodAsync<string>(0L, typeof(System.IO.Directory).GetMethod(nameof(System.IO.Directory.GetCurrentDirectory)), null, false).Result;
                if (result != System.IO.Directory.GetCurrentDirectory())
                    throw new Exception("Failed to get the correct ");

                Exception res = null;
                // Try unsupported type:
                try
                {
                    client.InvokeRemoteMethodAsync(0, typeof(System.IO.File).GetMethod(nameof(System.IO.File.Create)), new object[] { "test.txt" }, false).Wait();
                }
                catch(Exception ex)
                {
                    res = ex;
                }

                if (res == null)
                    throw new Exception("Unwanted access to other type");

                // Try other method
                res = null;
                try
                {
                    client.InvokeRemoteMethodAsync(0, typeof(System.IO.Directory).GetMethod(nameof(System.IO.Directory.CreateDirectory)), new object[] { "testdir" }, false).Wait();
                }
                catch (Exception ex)
                {
                    res = ex;
                }

                if (res == null)
                    throw new Exception("Unwanted access to other method");

                // Try invoking on the client
                res = null;
                try
                {
                    server.InvokeRemoteMethodAsync<string>(0, typeof(System.IO.Directory).GetMethod(nameof(System.IO.Directory.GetCurrentDirectory)), null, false).Wait();
                }
                catch(Exception ex)
                {
                    res = ex;
                }

                if (res == null)
                    throw new Exception("Unwanted access to client method");
            }
        }

        private interface IRemoteInvokeAble
        {
            string Echo(string message);
            long ID { get; set; }
        }

        private class RemoteInvokeAble : IRemoteInvokeAble
        {
            public string Echo(string message)
            {
                return "Echo : " + message;
            }

            public long ID { get; set; } = 42;

            public long CreateTime = DateTime.Now.Ticks;
        }

        private interface IRemoteInvokeAbleProxy : IRemoteInvokeAble, IDisposable
        {
            long CreateTime { get; set; }
        }

        private class RemoteInvokeAbleProxy : RemoteObject, IRemoteInvokeAbleProxy
        {
            public RemoteInvokeAbleProxy(RPCPeer peer, long handle)
                : base(peer, typeof(RemoteInvokeAble), handle)
            {
            }

            public string Echo(string message)
            {
                return m_peer.InvokeRemoteMethodAsync<string>(m_handle, typeof(RemoteInvokeAble).GetMethod(nameof(RemoteInvokeAble.Echo)), new object[] { message }, false).Result;
            }

            public long ID
            {
                get { return m_peer.InvokeRemoteMethodAsync<long>(m_handle, typeof(RemoteInvokeAble).GetProperty(nameof(RemoteInvokeAble.ID)), null, false).Result; }
                set { m_peer.InvokeRemoteMethodAsync(m_handle, typeof(RemoteInvokeAble).GetProperty(nameof(RemoteInvokeAble.ID)), new object[] { value }, true).Wait(); }
            }

            public long CreateTime
            {
                get { return m_peer.InvokeRemoteMethodAsync<long>(m_handle, typeof(RemoteInvokeAble).GetField(nameof(RemoteInvokeAble.CreateTime)), null, false).Result; }
                set { m_peer.InvokeRemoteMethodAsync(m_handle, typeof(RemoteInvokeAble).GetField(nameof(RemoteInvokeAble.CreateTime)), new object[] { value }, true).Wait(); }
            }
        }

        [Test()]
        public void RemoteReferences()
        {
            using (var setup = new ClientServerTester(FailMethod, FailMethod))
            {
                var client = new RPCPeer(setup.Client);
                var server = new RPCPeer(setup.Server, typeof(RemoteInvokeAble));

                // Let the client create proxy objects
                client.AddProxyGenerator((peer, type, handle) => new RemoteInvokeAbleProxy(peer, handle));

                // Let the server pass the object by reference
                server.TypeSerializer.RegisterSerializationAction(typeof(RemoteInvokeAble), SerializationAction.Reference);

                // Run the tests
                using (var proxy = client.InvokeRemoteMethodAsync<IRemoteInvokeAbleProxy>(0, typeof(RemoteInvokeAble).GetConstructor(new Type[0]), null, false).Result)
                {
                    if (proxy.Echo("test message") != "Echo : test message")
                        throw new Exception("Expected echo message back");

                    if (proxy.ID != 42)
                        throw new Exception("Expected 42 as the ID");

                    proxy.ID = 45;
                    if (proxy.ID != 45)
                        throw new Exception("Expected 45 as the ID");

                    if (proxy.CreateTime == 0)
                        throw new Exception("Expected a proxy creation time");

                    proxy.CreateTime = 0;
                    if (proxy.CreateTime != 0)
                        throw new Exception("Expected no proxy creation time");
                }
            }
        }

        public interface IRemoteInt
        {
            int Value { get; set; }

            IRemoteInt Add(IRemoteInt other);
        }

        public class RemoteInt : IRemoteInt
        {
            public RemoteInt(int value)
            {
                Value = value;
            }

            public int Value { get; set; }

            public IRemoteInt Add(IRemoteInt other)
            {
                return new RemoteInt(this.Value + other.Value);
            }
        }

        public interface IRemoteIntProxy : IRemoteInt, IRemoteInstance, IDisposable
        {
        }

        public class RemoteIntProxy : RemoteObject, IRemoteIntProxy
        {
            public RemoteIntProxy(RPCPeer peer, long handle)
                : base(peer, typeof(RemoteInt), handle)
            {
            }

            public int Value 
            { 
                get => HandleInvokePropertyGet<int>(nameof(RemoteInt.Value), null, null);
                set => HandleInvokePropertySet(nameof(RemoteInt.Value), value, null, null);
            }

            public IRemoteInt Add(IRemoteInt other)
            {
                return HandleInvokeMethod<IRemoteInt>(nameof(RemoteInt.Add), new Type[] { typeof(IRemoteInt) }, new object[] { other });
            }
        }

        [Test()]
        public void RemoteTwoWayRemote()
        {
            using (var setup = new ClientServerTester(FailMethod, FailMethod))
            {
                var client = new RPCPeer(setup.Client, typeof(RemoteInt));
                var server = new RPCPeer(setup.Server, typeof(RemoteInt));

                // Let the client/server create proxy objects
                client.AddProxyGenerator((peer, type, handle) => new RemoteIntProxy(peer, handle));
                server.AddProxyGenerator((peer, type, handle) => new RemoteIntProxy(peer, handle));

                // Let the client/server pass the object by reference
                client.TypeSerializer.RegisterSerializationAction(typeof(RemoteInt), SerializationAction.Reference);
                server.TypeSerializer.RegisterSerializationAction(typeof(RemoteInt), SerializationAction.Reference);

                // Create a remote object
                using (var serverInt = client.InvokeRemoteMethodAsync<IRemoteIntProxy>(0, typeof(RemoteInt).GetConstructor(new Type[] { typeof(int) }), new object[] { 4 }, false).Result)
                {
                    // Invoke the remote side with a local object
                    var res = serverInt.Add(new RemoteInt(6));

                    // Check the result
                    if (res.Value != 10)
                        throw new Exception("Expected addition result");
                }
            }
        }

        [Test]
        public void TestAutomaticProxy()
        {
            using (var setup = new ClientServerTester(FailMethod, FailMethod))
            {
                var client = new RPCPeer(setup.Client, typeof(RemoteInt));
                var server = new RPCPeer(setup.Server, typeof(RemoteInt));

                // Let the client/server pass the object by reference
                client.TypeSerializer.RegisterSerializationAction(typeof(RemoteInt), SerializationAction.Reference);
                server.TypeSerializer.RegisterSerializationAction(typeof(RemoteInt), SerializationAction.Reference);

                // Register automatic proxy generation
                client.AddAutomaticProxy(typeof(RemoteInt), typeof(IRemoteIntProxy));
                server.AddAutomaticProxy(typeof(RemoteInt), typeof(IRemoteIntProxy));

                // Create a remote object
                using(var serverInt = client.CreateAsync<IRemoteIntProxy>(typeof(RemoteInt), 4).Result)
                {
                    // Invoke the remote side with a local object
                    var res = serverInt.Add(new RemoteInt(6));

                    // Check the result
                    if (res.Value != 10)
                        throw new Exception("Expected addition result");
                }
            }
        }

        public interface IIndiceAdder
        {
            int this[int a, int b]
            {
                get;
                set;
            }

            int V { get; }
        }

        public class IndiceAdder : IIndiceAdder
        {
            private int m_v;

            public int this[int a, int b]
            {
                get => a + b;
                set => m_v = a + b + value;
            }

            public int V => m_v;
        }

        public interface IIndiceAdderProxy : IIndiceAdder, IDisposable
        {
        }

        public class IndiceAdderManual : RemoteObject, IIndiceAdder
        {
            public IndiceAdderManual(RPCPeer peer, Type type, long handle)
                : base(peer, type, handle)
            {
            }

            public int this[int a, int b] 
            {
                get => HandleInvokePropertyGet<int>("Item", new Type[] { typeof(int), typeof(int) }, new object[] { a, b });
                set => HandleInvokePropertySet("Item", value, new Type[] { typeof(int), typeof(int) }, new object[] { a, b }); 
            }

            public int V => HandleInvokePropertyGet<int>(nameof(V), null, null);
        }

        [Test]
        public void TestPropertyIndices()
        {
            using (var setup = new ClientServerTester(FailMethod, FailMethod))
            {
                var client = new RPCPeer(setup.Client, typeof(IndiceAdder));
                var server = new RPCPeer(setup.Server, typeof(IndiceAdder));

                // The server can send back adders
                server.TypeSerializer.RegisterSerializationAction(typeof(IndiceAdder), SerializationAction.Reference);

                // The client can receive adders
                client.AddAutomaticProxy(typeof(IndiceAdder), typeof(IIndiceAdderProxy));

                // Create a remote object
                using (var serverInt = client.CreateAsync<IIndiceAdderProxy>(typeof(IndiceAdder)).Result)
                {
                    var res = serverInt[4, 5];
                    if (res != 9)
                        throw new Exception("Expected 9 from the addition");

                    serverInt[2, 3] = 5;
                    var v = serverInt.V;
                    if (v != 10)
                        throw new Exception("Expected 10 from the addition");
                }
            }
        }

        /// <summary>
        /// Interface that calls remote asyncronous methods
        /// </summary>
        public interface IRemoteAsyncBasedObject : IDisposable
        {
            Task SetValueAsync(int value);
            Task<int> ValueAsync { get; }
            Task<int> GetValueAsync();
        }

        /// <summary>
        /// Interface that calls remote synchronous methods,
        /// but wraps them locally as async methods
        /// </summary>
        public interface ILocalAsyncBasedObject : IDisposable
        {
            Task SetValue(int value);
            Task<int> Value { get; }
            Task<int> GetValue();
        }

        /// <summary>
        /// Interface that calls remote asyncronous methods,
        /// but wraps the results locally
        /// </summary>
        public interface ILocalSyncBasedObject : IDisposable
        {
            void SetValueAsync(int value);
            int ValueAsync { get; }
            int GetValueAsync();
        }

        /// <summary>
        /// The remote instance with both sync and async methods
        /// </summary>
        public class TaskBasedObject
        {
            private int m_value;

            public void SetValue(int value)
            {
                m_value = value;
            }

            public int GetValue()
            {
                return m_value;
            }

            public Task SetValueAsync(int value)
            {
                m_value = value;
                return Task.FromResult(true);
            }

            public Task<int> GetValueAsync()
            {
                return Task.FromResult(m_value);
            }

            public Task<int> ValueAsync { get { return Task.FromResult(m_value); } }

            public int Value => m_value;

        }

        public class TaskBasedRemoteAsync : RemoteObject, IRemoteAsyncBasedObject
        {
            public TaskBasedRemoteAsync(RPCPeer peer, Type type, long handle)
                : base(peer, type, handle)
            {
            }

            public Task<int> ValueAsync => HandleInvokePropertyGetAsync<int>(nameof(ValueAsync), null, null);

            public Task SetValueAsync(int value)
            {
                return HandleInvokeMethodAsync(nameof(SetValueAsync), new Type[] { typeof(int) }, new object[] { value });
            }

            public Task<int> GetValueAsync()
            {
                return HandleInvokeMethodAsync<int>(nameof(GetValueAsync), null, null);
            }
        }

        public class TaskBasedLocalAsync : RemoteObject, ILocalAsyncBasedObject
        {
            public TaskBasedLocalAsync(RPCPeer peer, Type type, long handle)
                : base(peer, type, handle)
            {
            }

            public Task<int> Value => HandleInvokePropertyGetAsync<int>(nameof(Value), null, null);

            public Task SetValue(int value)
            {
                return HandleInvokeMethodAsync(nameof(SetValue), new Type[] { typeof(int) }, new object[] { value });
            }

            public Task<int> GetValue()
            {
                return HandleInvokeMethodAsync<int>(nameof(GetValue), null, null);
            }

        }

        public class TaskBasedLocalSync : RemoteObject, ILocalSyncBasedObject
        {
            public TaskBasedLocalSync(RPCPeer peer, Type type, long handle)
                : base(peer, type, handle)
            {
            }

            public int ValueAsync => HandleInvokePropertyGetAsync<int>(nameof(ValueAsync), null, null).Result;

            public void SetValueAsync(int value)
            {
                HandleInvokeMethodAsync(nameof(SetValueAsync), new Type[] { typeof(int) }, new object[] { value }).Wait();
            }

            public int GetValueAsync()
            {
                return HandleInvokeMethodAsync<int>(nameof(GetValueAsync), null, null).Result;
            }
        }


        [Test]
        public void TestAsyncManualSupport()
        {
            using (var setup = new ClientServerTester(FailMethod, FailMethod))
            {
                var client = new RPCPeer(setup.Client);
                var server = new RPCPeer(setup.Server, typeof(TaskBasedObject));

                // The server can send back the task object
                server.TypeSerializer.RegisterSerializationAction(typeof(TaskBasedObject), SerializationAction.Reference);

                // Setup our bait-n-switch type
                var rp = typeof(TaskBasedRemoteAsync);

                // Register generating the local proxy type
                client.AddProxyGenerator((peer, type, handle) => (IRemoteInstance)Activator.CreateInstance(rp, peer, type, handle));

                // Create a remote object
                using (var proxy = client.CreateAsync<IRemoteAsyncBasedObject>(typeof(TaskBasedObject)).Result)
                {
                    proxy.SetValueAsync(33).Wait();
                    var res = proxy.ValueAsync.Result;
                    if (res != 33)
                        throw new Exception("Expected 33");
                    res = proxy.GetValueAsync().Result;
                    if (res != 33)
                        throw new Exception("Expected 33");
                }

                // Switch type
                rp = typeof(TaskBasedLocalAsync);

                // Run tests on the new interface
                using (var proxy = client.CreateAsync<ILocalAsyncBasedObject>(typeof(TaskBasedObject)).Result)
                {
                    proxy.SetValue(68).Wait();
                    var res = proxy.Value.Result;
                    if (res != 68)
                        throw new Exception("Expected 68");
                    res = proxy.GetValue().Result;
                    if (res != 68)
                        throw new Exception("Expected 68");
                }

                // Switch type
                rp = typeof(TaskBasedLocalSync);

                // Run tests on the new interface
                using (var proxy = client.CreateAsync<ILocalSyncBasedObject>(typeof(TaskBasedObject)).Result)
                {
                    proxy.SetValueAsync(76);
                    var res = proxy.ValueAsync;
                    if (res != 76)
                        throw new Exception("Expected 76");
                    res = proxy.GetValueAsync();
                    if (res != 76)
                        throw new Exception("Expected 76");
                }
            }
        }
        [Test]
        public void TestAsyncAutomaticSupport()
        {
            using (var setup = new ClientServerTester(FailMethod, FailMethod))
            {
                var client = new RPCPeer(setup.Client);
                var server = new RPCPeer(setup.Server, typeof(TaskBasedObject));

                // The server can send back the task object
                server.TypeSerializer.RegisterSerializationAction(typeof(TaskBasedObject), SerializationAction.Reference);

                // Register a specific interface
                client.AddAutomaticProxy(typeof(TaskBasedObject), typeof(IRemoteAsyncBasedObject));

                // Create a remote object
                using (var proxy = client.CreateAsync<IRemoteAsyncBasedObject>(typeof(TaskBasedObject)).Result)
                {
                    proxy.SetValueAsync(33).Wait();
                    var res = proxy.ValueAsync.Result;
                    if (res != 33)
                        throw new Exception("Expected 33");
                    res = proxy.GetValueAsync().Result;
                    if (res != 33)
                        throw new Exception("Expected 33");
                }

                // Register a new specific interface
                client.RemoveAutomaticProxy(typeof(TaskBasedObject));
                client.AddAutomaticProxy(typeof(TaskBasedObject), typeof(ILocalAsyncBasedObject));

                // Run tests on the new interface
                using (var proxy = client.CreateAsync<ILocalAsyncBasedObject>(typeof(TaskBasedObject)).Result)
                {
                    proxy.SetValue(68).Wait();
                    var res = proxy.Value.Result;
                    if (res != 68)
                        throw new Exception("Expected 68");
                    res = proxy.GetValue().Result;
                    if (res != 68)
                        throw new Exception("Expected 68");
                }

                // Register a new specific interface
                client.RemoveAutomaticProxy(typeof(TaskBasedObject));
                client.AddAutomaticProxy(typeof(TaskBasedObject), typeof(ILocalSyncBasedObject));

                // Run tests on the new interface
                using (var proxy = client.CreateAsync<ILocalSyncBasedObject>(typeof(TaskBasedObject)).Result)
                {
                    proxy.SetValueAsync(78);
                    var res = proxy.ValueAsync;
                    if (res != 78)
                        throw new Exception("Expected 78");
                    res = proxy.GetValueAsync();
                    if (res != 78)
                        throw new Exception("Expected 78");
                }
            }
        }
    }
}
