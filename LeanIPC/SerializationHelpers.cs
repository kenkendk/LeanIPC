using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;

namespace LeanIPC
{
    public static class SerializationHelpers
    {
        /// <summary>
        /// Serializes an <see cref="IPEndPoint"/>
        /// </summary>
        /// <returns>The serialization representation.</returns>
        /// <param name="sourceType">The data type to serialize.</param>
        /// <param name="instance">The object to serialize.</param>
        public static Tuple<Type[], object[]> SerializeIPEndPoint(Type sourceType, object instance)
        {
            var addr = instance == null ? null : ((IPEndPoint)instance).Address.ToString();
            var port = instance == null ? 0 : ((IPEndPoint)instance).Port;

            return new Tuple<Type[], object[]>(
                new Type[] { typeof(string), typeof(int) },
                new object[] { addr, port }
            );
        }

        /// <summary>
        /// Serializes an <see cref="EndPoint"/>
        /// </summary>
        /// <returns>The serialization representation.</returns>
        /// <param name="sourceType">The data type to serialize.</param>
        /// <param name="instance">The object to serialize.</param>
        public static Tuple<Type[], object[]> SerializeEndPoint(Type sourceType, object instance)
        {
            // Special handling for IP addresses
            if (instance != null && instance is IPEndPoint)
                return SerializeIPEndPoint(sourceType, instance);

            byte[] buf = null;
            if (instance != null)
            {
                var sockaddr = ((EndPoint)instance).Serialize();
                buf = new byte[sockaddr.Size + 1];
                buf[0] = (byte)sockaddr.Family;
                for (var i = 0; i < buf.Length; i++)
                    buf[i + 1] = sockaddr[i];
            }

            return new Tuple<Type[], object[]>(
                new Type[] { typeof(byte[]) },
                new object[] { buf });
        }

        /// <summary>
        /// Deserializes an <see cref="IPEndPoint"/>
        /// </summary>
        /// <returns>The <see cref="IPEndPoint"/>.</returns>
        /// <param name="sourceType">The source type.</param>
        /// <param name="args">The serialized arguments.</param>
        public static object DeserializeIPEndPoint(Type sourceType, object[] args)
        {
            if (args == null || (args.Length >= 1 && args[0] == null))
                return null;

            return new IPEndPoint(IPAddress.Parse((string)args[0]), (int)args[1]);
        }

        /// <summary>
        /// Deserializes an <see cref="EndPoint"/>
        /// </summary>
        /// <returns>The <see cref="EndPoint"/>.</returns>
        /// <param name="sourceType">The source type.</param>
        /// <param name="args">The serialized arguments.</param>
        public static object DeserializeEndPoint(Type sourceType, object[] args)
        {
            if (args == null || (args.Length >= 1 && args[0] == null))
                return null;

            // Special handling for IP addresses
            if (args.Length == 2)
                return DeserializeIPEndPoint(sourceType, args);

            if (args == null || args.Length != 1 || !(args[0] is byte[]))
                throw new ArgumentException("Expected a single byte array for the endpoint");
            var data = (byte[])args[0];
            var sockaddr = new SocketAddress(
                (AddressFamily)data[0],
                data.Length - 1
            );

            for (var i = 0; i < data.Length - 1; i++)
                sockaddr[i] = data[i + 1];

            // TODO: Maybe figure out which class to use?
            return new IPEndPoint(0, 0).Create(sockaddr);
        }

        /// <summary>
        /// Registers a custom serializer/deserializer for <see cref="EndPoint"/>
        /// </summary>
        /// <param name="serializer">The type serializer.</param>
        public static void RegisterEndPointSerializers(this TypeSerializer serializer)
        {
            serializer.RegisterCustomSerializer(typeof(EndPoint), SerializeEndPoint, DeserializeEndPoint);
        }

        /// <summary>
        /// Registers a custom serializer/deserializer for <see cref="EndPoint"/>
        /// </summary>
        /// <param name="peer">The peer to register on.</param>
        /// <returns>The peer instance</returns>
        public static RPCPeer RegisterEndPointSerializers(this RPCPeer peer)
        {
            RegisterEndPointSerializers(peer.TypeSerializer);
            return peer;
        }

        /// <summary>
        /// Registers a custom serializer/deserializer for <see cref="IPEndPoint"/>
        /// </summary>
        /// <param name="serializer">The type serializer.</param>
        public static void RegisterIPEndPointSerializers(this TypeSerializer serializer)
        {
            serializer.RegisterCustomSerializer(typeof(IPEndPoint), SerializeIPEndPoint, DeserializeIPEndPoint);
        }

        /// <summary>
        /// Registers a custom serializer/deserializer for <see cref="IPEndPoint"/>
        /// </summary>
        /// <param name="peer">The peer to register on.</param>
        /// <returns>The peer instance</returns>
        public static RPCPeer RegisterIPEndPointSerializers(this RPCPeer peer)
        {
            RegisterIPEndPointSerializers(peer.TypeSerializer);
            return peer;
        }

        /// <summary>
        /// Registers a custom serializer that decomposes an interface into the properties
        /// </summary>
        /// <param name="serializer">The serializer to register on.</param>
        /// <param name="filter">An optional filter for the properties</param>
        /// <typeparam name="T">The type to register the custom serializer for.</typeparam>
        public static void RegisterPropertyDecomposer<T>(this TypeSerializer serializer, Func<System.Reflection.PropertyInfo, bool> filter = null)
        {
            filter = filter ?? (x => true);
            var fields = typeof(T)
                .GetProperties(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.FlattenHierarchy | System.Reflection.BindingFlags.Public)
                .Where(x => x.GetIndexParameters().Length == 0)
                .Where(filter)
                .ToArray();

            var types = new Type[] { typeof(string[]) }.Concat(fields.Select(x => x.PropertyType)).ToArray();
            var names = fields.Select(x => x.Name).ToArray();

            serializer.RegisterCustomSerializer(
                typeof(T),
                (a, b) => new Tuple<Type[], object[]>(
                    types,
                    new object[] { names }.Concat(fields.Select(x => x.GetValue(b, null))).ToArray()
                ),
                (a, b) => AutomaticProxy.WrapPropertyDecomposedInstance(null, a, typeof(T), b)
            );
        }

        /// <summary>
        /// Registers a custom serializer that decomposes an interface into the properties
        /// </summary>
        /// <param name="peer">The peer to register on.</param>
        /// <param name="filter">An optional filter for the properties</param>
        /// <typeparam name="T">The type to register the custom serializer for.</typeparam>
        public static RPCPeer RegisterPropertyDecomposer<T>(this RPCPeer peer, Func<System.Reflection.PropertyInfo, bool> filter = null)
        {
            RegisterPropertyDecomposer<T>(peer.TypeSerializer, filter);
            return peer;
        }

        ///// <summary>
        ///// Register streams as being served locally (passed by reference to the remote)
        ///// </summary>
        ///// <returns>The peer instance</returns>
        ///// <param name="peer">Peer.</param>
        //public static RPCPeer RegisterLocallyServedStream(this RPCPeer peer)
        //{
        //    return 
        //        peer

        //        // Convert any stream to the interface type
        //        //peer.RegisterCustomSerializer<System.IO.Stream>(
        //        //    x => new Tuple<Type[], object[]>(
        //        //        new Type[] { typeof(StreamSupport.IRemoteStream) },
        //        //        new object[] { new StreamSupport.LocalStreamProxy((System.IO.Stream)x) } )
        //        //)

        //        // And make sure we allow calls on the stream interface, and pass it by reference
        //        .AddPreSendHook(async (arg, type) => {
        //            if (arg is System.IO.Stream s)
        //            {
        //                // TODO: This is really dirty... we register multiple instances with
        //                // the same ID and do not clean up old references

        //                // Check if the stream is already registered
        //                if (await peer.RemoteHandler.RegisterLocalObjectAsync(arg))
        //                {
        //                    var id = peer.RemoteHandler.GetLocalHandle(arg);

        //                    // Hook up the wrapper to the ID
        //                    peer.RemoteHandler.MonkeySetLocalObject(new StreamSupport.LocalStreamProxy(s), id);

        //                    // Manually register the handle as a custom type
        //                    await peer.IPC.SendPassthroughAsync(Command.RegisterRemoteObject, new RegisterRemoteObjectRequest(typeof(StreamSupport.IRemoteStream), id));
        //                }
        //            }
        //        })

        //        .RegisterByRefType<System.IO.Stream>()
        //        .RegisterLocallyServedType<StreamSupport.IRemoteStream>();
        //}

        ///// <summary>
        ///// Registers a proxy for a remote stream reference.
        ///// </summary>
        ///// <returns>The peer instance</returns>
        ///// <param name="peer">Peer.</param>
        //public static RPCPeer RegisterProxyForRemoteStream(this RPCPeer peer)
        //{
        //    return peer
        //        .AddProxyGenerator((p, type, id) =>
        //            new StreamSupport.RemoteStreamProxy(
        //                (StreamSupport.IRemoteStream)AutomaticProxy.WrapRemote(p, type, typeof(StreamSupport.IRemoteStream), id)
        //            )
        //        );

        //}
    }
}
