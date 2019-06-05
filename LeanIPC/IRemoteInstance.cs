using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace LeanIPC
{
    /// <summary>
    /// Interface for implementing a proxied reference
    /// </summary>
    public interface IRemoteInstance
    {
        /// <summary>
        /// Gets the remote handle.
        /// </summary>
        long Handle { get; }

        /// <summary>
        /// Detaches the handle
        /// </summary>
        /// <param name="suppressPeerCall">If set to <c>true</c> does not call the peer, but assumes this has already been done.</param>
        Task Detach(bool suppressPeerCall);
    }

    /// <summary>
    /// Interface for a proxy support class
    /// </summary>
    public interface IProxyHelper
    {
        /// <summary>
        /// Gets the remote type being proxied.
        /// </summary>
        Type RemoteType { get; }
        /// <summary>
        /// Gets the peer where the remote instance is proxied on
        /// </summary>
        RPCPeer Peer { get; }

        /// <summary>
        /// Handles invocation of a method on the proxy
        /// </summary>
        /// <returns>The result of invoking the method.</returns>
        /// <param name="name">The name of the method to invoke.</param>
        /// <param name="types">The types of the arguments to invoke.</param>
        /// <param name="arguments">The arguments passed to the method.</param>
        object HandleInvokeMethod(string name, Type[] types, object[] arguments);
        /// <summary>
        /// Handles invocation of a method on the proxy
        /// </summary>
        /// <returns>The result of invoking the method.</returns>
        /// <param name="name">The name of the method to invoke.</param>
        /// <param name="types">The types of the arguments to invoke.</param>
        /// <param name="arguments">The arguments passed to the method.</param>
        Task<object> HandleInvokeMethodAsync(string name, Type[] types, object[] arguments);

        /// <summary>
        /// Handles property read
        /// </summary>
        /// <returns>The value of the property.</returns>
        /// <param name="name">The name of the property to read.</param>
        /// <param name="indextypes">The types of the indices, if accessing an indexed property.</param>
        /// <param name="indexarguments">The index arguments, if accessing an indexed property.</param>
        object HandleInvokePropertyGet(string name, Type[] indextypes, object[] indexarguments);
        /// <summary>
        /// Handles property read
        /// </summary>
        /// <returns>The value of the property.</returns>
        /// <param name="name">The name of the property to read.</param>
        /// <param name="indextypes">The types of the indices, if accessing an indexed property.</param>
        /// <param name="indexarguments">The index arguments, if accessing an indexed property.</param>
        T HandleInvokePropertyGet<T>(string name, Type[] indextypes, object[] indexarguments);
        /// <summary>
        /// Handles property read
        /// </summary>
        /// <returns>The value of the property.</returns>
        /// <param name="name">The name of the property to read.</param>
        /// <param name="indextypes">The types of the indices, if accessing an indexed property.</param>
        /// <param name="indexarguments">The index arguments, if accessing an indexed property.</param>
        Task<object> HandleInvokePropertyGetAsync(string name, Type[] indextypes, object[] indexarguments);

        /// <summary>
        /// Handles property read
        /// </summary>
        /// <returns>The value of the property.</returns>
        /// <param name="name">The name of the property to read.</param>
        /// <param name="indextypes">The types of the indices, if accessing an indexed property.</param>
        /// <param name="indexarguments">The index arguments, if accessing an indexed property.</param>
        Task<T> HandleInvokePropertyGetAsync<T>(string name, Type[] indextypes, object[] indexarguments);

        /// <summary>
        /// Handles property write
        /// </summary>
        /// <param name="name">The name of the property to write.</param>
        /// <param name="value">The value to set</param>
        /// <param name="indextypes">The types of the indices, if accessing an indexed property.</param>
        /// <param name="indexarguments">The index arguments, if accessing an indexed property.</param>
        void HandleInvokePropertySet(string name, object value, Type[] indextypes, object[] indexarguments);
        /// <summary>
        /// Handles property write
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="name">The name of the property to write.</param>
        /// <param name="value">The value to set</param>
        /// <param name="indextypes">The types of the indices, if accessing an indexed property.</param>
        /// <param name="indexarguments">The index arguments, if accessing an indexed property.</param>
        Task HandleInvokePropertySetAsync(string name, object value, Type[] indextypes, object[] indexarguments);
    }

    /// <summary>
    /// Class that wraps an interface type and provides access to the property through a dictionary of values
    /// </summary>
    public class PropertyDecomposedObject : IProxyHelper
    {
        /// <summary>
        /// The values passed to the accessor
        /// </summary>
        public Dictionary<string, object> m_values;

        /// <summary>
        /// The peer that this remote object is created by
        /// </summary>
        protected readonly RPCPeer m_peer;

        /// <summary>
        /// The type of the remote object
        /// </summary>
        protected readonly Type m_remoteType;

        /// <inheritdoc />
        public Type RemoteType => m_remoteType;

        /// <inheritdoc />
        public RPCPeer Peer => m_peer;

        /// <summary>
        /// Initializes a new instance of the <see cref="T:LeanIPC.PropertyDecomposedObject"/> class.
        /// </summary>
        /// <param name="peer">The unused peer argument.</param>
        /// <param name="remotetype">The remote type.</param>
        /// <param name="handle">The unused handle argument.</param>
        public PropertyDecomposedObject(RPCPeer peer, Type remotetype, long handle)
        {
            m_peer = peer;
            m_remoteType = remotetype;
        }

        /// <inheritdoc />
        public object HandleInvokeMethod(string name, Type[] types, object[] arguments) => throw new NotImplementedException($"Cannot invoke methods on a decomposed instance");
        /// <inheritdoc />
        public Task<object> HandleInvokeMethodAsync(string name, Type[] types, object[] arguments) => throw new NotImplementedException($"Cannot invoke methods on a decomposed instance");

        /// <inheritdoc />
        public object HandleInvokePropertyGet(string name, Type[] indextypes, object[] indexarguments)
        {
            return m_values[name];
        }

        /// <inheritdoc />
        public T HandleInvokePropertyGet<T>(string name, Type[] indextypes, object[] indexarguments)
        {
            return (T)m_values[name];
        }

        /// <inheritdoc />
        public Task<object> HandleInvokePropertyGetAsync(string name, Type[] indextypes, object[] indexarguments)
        {
            return Task.FromResult(m_values[name]);
        }

        /// <inheritdoc />
        public Task<T> HandleInvokePropertyGetAsync<T>(string name, Type[] indextypes, object[] indexarguments)
        {
            return Task.FromResult((T)m_values[name]);
        }

        /// <inheritdoc />
        public void HandleInvokePropertySet(string name, object value, Type[] indextypes, object[] indexarguments)
        {
            m_values[name] = value;
        }

        /// <inheritdoc />
        public Task HandleInvokePropertySetAsync(string name, object value, Type[] indextypes, object[] indexarguments)
        {
            m_values[name] = value;
            return Task.FromResult(true);
        }
    }

    /// <summary>
    /// Basic remote object class
    /// </summary>
    public class RemoteObject : IRemoteInstance, IDisposable, IProxyHelper
    {
        /// <summary>
        /// The handle instance
        /// </summary>
        protected readonly long m_handle;

        /// <summary>
        /// The peer that this remote object is created by
        /// </summary>
        protected readonly RPCPeer m_peer;

        /// <summary>
        /// The type of the remote object
        /// </summary>
        protected readonly Type m_remoteType;

        /// <summary>
        /// The list of type lookups
        /// </summary>
        protected readonly Type[] m_typelookups;

        /// <summary>
        /// A flag indicating if the item is already disposed
        /// </summary>
        protected bool m_isDisposed = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="T:LeanIPC.RemoteObject"/> class.
        /// </summary>
        /// <param name="handle">The handle to use.</param>
        public RemoteObject(RPCPeer peer, Type type, long handle)
        {
            m_peer = peer;
            m_handle = handle;
            m_remoteType = type;
            m_typelookups = m_remoteType.IsInterface
                ? new Type[] { m_remoteType }.Concat(m_remoteType.GetInterfaces()).Distinct().ToArray()
                : new Type[] { m_remoteType };
        }

        /// <summary>
        /// Invokes a remote method
        /// </summary>
        /// <returns>The result of the invocation.</returns>
        /// <param name="name">The name of the method to invoke.</param>
        /// <param name="types">The method types.</param>
        /// <param name="arguments">The arguments to invoke the method with.</param>
        /// <typeparam name="T">The data type parameter.</typeparam>
        public Task<T> HandleInvokeMethodAsync<T>(string name, Type[] types, object[] arguments)
        {
            types = types ?? Type.EmptyTypes;
            var m = m_typelookups.Select(x => x.GetMethod(name, types)).FirstOrDefault(x => x != null);
            if (m == null)
                throw new MissingMethodException($"No method with the signature {name}({string.Join(",", types.Select(x => x.FullName))})");
            
            return m_peer.InvokeRemoteMethodAsync<T>(m_handle, m, arguments, false);
        }

        /// <summary>
        /// Invokes a remote method
        /// </summary>
        /// <returns>The result of the invocation.</returns>
        /// <param name="name">The name of the method to invoke.</param>
        /// <param name="types">The method types.</param>
        /// <param name="arguments">The arguments to invoke the method with.</param>
        /// <typeparam name="T">The data type parameter.</typeparam>
        public T HandleInvokeMethod<T>(string name, Type[] types, object[] arguments)
        {
            return HandleInvokeMethodAsync<T>(name, types, arguments).Result;
        }

        /// <summary>
        /// Invokes a remote method
        /// </summary>
        /// <returns>The result of the invocation.</returns>
        /// <param name="name">The name of the method to invoke.</param>
        /// <param name="types">The method types.</param>
        /// <param name="arguments">The arguments to invoke the method with.</param>
        public Task<object> HandleInvokeMethodAsync(string name, Type[] types, object[] arguments)
        {
            types = types ?? Type.EmptyTypes;
            var m = m_typelookups.Select(x => x.GetMethod(name, types)).FirstOrDefault(x => x != null);
            if (m == null)
            {
                // If the remote type does not have the IDisposable interface, just call our local Dispose method
                if (name == nameof(IDisposable.Dispose) && (types?.Length == 0))
                {
                    this.Dispose();
                    return Task.FromResult<object>(null);
                }

                // If this was not a dispose call, it is an error
                throw new MissingMethodException($"No method with the signature {name}({string.Join(",", types.Select(x => x.FullName))})");
            }
            return m_peer.InvokeRemoteMethodAsync(m_handle, m, arguments, false);
        }

        /// <summary>
        /// Invokes a remote method
        /// </summary>
        /// <returns>The result of the invocation.</returns>
        /// <param name="name">The name of the method to invoke.</param>
        /// <param name="types">The method types.</param>
        /// <param name="arguments">The arguments to invoke the method with.</param>
        /// <typeparam name="T">The data type parameter.</typeparam>
        public object HandleInvokeMethod(string name, Type[] types, object[] arguments)
        {
            return HandleInvokeMethodAsync(name, types, arguments).Result;
        }

        /// <summary>
        /// Invokes a remote property's get method
        /// </summary>
        /// <returns>The property value.</returns>
        /// <param name="name">The name of the property.</param>
        /// <param name="indextypes">The types of the index values for the property.</param>
        /// <param name="indexarguments">The index arguments.</param>
        /// <typeparam name="T">The property type parameter.</typeparam>
        public Task<T> HandleInvokePropertyGetAsync<T>(string name, Type[] indextypes, object[] indexarguments)
        {
            indextypes = indextypes ?? Type.EmptyTypes;
            var p = m_typelookups.Select(x => x.GetProperty(name, indextypes)).FirstOrDefault(x => x != null);
            if (p == null)
                throw new MissingMethodException($"No property with the signature {indextypes.First().FullName} {name}({string.Join(",", indextypes.Skip(1).Select(x => x.FullName))})");
            return m_peer.InvokeRemoteMethodAsync<T>(m_handle, p, indexarguments, false);
        }

        /// <summary>
        /// Invokes a remote property's get method
        /// </summary>
        /// <returns>The property value.</returns>
        /// <param name="name">The name of the property.</param>
        /// <param name="indextypes">The types of the index values for the property.</param>
        /// <param name="indexarguments">The index arguments.</param>
        /// <typeparam name="T">The property type parameter.</typeparam>
        public T HandleInvokePropertyGet<T>(string name, Type[] indextypes, object[] indexarguments)
        {
            return HandleInvokePropertyGetAsync<T>(name, indextypes, indexarguments).Result;
        }


        /// <summary>
        /// Invokes a remote property's get method
        /// </summary>
        /// <returns>The property value.</returns>
        /// <param name="name">The name of the property.</param>
        /// <param name="indextypes">The types of the index values for the property.</param>
        /// <param name="indexarguments">The index arguments.</param>
        public Task<object> HandleInvokePropertyGetAsync(string name, Type[] indextypes, object[] indexarguments)
        {
            indextypes = indextypes ?? Type.EmptyTypes;
            var p = m_typelookups.Select(x => x.GetProperty(name, indextypes)).FirstOrDefault(x => x != null);
            if (p == null)
                throw new MissingMethodException($"No property with the signature {indextypes.First().FullName} {name}({string.Join(",", indextypes.Skip(1).Select(x => x.FullName))})");
            return m_peer.InvokeRemoteMethodAsync(m_handle, p, indexarguments, false);
        }

        /// <summary>
        /// Invokes a remote property's get method
        /// </summary>
        /// <returns>The property value.</returns>
        /// <param name="name">The name of the property.</param>
        /// <param name="indextypes">The types of the index values for the property.</param>
        /// <param name="indexarguments">The index arguments.</param>
        public object HandleInvokePropertyGet(string name, Type[] indextypes, object[] indexarguments)
        {
            return HandleInvokePropertyGetAsync(name, indextypes, indexarguments).Result;
        }

        /// <summary>
        /// Invokes a remote property's set method
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="name">The name of the property.</param>
        /// <param name="value">The value to set.</param>
        /// <param name="indextypes">The types of the index values for the property.</param>
        /// <param name="indexarguments">The index arguments.</param>
        public Task HandleInvokePropertySetAsync(string name, object value, Type[] indextypes, object[] indexarguments)
        {
            indextypes = indextypes ?? Type.EmptyTypes;
            var p = m_typelookups.Select(x => x.GetProperty(name, indextypes)).FirstOrDefault(x => x != null);
            if (p == null)
                throw new MissingMethodException($"No property with the signature {indextypes.First().FullName} {name}({string.Join(",", indextypes.Skip(1).Select(x => x.FullName))})");
            return m_peer.InvokeRemoteMethodAsync(m_handle, p, new object[] { value }.Concat(indexarguments).ToArray(), true);
        }

        /// <summary>
        /// Invokes a remote property's set method
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="name">The name of the property.</param>
        /// <param name="value">The value to set.</param>
        /// <param name="indextypes">The types of the index values for the property.</param>
        /// <param name="indexarguments">The index arguments.</param>
        public void HandleInvokePropertySet(string name, object value, Type[] indextypes, object[] indexarguments)
        {
            HandleInvokePropertySetAsync(name, value, indextypes, indexarguments).Wait();
        }

        /// <summary>
        /// Gets the remote handle.
        /// </summary>
        public long Handle => m_handle;

        /// <summary>
        /// Gets the actual remote type
        public Type RemoteType => m_remoteType;

        /// <summary>
        /// Gets the remote peer that this proxy is bound to
        /// </summary>
        public RPCPeer Peer => m_peer;

        /// <summary>
        /// Detaches the handle
        /// </summary>
        /// <param name="suppressPeerCall">If set to <c>true</c> does not call the peer, but assumes this has already been done.</param>
        public Task Detach(bool suppressPeerCall)
        {
            if (!m_isDisposed)
            {
                m_isDisposed = true;
                if (!suppressPeerCall)
                    return m_peer.InvokeDetachAsync(m_handle);
            }

            return Task.FromResult(true);
        }

        /// <summary>
        /// Dispose the current instance.
        /// </summary>
        /// <param name="disposing">If set to <c>true</c> disposing.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!m_isDisposed)
            {
                try
                {
                    m_isDisposed = true;

                    // Invoke the Dispose call remotely as well
                    if (disposing && typeof(IDisposable).IsAssignableFrom(m_remoteType))
                        m_peer.InvokeRemoteMethodAsync(m_handle, m_remoteType.GetMethod(nameof(Dispose)), null, false).Wait();

                    // Unregister this instance
                    m_peer.InvokeDetachAsync(m_handle).Wait();
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine("Exception while disposing item: {0}", ex);
                }
            }
        }

        /// <summary>
        /// Releases unmanaged resources and performs other cleanup operations before the
        /// <see cref="T:LeanIPC.RemoteObject"/> is reclaimed by garbage collection.
        /// </summary>
         ~RemoteObject() {
           Dispose(false);
         }

        /// <summary>
        /// Releases all resource used by the <see cref="T:LeanIPC.RemoteObject"/> object.
        /// </summary>
        /// <remarks>Call <see cref="Dispose"/> when you are finished using the <see cref="T:LeanIPC.RemoteObject"/>. The
        /// <see cref="Dispose"/> method leaves the <see cref="T:LeanIPC.RemoteObject"/> in an unusable state. After
        /// calling <see cref="Dispose"/>, you must release all references to the <see cref="T:LeanIPC.RemoteObject"/>
        /// so the garbage collector can reclaim the memory that the <see cref="T:LeanIPC.RemoteObject"/> was occupying.</remarks>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}
