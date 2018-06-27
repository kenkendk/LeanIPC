using System;
using System.Linq;
using System.Threading.Tasks;

namespace LeanIPC
{
    /// <summary>
    /// Marker interface to signal that an instance is a remote entity
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
    /// Basic remote object class
    /// </summary>
    public abstract class RemoteObject : IRemoteInstance, IDisposable
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
        }

        /// <summary>
        /// Invokes a remote method
        /// </summary>
        /// <returns>The result of the invocation.</returns>
        /// <param name="name">The name of the method to invoke.</param>
        /// <param name="types">The method types.</param>
        /// <param name="arguments">The arguments to invoke the method with.</param>
        /// <typeparam name="T">The data type parameter.</typeparam>
        protected Task<T> RemoteInvokeMethodAsync<T>(string name, Type[] types, object[] arguments)
        {
            types = types ?? Type.EmptyTypes;
            var m = m_remoteType.GetMethod(name, types);
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
        protected T RemoteInvokeMethod<T>(string name, Type[] types, object[] arguments)
        {
            return RemoteInvokeMethodAsync<T>(name, types, arguments).Result;
        }

        /// <summary>
        /// Invokes a remote method
        /// </summary>
        /// <returns>The result of the invocation.</returns>
        /// <param name="name">The name of the method to invoke.</param>
        /// <param name="types">The method types.</param>
        /// <param name="arguments">The arguments to invoke the method with.</param>
        protected Task<object> RemoteInvokeMethodAsync(string name, Type[] types, object[] arguments)
        {
            types = types ?? Type.EmptyTypes;
            var m = m_remoteType.GetMethod(name, types);
            if (m == null)
                throw new MissingMethodException($"No method with the signature {name}({string.Join(",", types.Select(x => x.FullName))})");
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
        protected object RemoteInvokeMethod(string name, Type[] types, object[] arguments)
        {
            return RemoteInvokeMethodAsync(name, types, arguments).Result;
        }

        /// <summary>
        /// Invokes a remote property's get method
        /// </summary>
        /// <returns>The property value.</returns>
        /// <param name="name">The name of the property.</param>
        /// <param name="indextypes">The types of the index values for the property.</param>
        /// <param name="indexarguments">The index arguments.</param>
        /// <typeparam name="T">The property type parameter.</typeparam>
        protected Task<T> RemoteInvokePropertyGetAsync<T>(string name, Type[] indextypes, object[] indexarguments)
        {
            indextypes = indextypes ?? Type.EmptyTypes;
            var p = m_remoteType.GetProperty(name, indextypes);
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
        protected T RemoteInvokePropertyGet<T>(string name, Type[] indextypes, object[] indexarguments)
        {
            return RemoteInvokePropertyGetAsync<T>(name, indextypes, indexarguments).Result;
        }


        /// <summary>
        /// Invokes a remote property's get method
        /// </summary>
        /// <returns>The property value.</returns>
        /// <param name="name">The name of the property.</param>
        /// <param name="indextypes">The types of the index values for the property.</param>
        /// <param name="indexarguments">The index arguments.</param>
        protected Task<object> RemoteInvokePropertyGetAsync(string name, Type[] indextypes, object[] indexarguments)
        {
            return m_peer.InvokeRemoteMethodAsync(m_handle, m_remoteType.GetProperty(name, indextypes ?? Type.EmptyTypes), indexarguments, false);
        }

        /// <summary>
        /// Invokes a remote property's get method
        /// </summary>
        /// <returns>The property value.</returns>
        /// <param name="name">The name of the property.</param>
        /// <param name="indextypes">The types of the index values for the property.</param>
        /// <param name="indexarguments">The index arguments.</param>
        protected object RemoteInvokePropertyGet(string name, Type[] indextypes, object[] indexarguments)
        {
            return RemoteInvokePropertyGetAsync(name, indextypes, indexarguments).Result;
        }

        /// <summary>
        /// Invokes a remote property's set method
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="name">The name of the property.</param>
        /// <param name="value">The value to set.</param>
        /// <param name="indextypes">The types of the index values for the property.</param>
        /// <param name="indexarguments">The index arguments.</param>
        protected Task RemoteInvokePropertySetAsync(string name, object value, Type[] indextypes, object[] indexarguments)
        {
            indextypes = indextypes ?? Type.EmptyTypes;
            var p = m_remoteType.GetProperty(name, indextypes);
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
        protected void RemoteInvokePropertySet(string name, object value, Type[] indextypes, object[] indexarguments)
        {
            RemoteInvokePropertySetAsync(name, value, indextypes, indexarguments).Wait();
        }

        /// <summary>
        /// Gets the remote handle.
        /// </summary>
        long IRemoteInstance.Handle => m_handle;

        /// <summary>
        /// Detaches the handle
        /// </summary>
        /// <param name="suppressPeerCall">If set to <c>true</c> does not call the peer, but assumes this has already been done.</param>
        Task IRemoteInstance.Detach(bool suppressPeerCall)
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
