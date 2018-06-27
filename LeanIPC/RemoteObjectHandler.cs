using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace LeanIPC
{
	/// <summary>
    /// Class that keeps track of remote object instances
    /// </summary>
    public class RemoteObjectHandler
    {
        /// <summary>
        /// The lookup table with the known remote handles and their proxies
        /// </summary>
        private readonly Dictionary<long, IRemoteInstance> m_remoteHandles = new Dictionary<long, IRemoteInstance>();
        /// <summary>
        /// The lookup table with the known remote handles and their proxies
        /// </summary>
        private readonly Dictionary<IRemoteInstance, long> m_remoteObjects = new Dictionary<IRemoteInstance, long>();

        /// <summary>
        /// The lookup table with the known local handles and the objects they reference
        /// </summary>
        private readonly Dictionary<long, object> m_localHandles = new Dictionary<long, object>();
        /// <summary>
        /// The lookup table with the known local handles and the objects they reference
        /// </summary>
        private readonly Dictionary<object, long> m_localObjects = new Dictionary<object, long>();

        /// <summary>
        /// The lock used to guard the handle table
        /// </summary>
        private readonly AsyncLock m_lock = new AsyncLock();

        /// <summary>
        /// The object handle counter
        /// </summary>
        private long m_nextObjectId = 1;

        /// <summary>
        /// Registers a remote object
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The remote item.</param>
        public async Task<bool> RegisterRemoteObjectAsync(long id, IRemoteInstance item)
        {
            if (item == null)
                throw new ArgumentNullException(nameof(item));
            
            using (await m_lock.LockAsync())
            {
                if (m_remoteHandles.TryGetValue(id, out var tmp))
                {
                    if (tmp != item)
                        throw new InvalidOperationException("Attempted to register handle with new instance");
                    return false;
                }

                if (m_remoteObjects.TryGetValue(item, out var tmpid) && tmpid != id)
                    throw new InvalidOperationException("Attempted to register handle with new instance");

                m_remoteHandles[id] = item;
                m_remoteObjects[item] = id;
                return true;
            }
        }

        /// <summary>
        /// Registers the local object and increments the reference count.
        /// </summary>
        /// <returns><c>true</c> if a new reference was added, <c>false</c> otherwise.</returns>
        /// <param name="item">The item to register.</param>
        public async Task<bool> RegisterLocalObjectAsync(object item)
        {
            if (item == null)
                throw new ArgumentNullException(nameof(item));
            
            using (await m_lock.LockAsync())
            {
                if (m_localObjects.ContainsKey(item))
                    return false;

                var id = System.Threading.Interlocked.Increment(ref m_nextObjectId);
                m_localHandles[id] = item;
                m_localObjects[item] = id;
                return true;
            }
        }

        /// <summary>
        /// Removes a handle for a locally registered object
        /// </summary>
        /// <returns>The local handle async.</returns>
        /// <param name="id">Identifier.</param>
        public async Task<bool> RemoveLocalHandleAsync(long id)
        {
            using (await m_lock.LockAsync())
            {
                if (m_localHandles.TryGetValue(id, out var val))
                    m_localObjects.Remove(val);
                return m_localHandles.Remove(id);
            }
        }

        /// <summary>
        /// Removes a handle for a remote registered object
        /// </summary>
        /// <returns>The local handle async.</returns>
        /// <param name="id">Identifier.</param>
        public async Task<bool> RemoveRemoteHandleAsync(long id)
        {
            using (await m_lock.LockAsync())
            {
                if (m_remoteHandles.TryGetValue(id, out var val))
                    m_remoteObjects.Remove(val);
                return m_remoteHandles.Remove(id);
            }
        }

        /// <summary>
        /// Checks if an object is a remote handle
        /// </summary>
        /// <returns><c>true</c>, if the item is registered as a remote item, <c>false</c> otherwise.</returns>
        /// <param name="item">Item.</param>
        public bool IsRemoteObject(IRemoteInstance item)
        {
            if (item == null)
                return false;

            return m_remoteObjects.ContainsKey(item);
        }

        /// <summary>
        /// Checks if an object is a remote handle
        /// </summary>
        /// <returns><c>true</c>, if the handles is registered as a remote handle, <c>false</c> otherwise.</returns>
        /// <param name="id">The ID to examine.</param>
        public bool IsRemoteHandle(long id)
        {
            return m_remoteHandles.ContainsKey(id);
        }

        /// <summary>
        /// Gets the handle for a remote object
        /// </summary>
        /// <returns>The remote object handle.</returns>
        /// <param name="item">The object to get the handle for.</param>
        public long GetRemoteHandle(IRemoteInstance item)
        {
            if (item == null)
                throw new ArgumentNullException(nameof(item));

            return m_remoteObjects[item];
        }

        /// <summary>
        /// Gets the remote object with the given handle
        /// </summary>
        /// <returns>The remote object.</returns>
        /// <param name="id">The remote object handle.</param>
        public object GetRemoteObject(long id)
        {
            return m_remoteHandles[id];
        }

        /// <summary>
        /// Attempts to get the local object with the given handle
        /// </summary>
        /// <returns><c>true</c>, if get remote object was found, <c>false</c> otherwise.</returns>
        /// <param name="id">The handle for the remote object.</param>
        /// <param name="item">The remote object instance.</param>
        public bool TryGetRemoteObject(long id, out IRemoteInstance item)
        {
            return m_remoteHandles.TryGetValue(id, out item);
        }

        /// <summary>
        /// Attempts to get the handle for the local object
        /// </summary>
        /// <returns><c>true</c>, if get local handle was obtained, <c>false</c> otherwise.</returns>
        /// <param name="item">The item to get the handle for.</param>
        /// <param name="id">The handle for the item.</param>
        public bool TryGetRemoteHandle(IRemoteInstance item, out long id)
        {
            if (item == null)
                throw new ArgumentNullException(nameof(item));

            return m_remoteObjects.TryGetValue(item, out id);
        }

        /// <summary>
        /// Checks if an object is a local handle
        /// </summary>
        /// <returns><c>true</c>, if the item is registered as a remote item, <c>false</c> otherwise.</returns>
        /// <param name="item">Item.</param>
        public bool IsLocalObject(object item)
        {
            if (item == null)
                return false;

            return m_localObjects.ContainsKey(item);
        }

        /// <summary>
        /// Checks if an object is a local handle
        /// </summary>
        /// <returns><c>true</c>, if the handles is registered as a remote handle, <c>false</c> otherwise.</returns>
        /// <param name="id">The ID to examine.</param>
        public bool IsLocalHandle(long id)
        {
            return m_localHandles.ContainsKey(id);
        }

        /// <summary>
        /// Gets the handle for a local object
        /// </summary>
        /// <returns>The local object handle.</returns>
        /// <param name="item">The object to get the handle for.</param>
        public long GetLocalHandle(object item)
        {
            if (item == null)
                throw new ArgumentNullException(nameof(item));

            return m_localObjects[item];
        }

        /// <summary>
        /// Gets the local object with the given handle
        /// </summary>
        /// <returns>The local object.</returns>
        /// <param name="id">The local object handle.</param>
        public object GetLocalObject(long id)
        {
            return m_localHandles[id];
        }

        /// <summary>
        /// Attempts to get the local object with the given handle
        /// </summary>
        /// <returns><c>true</c>, if get remote object was found, <c>false</c> otherwise.</returns>
        /// <param name="id">The handle for the local object.</param>
        /// <param name="item">The local object instance.</param>
        public bool TryGetLocalObject(long id, out object item)
        {
            return m_localHandles.TryGetValue(id, out item);
        }

        /// <summary>
        /// Attempts to get the handle for the local object
        /// </summary>
        /// <returns><c>true</c>, if get local handle was obtained, <c>false</c> otherwise.</returns>
        /// <param name="item">The item to get the handle for.</param>
        /// <param name="id">The handle for the item.</param>
        public bool TryGetLocalHandle(object item, out long id)
        {
            if (item == null)
                throw new ArgumentNullException(nameof(item));
            
            return m_localObjects.TryGetValue(item, out id);
        }
    }
}
