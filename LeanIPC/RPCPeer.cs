using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace LeanIPC
{
    /// <summary>
    /// A peer that supports remote method invocations over an instance of <see cref="InterProcessConnection"/>
    /// </summary>
    public class RPCPeer : IDisposable
    {
        /// <summary>
        /// The connection instance
        /// </summary>
        protected readonly InterProcessConnection m_connection;

        /// <summary>
        /// The remote object manager
        /// </summary>
        protected readonly RemoteObjectHandler m_remoteObjects;

        /// <summary>
        /// The type serializer
        /// </summary>
        protected readonly TypeSerializer m_typeSerializer;

        /// <summary>
        /// The allowed remote invocation types
        /// </summary>
        protected readonly List<Type> m_allowedTypes = new List<Type>();

        /// <summary>
        /// The lock guarding the <see cref="m_remoteProxies"/> list
        /// </summary>
        private readonly object m_lock = new object();

        /// <summary>
        /// The list of methods used to create a proxy instance from a remote type
        /// </summary>
        private readonly List<Func<RPCPeer, Type, long, IRemoteInstance>> m_remoteProxies = new List<Func<RPCPeer, Type, long, IRemoteInstance>>();

        /// <summary>
        /// The dictionary with automatic proxy generators
        /// </summary>
        private readonly Dictionary<Type, Type> m_automaticProxies = new Dictionary<Type, Type>();

        /// <summary>
        /// The method used to filter which members can be remotely accessed
        /// </summary>
        private Func<System.Reflection.MemberInfo, object[], bool> m_filterFunction;

        /// <summary>
        /// Gets the type serializer used for this instance.
        /// </summary>
        public TypeSerializer TypeSerializer => m_typeSerializer;

        /// <summary>
        /// The remote object handler
        /// </summary>
        /// <value>The remote handler.</value>
        public RemoteObjectHandler RemoteHandler => m_remoteObjects;

        /// <summary>
        /// Initializes a new instance of the <see cref="T:LeanIPC.RPCPeer"/> class.
        /// </summary>
        /// <param name="connection">The connection to use for invoking methods.</param>
        /// <param name="allowedTypes">The types on which remote execution is allowed</param>
        /// <param name="filterPredicate">A predicate function used to filter which methods can be invoked</param>
        public RPCPeer(InterProcessConnection connection, Type[] allowedTypes, Func<System.Reflection.MemberInfo, object[], bool> filterPredicate)
        {
            m_connection = connection ?? throw new ArgumentNullException(nameof(connection));
            m_connection.AddMessageHandler(HandleMessage);
            m_allowedTypes.AddRange(allowedTypes ?? new Type[0]);
            if (m_allowedTypes.Count != 0 && m_filterFunction == null)
                filterPredicate = (a, b) => true;

            m_filterFunction = filterPredicate;

            m_typeSerializer = m_connection.TypeSerializer ?? throw new ArgumentNullException(nameof(m_connection.TypeSerializer));
            m_remoteObjects = m_connection.RemoteHandler ?? throw new ArgumentNullException(nameof(m_connection.RemoteHandler));
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="T:LeanIPC.RPCPeer"/> class.
        /// </summary>
        /// <param name="connection">The connection to use for invoking methods.</param>
        /// <param name="filterPredicate">A predicate function used to filter which methods can be invoked</param>
        public RPCPeer(InterProcessConnection connection, Func<System.Reflection.MemberInfo, object[], bool> filterPredicate)
            : this(connection, null, filterPredicate)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="T:LeanIPC.RPCPeer"/> class.
        /// </summary>
        /// <param name="connection">The connection to use for invoking methods.</param>
        /// <param name="allowedTypes">The types on which remote execution is allowed</param>
        public RPCPeer(InterProcessConnection connection, params Type[] allowedTypes)
            : this(connection, allowedTypes, null)
        {
        }

        /// <summary>
        /// Adds a new proxy generator method
        /// </summary>
        /// <param name="method">The method used to generate remoting proxies.</param>
        public void AddProxyGenerator(Func<RPCPeer, Type, long, IRemoteInstance> method)
        {
            if (method == null)
                throw new ArgumentNullException(nameof(method));

            lock(m_lock)
                m_remoteProxies.Add(method);
        }

        /// <summary>
        /// Adds an automatically generated remoting proxy for the given type
        /// </summary>
        /// <param name="remotetype">The type to generate a proxy for.</param>
        /// <param name="localinterface">The local defined interface for the remote object.</param>
        public void AddAutomaticProxy(Type remotetype, Type localinterface)
        {
            lock (m_lock)
                m_automaticProxies.Add(remotetype, localinterface);
        }

        /// <summary>
        /// Removes the automatic proxy generator for the given type
        /// </summary>
        /// <param name="remotetype">The type to remove the automatic proxy for.</param>
        /// <returns><c>true</c> if the item was removed; <c>false</c> otherwise</returns>
        public bool RemoveAutomaticProxy(Type remotetype)
        {
            lock (m_lock)
                return m_automaticProxies.Remove(remotetype);
        }

        /// <summary>
        /// Removes a proxy generator method
        /// </summary>
        /// <param name="method">The method used to generate remoting proxies.</param>
        /// <returns><c>true</c> if the item was removed from the list, <c>false</c> otherwise</returns>
        public bool RemoveProxyGenerator(Func<RPCPeer, Type, long, IRemoteInstance> method)
        {
            if (method == null)
                throw new ArgumentNullException(nameof(method));

            lock (m_lock)
                return m_remoteProxies.Remove(method);
        }

        /// <summary>
        /// Registers a local object for invocation by the remote.
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The local object to expose to the remote peer.</param>
        /// <param name="type">The type to register the object as, if <c>null</c>, the object type is used</param>
        public async Task RegisterLocalObjectOnRemote(object item, Type type = null)
        {
            if (item == null)
                throw new ArgumentNullException(nameof(item));
            
            if (await m_remoteObjects.RegisterLocalObjectAsync(item))
                await m_connection.SendPassthroughAsync(Command.RegisterRemoteObject, new RegisterRemoteObjectRequest(type ?? item.GetType(), m_remoteObjects.GetLocalHandle(item)));            
        }

        /// <summary>
        /// Invokes a method remotely, and returns the result
        /// </summary>
        /// <returns>The result of the remote invocation.</returns>
        /// <param name="handle">The remote instance handle, or zero for null.</param>
        /// <param name="remotetype">A type descriptor for the type the method is invoked on.</param>
        /// <param name="remotemethod">A serialized description of the method to invoke.</param>
        /// <param name="types">The types of the input arguments.</param>
        /// <param name="arguments">The values given to the method when invoked.</param>
        /// <param name="isWrite">If set to <c>true</c> this is a write field or property request.</param>
        /// <typeparam name="T">The return type</typeparam>
        public async Task<T> InvokeRemoteMethodAsync<T>(long handle, string remotetype, string remotemethod, Type[] types, object[] arguments, bool isWrite)
        {
            return (T)await InvokeRemoteMethodAsync(handle, remotetype, remotemethod, types, arguments, isWrite);
        }

        /// <summary>
        /// Invokes a method remotely, and returns the result
        /// </summary>
        /// <returns>The result of the remote invocation.</returns>
        /// <param name="handle">The remote instance handle, or zero for null.</param>
        /// <param name="remotetype">A type descriptor for the type the method is invoked on.</param>
        /// <param name="remotemethod">A serialized description of the method to invoke.</param>
        /// <param name="types">The types of the input arguments.</param>
        /// <param name="arguments">The values given to the method when invoked.</param>
        /// <param name="isWrite">If set to <c>true</c> this is a write field or property request.</param>
        public async Task<object> InvokeRemoteMethodAsync(long handle, string remotetype, string remotemethod, Type[] types, object[] arguments, bool isWrite)
        {
            if (string.IsNullOrWhiteSpace(remotetype))
                throw new ArgumentNullException(remotetype);
            if (string.IsNullOrWhiteSpace(remotemethod))
                throw new ArgumentNullException(remotemethod);

            types = types ?? new Type[0];
            arguments = arguments ?? new object[0];

            if (types.Length != arguments.Length)
                throw new ArgumentOutOfRangeException(nameof(types), $"The length of  {nameof(types)} must be the same as the length of {nameof(arguments)}");

            // Register each local reference item with the remote
            foreach (var arg in arguments)
                if (arg != null && (m_typeSerializer.GetAction(arg.GetType()) == SerializationAction.Reference || m_remoteObjects.IsLocalObject(arg)))
                    await RegisterLocalObjectOnRemote(arg);

            var res = await m_connection.SendAndWaitAsync(
                Command.InvokeRemoteMethod,
                new InvokeRemoteMethodRequest(handle, remotetype, remotemethod, isWrite, types, arguments)
            );

            if (res.Types.Length == 0 || res.Types[0] == typeof(void))
                return null;

            var resp = (InvokeRemoteMethodResponse)res.Values[0];
            if (resp.ResultType == typeof(void) || resp.ResultType == typeof(Task))
                return null;
            if (resp.ResultType.IsConstructedGenericType && resp.ResultType.GetGenericTypeDefinition() == typeof(Task<>))
                return resp.Result;

            return resp.Result;
        }

        /// <summary>
        /// Invokes a method remotely, and returns the result
        /// </summary>
        /// <returns>The result of the remote invocation.</returns>
        /// <param name="handle">The remote instance handle, or zero for null.</param>
        /// <param name="method">The method to invoke remotely.</param>
        /// <param name="arguments">The values given to the method when invoked.</param>
        /// <param name="isWrite">If set to <c>true</c> this is a write field or property request.</param>
        /// <typeparam name="T">The return type</typeparam>
        public async Task<T> InvokeRemoteMethodAsync<T>(long handle, System.Reflection.MemberInfo method, object[] arguments, bool isWrite)
        {
            return (T)await InvokeRemoteMethodAsync(handle, method, arguments, isWrite);
        }

        /// <summary>
        /// Invokes a method remotely, and returns the result
        /// </summary>
        /// <returns>The result of the remote invocation.</returns>
        /// <param name="handle">The remote instance handle, or zero for null.</param>
        /// <param name="method">The method to invoke remotely.</param>
        /// <param name="arguments">The values given to the method when invoked.</param>
        /// <param name="isWrite">If set to <c>true</c> this is a write field or property request.</param>
        public Task<object> InvokeRemoteMethodAsync(long handle, System.Reflection.MemberInfo method, object[] arguments, bool isWrite)
        {
            var methodstring = m_typeSerializer.GetShortDefinition(method);
            var type = m_typeSerializer.GetShortTypeName(method.DeclaringType);

            Type[] methodtypes;
            if (method is System.Reflection.MethodInfo)
            {
                var mi = method as System.Reflection.MethodInfo;
                methodtypes = mi.GetParameters().Select(x => x.ParameterType).ToArray();
            }
            else if (method is System.Reflection.ConstructorInfo)
            {
                var ci = method as System.Reflection.ConstructorInfo;
                methodtypes = ci.GetParameters().Select(x => x.ParameterType).ToArray();
            }
            else if (method is System.Reflection.FieldInfo)
            {
                if (isWrite)
                    methodtypes = new Type[] { ((System.Reflection.FieldInfo)method).FieldType };
                else
                    methodtypes = new Type[0];
            }
            else if (method is System.Reflection.PropertyInfo)
            {
                var pi = method as System.Reflection.PropertyInfo;
                var indexTypes = pi.GetIndexParameters().Select(x => x.ParameterType);
                if (isWrite)
                    indexTypes = new Type[] { pi.PropertyType }.Concat(indexTypes);
                
                methodtypes = indexTypes.ToArray();
            }
            else
                throw new Exception($"Method is not supported for invoke: {method}");


            return InvokeRemoteMethodAsync(handle, type, methodstring, methodtypes, arguments, isWrite);
        }

        /// <summary>
        /// Sends a detach message to the remote side, invalidating the handle
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="handle">The remote handle.</param>
        public async Task InvokeDetachAsync(long handle)
        {
            await m_connection.SendPassthroughAsync(Command.DetachRemoteObject, new DetachRemoteObjectRequest(handle));
            await m_remoteObjects.RemoveRemoteHandleAsync(handle);
        }

        /// <summary>
        /// The RPC peer message handler
        /// </summary>
        /// <returns><c>true</c> if the message is handled, <c>false</c> otherwise.</returns>
        /// <param name="message">The message to handle.</param>
        protected async Task<bool> HandleMessage(ParsedMessage message)
        {
            if (message.Command == Command.InvokeRemoteMethod)
            {
                var arg = message.Arguments?.FirstOrDefault();
                if (arg is InvokeRemoteMethodRequest)
                {
                    m_connection.RegisterDanglingTask(
                        Task.Run(() =>
                            HandleRemoteInvocation(message.ID, (InvokeRemoteMethodRequest)arg)
                        )
                    );
                }
                else
                {
                    m_connection.RegisterDanglingTask(
                        Task.Run(() =>
                            m_connection.SendErrorResponseAsync(message.ID, message.Command, new Exception($"Invalid argument for the {nameof(Command.InvokeRemoteMethod)} request"))
                        )
                    );
                }
                return true;
            }
            else if (message.Command == Command.RegisterRemoteObject)
            {
                var arg = message.Arguments?.FirstOrDefault();
                if (arg is RegisterRemoteObjectRequest)
                {
                    var req = (RegisterRemoteObjectRequest)arg;
                    await m_remoteObjects.RegisterRemoteObjectAsync(req.ID, CreateRemoteProxy(req.ObjectType, req.ID));
                }
                else
                {
                    throw new Exception($"Invalid argument for the {nameof(Command.RegisterRemoteObject)} request");
                }
                return true;
            }
            else if (message.Command == Command.DetachRemoteObject)
            {
                var arg = message.Arguments?.FirstOrDefault();
                if (arg is DetachRemoteObjectRequest)
                {
                    var id = ((DetachRemoteObjectRequest)arg).ID;
                    await m_remoteObjects.RemoveLocalHandleAsync(id);
                }
                else
                {
                    System.Diagnostics.Debug.WriteLine($"Invalid argument for the {nameof(Command.DetachRemoteObject)} request");
                }
                return true;
            }

            return false;
        }

        /// <summary>
        /// Creates a remote proxy by invoking all the proxy generator methods
        /// </summary>
        /// <returns>The remote proxy.</returns>
        /// <param name="objectType">The type of proxy to create.</param>
        /// <param name="id">The remote proxy handle.</param>
        private IRemoteInstance CreateRemoteProxy(Type objectType, long id)
        {
            if (m_automaticProxies.TryGetValue(objectType, out var interfaceType))
                return AutomaticProxy.WrapRemote(this, objectType, interfaceType, id);

            for (var i = 0; i < m_remoteProxies.Count; i++)
            {
                Func<RPCPeer, Type, long, IRemoteInstance> method;
                lock (m_lock)
                {
                    if (i >= m_remoteProxies.Count)
                        break;
                    method = m_remoteProxies[i];
                }

                var res = method(this, objectType, id);
                if (res != null)
                    return res;
            }

            throw new Exception($"Cannot generate a proxy for type: {objectType}");
        }

        /// <summary>
        /// Handles the remote invocation request, can be overridden.
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="requestId">The requestID to respond to.</param>
        /// <param name="req">The invocation request.</param>
        protected virtual async Task HandleRemoteInvocation(long requestId, InvokeRemoteMethodRequest req)
        {
            // Flag to avoid sending communication errors over a failed link
            var hasResponded = false;

            try
            {
                var type = m_typeSerializer.ParseShortTypeName(req.Type);
                if (!IsTypeAllowed(type))
                    throw new Exception("Operations on that type are not allowed");

                var methodDefinition = m_typeSerializer.GetFromShortDefinition(type, req.Method);

                if (!IsMethodAllowed(methodDefinition, req.Arguments))
                    throw new Exception("Method not allowed");

                Type resType;

                if (methodDefinition is System.Reflection.MethodInfo)
                    resType = ((System.Reflection.MethodInfo)methodDefinition).ReturnType;
                else if (methodDefinition is System.Reflection.ConstructorInfo)
                    resType = type;
                else if (methodDefinition is System.Reflection.PropertyInfo)
                {
                    if (req.IsWrite)
                        resType = typeof(void);
                    else
                        resType = ((System.Reflection.PropertyInfo)methodDefinition).PropertyType;
                }
                else if (methodDefinition is System.Reflection.FieldInfo)
                {
                    if (req.IsWrite)
                        resType = typeof(void);
                    else
                        resType = ((System.Reflection.FieldInfo)methodDefinition).FieldType;
                }
                else
                    throw new Exception($"The reflected item is not supported: {methodDefinition}");

                object source = null;
                if (req.Handle != 0 && !m_remoteObjects.TryGetLocalObject(req.Handle, out source))
                    throw new Exception("The requested handle was not valid");

                object res = InvokeMethod(methodDefinition, source, req.Arguments, req.IsWrite);

                // We perform async tasks locally, and return the unwrapped result
                if (res != null && res is Task)
                {
                    await (Task)res;

                    // Unwrap the Task<T> value
                    if (res.GetType().IsConstructedGenericType)
                    {
                        res = res.GetType().GetProperty("Result").GetValue(res);
                    }
                    else
                    {
                        res = true;
                    }
                }

                // If we are sending an object back, register a local reference to it
                if (res != null && m_typeSerializer.GetAction(res.GetType()) == SerializationAction.Reference)
                {
                    if (await m_remoteObjects.RegisterLocalObjectAsync(res))
                        await m_connection.SendPassthroughAsync(Command.RegisterRemoteObject, new RegisterRemoteObjectRequest(res.GetType(), m_remoteObjects.GetLocalHandle(res)));
                }

                hasResponded = true;
                await m_connection.SendResponseAsync(requestId, Command.InvokeRemoteMethod, new InvokeRemoteMethodResponse(resType, res));
            }
            catch (Exception ex)
            {
                if (hasResponded)
                    throw;

                await m_connection.SendErrorResponseAsync(requestId, Command.InvokeRemoteMethod, ex);
            }

        }

        /// <summary>
        /// Plugin method that can be overriden to filter which types are safe to call.
        /// </summary>
        /// <returns><c>true</c>, if type was allowed, <c>false</c> otherwise.</returns>
        /// <param name="targetType">The type to evaluate.</param>
        protected virtual bool IsTypeAllowed(Type targetType)
        {
            // If there is a filter but no types, allow the filter to decide
            return m_allowedTypes.Count == 0 
                ? m_filterFunction != null
                : m_allowedTypes.Contains(targetType);
        }

        /// <summary>
        /// Plugin method that can be overriden to filter individual methods
        /// </summary>
        /// <returns><c>true</c>, if method was allowed, <c>false</c> otherwise.</returns>
        /// <param name="method">The method to evaluate.</param>
        /// <param name="arguments">The arguments to the method.</param>
        protected virtual bool IsMethodAllowed(System.Reflection.MemberInfo method, object[] arguments)
        {
            if (m_filterFunction == null)
                return false;
            
            return m_filterFunction(method, arguments);
        }

        /// <summary>
        /// Method that does the actual remote invocation. This can be overriden to make a more granular permission system than just filtering by the types.
        /// </summary>
        /// <returns>The result of invoking the method.</returns>
        /// <param name="method">Method.</param>
        /// <param name="instance">Instance.</param>
        /// <param name="isWrite">If set to <c>true</c> is write.</param>
        protected virtual object InvokeMethod(System.Reflection.MemberInfo method, object instance, object[] arguments, bool isWrite)
        {
            if (method is System.Reflection.MethodInfo)
                return ((System.Reflection.MethodInfo)method).Invoke(instance, arguments);
            else if (method is System.Reflection.ConstructorInfo)
                return ((System.Reflection.ConstructorInfo)method).Invoke(arguments);
            else if (method is System.Reflection.PropertyInfo)
            {
                if (((System.Reflection.PropertyInfo)method).GetIndexParameters().Length == 0)
                {
                    if (isWrite)
                    {
                        ((System.Reflection.PropertyInfo)method).SetValue(instance, arguments[0]);
                        return null;
                    }
                    else
                        return ((System.Reflection.PropertyInfo)method).GetValue(instance);
                }
                else
                {
                    if (isWrite)
                    {
                        ((System.Reflection.PropertyInfo)method).SetValue(instance, arguments[0], arguments.Skip(1).ToArray());
                        return null;
                    }
                    else
                    {
                        return ((System.Reflection.PropertyInfo)method).GetValue(instance, arguments);
                    }
                }
            }
            else if (method is System.Reflection.FieldInfo)
            {
                if (isWrite)
                {
                    ((System.Reflection.FieldInfo)method).SetValue(instance, arguments[0]);
                    return null;
                }
                else
                    return ((System.Reflection.FieldInfo)method).GetValue(instance);

            }

            throw new Exception($"The reflected item is not supported: {method}");

        }

        /// <summary>
        /// Releases all resource used by the <see cref="T:LeanIPC.RPCClient"/> object.
        /// </summary>
        /// <remarks>Call <see cref="Dispose"/> when you are finished using the <see cref="T:LeanIPC.RPCClient"/>. The
        /// <see cref="Dispose"/> method leaves the <see cref="T:LeanIPC.RPCClient"/> in an unusable state. After
        /// calling <see cref="Dispose"/>, you must release all references to the <see cref="T:LeanIPC.RPCClient"/> so
        /// the garbage collector can reclaim the memory that the <see cref="T:LeanIPC.RPCClient"/> was occupying.</remarks>
        public void Dispose()
        {
            m_connection?.Dispose();
        }
    }
}
