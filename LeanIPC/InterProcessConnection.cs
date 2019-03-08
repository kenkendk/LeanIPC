using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace LeanIPC
{
    /// <summary>
    /// Encapsulates all commands that can be send or received
    /// </summary>
    public class InterProcessConnection : IDisposable
    {
        /// <summary>
        /// Flag to enable trace output to console
        /// </summary>
        public bool TRACE_TO_CONSOLE = false;

        /// <summary>
        /// An interval to determine how often to poll the spawned process
        /// </summary>
        private static readonly TimeSpan POLL_INTERVAL =
#if DEBUG
            TimeSpan.FromSeconds(200);
#else
            TimeSpan.FromSeconds(120);
#endif

        /// <summary>
        /// The interval to wait for a response from the spawned process
        /// </summary>
        private static readonly TimeSpan RESPONSE_TIMEOUT = 
#if DEBUG
            TimeSpan.FromSeconds(10);
#else
            TimeSpan.FromSeconds(60);
#endif

        /// <summary>
        /// The reader stream instance
        /// </summary>
        private readonly BinaryConverterStream m_reader;

        /// <summary>
        /// The writer stream instance
        /// </summary>
        private readonly BinaryConverterStream m_writer;


        /// <summary>
        /// The lock use to guard against multiple writers
        /// </summary>
        private readonly AsyncLock m_lock = new AsyncLock();

        /// <summary>
        /// The current request ID
        /// </summary>
        private long m_requestID = 1;

        /// <summary>
        /// The authentication code to use
        /// </summary>
        private readonly IAuthenticationHandler m_authenticationHandler;

        /// <summary>
        /// A task used to signal that the initializer has completed
        /// </summary>
        private TaskCompletionSource<bool> m_initialized = new TaskCompletionSource<bool>();

        /// <summary>
        /// The list of pending requests
        /// </summary>
        private Dictionary<long, TaskCompletionSource<ParsedMessage>> m_requestQueue = new Dictionary<long, TaskCompletionSource<ParsedMessage>>();
        /// <summary>
        /// The list of pending responses
        /// </summary>
        private Dictionary<long, TaskCompletionSource<bool>> m_responseQueue = new Dictionary<long, TaskCompletionSource<bool>>();
        /// <summary>
        /// The number of requests sent to the remote, but without a response
        /// </summary>
        private int m_activeRequests = 0;

        /// <summary>
        /// The maximum number of requests emitted with no response
        /// </summary>
        private int m_maxPendingRequests = -1;
        /// <summary>
        /// Lock to enforce <see cref="m_maxPendingRequests"/> and ensure order or requests
        /// </summary>
        private readonly AsyncLock m_maxRequestLock = new AsyncLock();
        /// <summary>
        /// A signal to allow blocked requests to continue once a pending request is completed
        /// </summary>
        private TaskCompletionSource<bool> m_maxRequestWaiter = null;

        /// <summary>
        /// The list of request handlers
        /// </summary>
        private readonly List<Func<ParsedMessage, Task<bool>>> m_requestHandlers = new List<Func<ParsedMessage, Task<bool>>>();
        /// <summary>
        /// Lookup table to allow de-registering a typed request handler
        /// </summary>
        private readonly Dictionary<Type, Func<ParsedMessage, Task<bool>>> m_typeRequestHandlers = new Dictionary<Type, Func<ParsedMessage, Task<bool>>>();

        /// <summary>
        /// A list of tasks that are started, but are not directly monitored, because the handling must be out-of-order
        /// </summary>
        private readonly List<Task> m_danglingTask = new List<Task>();

        /// <summary>
        /// The lock that guards the danglingTask array
        /// </summary>
        private readonly object m_danglingTaskLock = new object();

        /// <summary>
        /// A flag signaling if this is the client instance
        /// </summary>
        private bool m_isClient;

        /// <summary>
        /// A flag signaling if the connection is shut down normally
        /// </summary>
        private bool m_isShutDown;

        /// <summary>
        /// A flag signaling if the connection has been started
        /// </summary>
        private bool m_isStarted;

        /// <summary>
        /// Initializes a new instance of the <see cref="T:LeanIPC.InterProcessConnection"/> class.
        /// Use this method if the stream is independently bi-directional (i.e. a socket)
        /// </summary>
        /// <param name="stream">The stream to use for reading and writing.</param>
        /// <param name="serializer">The serializer instance to use</param>
        /// <param name="authenticationHandler">The authentication code to use</param>
        public InterProcessConnection(Stream stream, TypeSerializer serializer = null, IAuthenticationHandler authenticationHandler = null)
            : this(stream, stream, serializer, authenticationHandler)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="T:LeanIPC.InterProcessConnection"/> class.
        /// </summary>
        /// <param name="reader">The stream to read from.</param>
        /// <param name="reader">The stream to write to.</param>
        /// <param name="serializer">The serializer instance to use</param>
        /// <param name="authenticationHandler">The authentication code to use</param>
        /// <param name="remoteHandler">The remote object handler to use</param>
        public InterProcessConnection(Stream reader, Stream writer, RemoteObjectHandler remoteHandler, TypeSerializer serializer, IAuthenticationHandler authenticationHandler = null)
            : this(new BinaryConverterStream(reader, serializer, remoteHandler), new BinaryConverterStream(writer, serializer, remoteHandler), authenticationHandler)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="T:LeanIPC.InterProcessConnection"/> class.
        /// </summary>
        /// <param name="reader">The stream to read from.</param>
        /// <param name="reader">The stream to write to.</param>
        /// <param name="serializer">The serializer instance to use</param>
        /// <param name="authenticationHandler">The authentication code to use</param>
        public InterProcessConnection(Stream reader, Stream writer, TypeSerializer serializer = null, IAuthenticationHandler authenticationHandler = null)
            : this(reader, writer, new RemoteObjectHandler(), serializer ?? new TypeSerializer(true, true), authenticationHandler)
        {
        }

        /// <summary>
        /// A task that can be used to wait for the initial handshake to complete
        /// </summary>
        /// <returns>The awaitable task.</returns>
        public Task WaitForHandshakeAsync()
        {
            return m_initialized.Task;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="T:LeanIPC.InterProcessConnection"/> class.
        /// </summary>
        /// <param name="reader">The stream to read from.</param>
        /// <param name="reader">The stream to write to.</param>
        /// <param name="authenticationHandler">The authentication code to use</param>
        public InterProcessConnection(BinaryConverterStream reader, BinaryConverterStream writer, IAuthenticationHandler authenticationHandler = null)
        {
            m_reader = reader ?? throw new ArgumentNullException(nameof(reader));
            m_writer = writer ?? throw new ArgumentNullException(nameof(writer));
            m_authenticationHandler = authenticationHandler ?? new NoAuthenticationHandler();
            m_requestHandlers.Add(HandlePingRequests);
            m_requestHandlers.Add(HandleShutdownRequests);
        }

        /// <summary>
        /// Registers a task as dangling. The registered tasks will be monitored for completion.
        /// Any failed tasks will cause the connection to terminate.
        /// </summary>
        /// <param name="task">The task to register.</param>
        public void RegisterDanglingTask(Task task)
        {
            lock (m_danglingTaskLock)
                m_danglingTask.Add(task);
        }

        /// <summary>
        /// Gets the type serializer used by the connection
        /// </summary>
        public TypeSerializer TypeSerializer => m_reader.TypeSerializer;

        /// <summary>
        /// Gets the remote object handler
        /// </summary>
        public RemoteObjectHandler RemoteHandler => m_reader.RemoteHandler;

        /// <summary>
        /// Gets or sets the maximum number of pending requests.
        /// </summary>
        public int MaxPendingRequests
        {
            get => m_maxPendingRequests;
            set
            {
                m_maxPendingRequests = value;
                TriggerPendingRequests().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Triggers re-evaluation of blocked pending requests
        /// </summary>
        /// <returns>An awaitable task.</returns>
        private async Task TriggerPendingRequests()
        {
            using (await m_lock.LockAsync())
            {
                if (m_maxRequestWaiter != null)
                {
                    m_maxRequestWaiter.TrySetResult(true);
                    m_maxRequestWaiter = null;
                }
            }
        }

        /// <summary>
        /// Registers a new message handler
        /// </summary>
        /// <param name="handler">The handler to use.</param>
        public void AddMessageHandler(Func<ParsedMessage, Task<bool>> handler)
        {
            m_requestHandlers.Add(handler);
        }

        /// <summary>
        /// Unregisters a message handler
        /// </summary>
        /// <returns><c>true</c> if the handler was removed, <c>false</c> otherwise</returns>
        /// <param name="handler">The handler to remove.</param>
        public bool RemoveMessageHandler(Func<ParsedMessage, Task<bool>> handler)
        {
            return m_requestHandlers.Remove(handler);
        }

        /// <summary>
        /// Registers a handler method for a specific type
        /// </summary>
        /// <param name="handler">The method to invoke when receiving data of type <typeparamref name="T"/>.</param>
        /// <typeparam name="T">The type of data to register for parameter.</typeparam>
        public void AddUserTypeHandler<T>(Func<long, T, Task<bool>> handler)
        {
            if (m_typeRequestHandlers.ContainsKey(typeof(T)))
                throw new ArgumentException($"There is another handler registered for type {typeof(T)}, please unregister it first", nameof(handler));

            m_requestHandlers.Add(m_typeRequestHandlers[typeof(T)] = message => {
                if (
                    message.Command == Command.UserData
                    && message.Types != null
                    && message.Types.Length == 1
                    && typeof(T).IsAssignableFrom(message.Types[0]))
                {
                    return handler(message.ID, (T)message.Arguments[0]);
                }

                return Task.FromResult(false);
            });
        }

        /// <summary>
        /// Unregisters the handler for the specified type
        /// </summary>
        /// <returns><c>true</c>, if the handler was removed, <c>false</c> otherwise.</returns>
        /// <typeparam name="T">The type handler to deregister.</typeparam>
        public bool RemoveUserTypeHandler<T>()
        {
            if (!m_typeRequestHandlers.TryGetValue(typeof(T), out var f))
                return false;

            m_typeRequestHandlers.Remove(typeof(T));
            return m_requestHandlers.Remove(f);
        }

        /// <summary>
        /// Registers a new request, and allocates an ID for it
        /// </summary>
        /// <returns>The pending request.</returns>
        /// <param name="id">The ID of the new request.</param>
        protected KeyValuePair<Task<ParsedMessage>, TaskCompletionSource<bool>> RegisterPendingRequest(out long id)
        {
            if (m_isShutDown)
                throw new ObjectDisposedException("Cannot send new requests when the connection is closed");
            
            id = System.Threading.Interlocked.Increment(ref m_requestID);
            return
                new KeyValuePair<Task<ParsedMessage>, TaskCompletionSource<bool>>(
                    (m_requestQueue[id] = new TaskCompletionSource<ParsedMessage>()).Task,
                    m_responseQueue[id] = new TaskCompletionSource<bool>()
                );
        }

        /// <summary>
        /// Sends a custom message to the other process and awaits the result
        /// </summary>
        /// <param name="data">The item to transmit</param>
        /// <param name="preEmitHandler">A method invoked prior to sending the message, while the send lock is being held</param>
        /// <param name="postEmitHandler">A method invoked after sending the message, while the send lock is being held</param>
        public Task<RequestHandlerResponse> SendAndWaitAsync(Type[] types, object[] data, Func<Task> preEmitHandler = null, Func<Task> postEmitHandler = null)
        {
            return SendAndWaitAsync(async reqid =>
            {
                if (preEmitHandler != null)
                    await preEmitHandler();
                
                System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(m_isClient ? 'C' : 'S')}: Sending user-data command with ID {reqid}");

                await InterprocessMessages.WriteRequestAsync(m_writer, reqid, Command.UserData, types, data);
                if (postEmitHandler != null)
                    await postEmitHandler();
            });
        }

        /// <summary>
        /// Sends a custom message to the other process and awaits the result
        /// </summary>
        /// <param name="data">The item to transmit</param>
        /// <param name="preEmitHandler">A method invoked prior to sending the message, while the send lock is being held</param>
        /// <param name="postEmitHandler">A method invoked after sending the message, while the send lock is being held</param>
        public async Task<TOutput> SendAndWaitAsync<TInput, TOutput>(TInput data, Func<Task> preEmitHandler = null, Func<Task> postEmitHandler = null)
        {
            return (TOutput)(await SendAndWaitAsync(new Type[] { typeof(TInput) }, new object[] { data }, preEmitHandler, postEmitHandler)).Values[0];
        }

        /// <summary>
        /// Submits a request, and waits for a reply
        /// </summary>
        /// <returns>The response arguments.</returns>
        /// <param name="command">The command to send.</param>
        /// <param name="args">The arguments to send.</param>
        internal Task<RequestHandlerResponse> SendAndWaitAsync(Command command, Type[] types, object[] args)
        {
            return SendAndWaitAsync(reqid =>
            {
                System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(m_isClient ? 'C' : 'S')}: Sending request {reqid}: {command}");
                return InterprocessMessages.WriteRequestAsync(m_writer, reqid, command, types, args);
            });
        }

        /// <summary>
        /// Submits a request, and waits for a reply
        /// </summary>
        /// <returns>The response arguments.</returns>
        /// <param name="command">The command to send.</param>
        /// <param name="data">The argument to send.</param>
        internal Task<RequestHandlerResponse> SendAndWaitAsync<T>(Command command, T data)
        {
            return SendAndWaitAsync(reqid =>
            {
                System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(m_isClient ? 'C' : 'S')}: Sending command {reqid}: {command} with data {typeof(T)}");
                return InterprocessMessages.WriteRequestAsync(m_writer, reqid, command, new Type[] { typeof(T) }, new object[] { data });
            });
        }

        /// <summary>
        /// Submits a request, and waits for a reply
        /// </summary>
        /// <returns>The response arguments.</returns>
        /// <param name="request">The method used to create the request.</param>
        protected async Task<RequestHandlerResponse> SendAndWaitAsync(Func<long, Task> request)
        {
            await m_initialized.Task;

            KeyValuePair<Task<ParsedMessage>, TaskCompletionSource<bool>> tsk;
            long reqid;
            using (await m_lock.LockAsync())
                tsk = RegisterPendingRequest(out reqid);

            // Make sure we are not over queue limits
            if (m_maxPendingRequests > 0)
            {
                // Take and keep the lock, to ensure we are head-of-line,
                // the lock uses a list which ensures FIFO semantics
                using (await m_maxRequestLock.LockAsync())
                {
                    // Check the condition
                    while (m_maxPendingRequests > 0 && m_activeRequests >= m_maxPendingRequests)
                    {
                        Task waiter;

                        // Assign the waiter in the critical reqion
                        using (await m_lock.LockAsync())
                            waiter = (m_maxRequestWaiter = new TaskCompletionSource<bool>()).Task;

                        // Wait until there is space in the queue again
                        await waiter;
                    }

                    // This request is now active
                    System.Threading.Interlocked.Increment(ref m_activeRequests);
                }
            }
            else
            {
                // If we are not limiting, just increment to keep track
                System.Threading.Interlocked.Increment(ref m_activeRequests);
            }

            System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(m_isClient ? 'C' : 'S')}: Registered new task {reqid}");

            using (await m_lock.LockAsync())
                await request(reqid);

            System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(m_isClient ? 'C' : 'S')}: Waiting on response for {reqid}");
            ParsedMessage response;
            try
            {
                response = await tsk.Key;
            }
            catch(Exception ex)
            {
                // If the connection is closed due to dangling errors,
                // we need to slurp up the error here to ensure that it
                // is reported as well as the actual parsing result
                if (m_initialized.Task.IsFaulted)
                {
                    // If the reader was simply cancelled, we filter that
                    if (tsk.Key.IsCanceled)
                    {
                        await m_initialized.Task;
                    }
                    // Otherwise we return both errors
                    else
                    {
                        throw new AggregateException(m_initialized.Task.Exception, ex);
                    }
                }

                // Default is to just report whatever error happened here
                throw;
            }

            tsk.Value.TrySetResult(true);

            System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(m_isClient ? 'C' : 'S')}: Got response for {reqid}: {response.Type}-{response.Command}");

            if (response.Type == MessageType.Error)
                throw response.Exception ?? new Exception("Null exception in error response from remote host");

            return new RequestHandlerResponse(response.Types, response.Arguments);
        }


        /// <summary>
        /// Submits a request, and waits for a reply
        /// </summary>
        /// <returns>The response arguments.</returns>
        /// <param name="data">The data to send</param>
        /// <typeparam name="T">The data type to transfer</typeparam>
        public Task SendPassthroughAsync<T>(T data, Func<Task> preEmitHandler = null, Func<Task> postEmitHandler = null)
        {
            return SendPassthroughAsync(new Type[] { typeof(T) }, new object[] { data }, preEmitHandler, postEmitHandler);
        }

        /// <summary>
        /// Sends a custom message to the other process and awaits the result
        /// </summary>
        /// <param name="types">The data types to transmit</param>
        /// <param name="data">The items to transmit</param>
        /// <param name="preEmitHandler">A method invoked prior to sending the message, while the send lock is being held</param>
        /// <param name="postEmitHandler">A method invoked after sending the message, while the send lock is being held</param>
        public Task SendPassthroughAsync(Type[] types, object[] data, Func<Task> preEmitHandler = null, Func<Task> postEmitHandler = null)
        {
            return SendPassthroughAsync(Command.UserData, types, data, preEmitHandler, postEmitHandler);
        }

        /// <summary>
        /// Submits a request, and waits for a reply
        /// </summary>
        /// <returns>The response arguments.</returns>
        /// <param name="command">The command to send.</param>
        /// <param name="data">The data to send.</param>
        /// <param name="preEmitHandler">A method invoked prior to sending the message, while the send lock is being held</param>
        /// <param name="postEmitHandler">A method invoked after sending the message, while the send lock is being held</param>
        internal async Task SendPassthroughAsync(Command command, Type[] types, object[] data, Func<Task> preEmitHandler = null, Func<Task> postEmitHandler = null)
        {
            await m_initialized.Task;
            
            if (m_isShutDown)
                throw new ObjectDisposedException("Cannot send new messages after the connection is closed");

            var id = System.Threading.Interlocked.Increment(ref m_requestID);
            System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(m_isClient ? 'C' : 'S')}: Registered new passthrough {id}");

            using (await m_lock.LockAsync())
            {
                if (preEmitHandler != null)
                    await preEmitHandler();

                System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(m_isClient ? 'C' : 'S')}: Sending user-data passthrough with ID {id}");
                await InterprocessMessages.WritePassthroughAsync(m_writer, id, command, types, data);

                if (postEmitHandler != null)
                    await postEmitHandler();
            }
        }

        /// <summary>
        /// Submits a request, and waits for a reply
        /// </summary>
        /// <returns>The response arguments.</returns>
        /// <param name="command">The command to send.</param>
        /// <param name="data">The argument to send.</param>
        /// <typeparam name="T">The type of data to send</typeparam>
        internal Task SendPassthroughAsync<T>(Command command, T data, Func<Task> preEmitHandler = null, Func<Task> postEmitHandler = null)
        {
            return SendPassthroughAsync(command, new Type[] { typeof(T) }, new object[] { data }, preEmitHandler, postEmitHandler);

        }

        /// <summary>
        /// Handles an incoming response message
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="response">The response message.</param>
        protected async Task HandleResponse(ParsedMessage response)
        {
            System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(m_isClient ? 'C' : 'S')}: Handling response {response.ID}: {response.Type}-{response.Command}");

            TaskCompletionSource<ParsedMessage> req;
            TaskCompletionSource<bool> resp;

            // Remove the pending request
            using (await m_lock.LockAsync())
            {
                if (!m_requestQueue.TryGetValue(response.ID, out req))
                    throw new Exception("Attempted to process non-existing request");
                if (!m_responseQueue.TryGetValue(response.ID, out resp))
                    throw new Exception("Attempted to process non-existing request");

                m_requestQueue.Remove(response.ID);
                m_responseQueue.Remove(response.ID);
                System.Threading.Interlocked.Decrement(ref m_activeRequests);

                // Signal that we have removed an item from the queue
                if (m_maxRequestWaiter != null)
                {
                    m_maxRequestWaiter.TrySetResult(true);
                    m_maxRequestWaiter = null;
                }
            }

            // Signal data is ready
            req.TrySetResult(response);

            // Return the lock-taken signal
            await resp.Task;
        }

        /// <summary>
        /// The ping request handler
        /// </summary>
        /// <returns><c>true</c> if the message is handled, <c>false</c> otherwise.</returns>
        /// <param name="message">Message.</param>
        protected virtual Task<bool> HandlePingRequests(ParsedMessage message)
        {
            if (message.Command == Command.Ping)
            {
                System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(m_isClient ? 'C' : 'S')}: Handling ping request, and sending ping response");

                Task.Run(() => {
                    RegisterDanglingTask(
                        SendResponseAsync(message.ID, message.Command, PingMessage.CreateMessage())
                    );
                });
                return Task.FromResult(true);
            }

            return Task.FromResult(false);
        }
        /// <summary>
        /// The shutdown request handler
        /// </summary>
        /// <returns><c>true</c> if the message is handled, <c>false</c> otherwise.</returns>
        /// <param name="message">Message.</param>
        protected virtual Task<bool> HandleShutdownRequests(ParsedMessage message)
        {
            if (message.Command == Command.Shutdown)
            {
                System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(m_isClient ? 'C' : 'S')} - Handling shutdown request, and sending shutdown response");

                RegisterDanglingTask(
                    Task.Run(async () => {
                        System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(m_isClient ? 'C' : 'S')}: Sending shutdown response");
                        await SendResponseAsync(message.ID, message.Command, new ShutdownMessage());
                        System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(m_isClient ? 'C' : 'S')}: Sent shutdown response, disposing");
                        m_isShutDown = true;
                        Dispose();
                    })
                );


                return Task.FromResult(true);
            }

            return Task.FromResult(false);
        }

        /// <summary>
        /// Helper method to make await with timeout
        /// </summary>
        /// <returns>The non-timeout result.</returns>
        /// <param name="task">The task to wait for</param>
        /// <param name="timeout">The timeout to apply.</param>
        /// <typeparam name="T">The return type parameter.</typeparam>
        private static async Task<T> WaitWithTimeout<T>(Task<T> task, TimeSpan timeout)
        {
            if (await Task.WhenAny(task, Task.Delay(timeout)) != task)
                throw new TimeoutException();
            
            return await task;
        }

        /// <summary>
        /// Runs the initial handshake sequence
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="authenticationHandler">The authentication handler to use.</param>
        protected async Task InitialHandShake(IAuthenticationHandler authenticationHandler)
        {
            authenticationHandler = authenticationHandler ?? m_authenticationHandler;

            // Set up the initial handshake
            if (m_isClient)
            {
                System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, "C: Starting handshake");

                var requestCode = authenticationHandler.CreateRequest();

                // Send the initial request
                var reqid = System.Threading.Interlocked.Increment(ref m_requestID);
                await InterprocessMessages.WriteRequestAsync(m_writer, reqid, Command.Ready, new ReadyMessage(requestCode));

                System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, "C: Sent Ready Req");

                // Grab the response and validate the basics

                var resp = await WaitWithTimeout(InterprocessMessages.ReadRequestOrResponseAsync(m_reader), RESPONSE_TIMEOUT);
                var mref = resp.Arguments?.OfType<ReadyMessage>().FirstOrDefault();
                if (resp.Type != MessageType.Response || resp.Command != Command.Ready || !mref.HasValue)
                    throw new Exception("Initial response message was not correct");

                System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, "C: Read Ready Resp");

                var m = mref.Value;

                if (m.ProtocolVersion > ProtocolDefinitions.MAX_VERSION || m.ProtocolVersion < ProtocolDefinitions.MIN_VERSION)
                {
                    this.Dispose();
                    throw new Exception("The supplied protocol version is not supported");
                }

                // Verify that we can approve the server
                if (!authenticationHandler.ValidateResponse(requestCode, m.AuthenticationCode))
                {
                    var ex = new Exception("Failed to validate server credentials");
                    this.Dispose();
                    throw ex;
                }

                System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, "C: Accepting requests");

                // We are now ready to accept requests and responses
            }
            else
            {
                System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, "S: Starting handshake");

                // Wait for the client to initiate, then validate the initial request
                var req = await WaitWithTimeout(InterprocessMessages.ReadRequestOrResponseAsync(m_reader), RESPONSE_TIMEOUT);
                var mref = req.Arguments?.OfType<ReadyMessage>().FirstOrDefault();

                if (req.Type != MessageType.Request || req.Command != Command.Ready || !mref.HasValue)
                    throw new Exception("Initial response message was not correct");
                var m = mref.Value;

                System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, "S: Got ready message");

                if (m.ProtocolVersion > ProtocolDefinitions.MAX_VERSION || m.ProtocolVersion < ProtocolDefinitions.MIN_VERSION)
                {
                    var ex = new Exception("The supplied protocol version is not supported");
                    await SendErrorResponseAsync(req.ID, req.Command, ex);
                    this.Dispose();
                    throw ex;
                }

                System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, "S: Validating handshake");

                // Verify that we can approve the client
                if (!authenticationHandler.ValidateRequest(m.AuthenticationCode))
                {
                    var ex = new Exception("Failed to validate client credentials");
                    await SendErrorResponseAsync(req.ID, req.Command, ex);
                    this.Dispose();
                    throw ex;
                }


                System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, "S: Sending reponse");

                // All is well, send our part of the handshake and continue to the main loop
                await InterprocessMessages.WriteResponseAsync(m_writer, req.ID, req.Command, new ReadyMessage(authenticationHandler.CreateResponse(m.AuthenticationCode)));

                System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, "S: Accepting requests");
            }
        }

        /// <summary>
        /// Runs the main loop, parsing input messages, reporting error for unknown messages.
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="asClient">A value indicating if this instance is connecting as a client</param>
        /// <param name="authenticationHandler">An optional authentication handler</param>
        public Task RunMainLoopAsync(bool asClient, IAuthenticationHandler authenticationHandler = null)
        {
            return RunMainLoopAsync(m => throw new InvalidDataException(), asClient, authenticationHandler);
        }

        /// <summary>
        /// Runs the main loop, parsing input messages.
        /// </summary>
        /// <returns>The main loop.</returns>
        /// <param name="requestHandler">A method invoked for each received message.</param>
        /// <param name="asClient">A value indicating if this instance is connecting as a client</param>
        /// <param name="authenticationHandler">An optional authentication handler</param>
        public async Task RunMainLoopAsync(RequestHandler requestHandler, bool asClient, IAuthenticationHandler authenticationHandler = null)
        {
            if (requestHandler == null)
                throw new ArgumentNullException(nameof(requestHandler));
            if (m_isStarted)
                throw new InvalidOperationException($"Cannot call {nameof(RunMainLoopAsync)} more than once");

            m_isStarted = true;
            var task = Task.Run(() => RunMainLoopInnerAsync(requestHandler, asClient, authenticationHandler));
            RegisterDanglingTask(task);

            var danglerTask = RunDanglingTaskHelper(task);

            try
            {
                await task;
                System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(asClient ? "C" : "S")}: Main loop stopped (finished)");
            }
            catch (Exception aex)
            {
                if (aex is ConnectionClosedException || aex.InnerException is ConnectionClosedException)
                {
                    System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(asClient ? "C" : "S")}: Main loop stopped (instance disposed)");
                }
                else
                    throw;
            }
            finally
            {
                Dispose();
                await RetireAllPendingRequests();
            }

            await danglerTask;
        }

        /// <summary>
        /// Runs the main loop, parsing input messages.
        /// </summary>
        /// <returns>The main loop.</returns>
        /// <param name="requestHandler">A method invoked for each received message.</param>
        /// <param name="asClient">A value indicating if this instance is connecting as a client</param>
        /// <param name="authenticationHandler">An optional authentication handler</param>
        private async Task RunMainLoopInnerAsync(RequestHandler requestHandler, bool asClient, IAuthenticationHandler authenticationHandler = null)
        {
            m_isClient = asClient;

            try
            {
                await InitialHandShake(authenticationHandler);
                m_initialized.TrySetResult(true);
            }
            catch(Exception ex)
            {
                m_initialized.TrySetException(ex);
            }

            System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(asClient ? "C" : "S")}: Entering main loop");

            Task pongtask = null;

            while (true)
            {
                await m_initialized.Task;

                // No lock on the read, because we only access it from here
                var readtask = InterprocessMessages.ReadRequestOrResponseAsync(m_reader);

                if (pongtask != null && pongtask.IsCompleted)
                    pongtask = null;

                while (await Task.WhenAny(readtask, Task.Delay(pongtask == null ? POLL_INTERVAL : RESPONSE_TIMEOUT)) != readtask)
                {
                    await m_initialized.Task;

                    System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(asClient ? "C" : "S")}: Timeout occurred");
                    if (pongtask != null && !pongtask.IsCompleted)
                        throw new System.IO.IOException("Connection died, no ping response");

                    System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(asClient ? "C" : "S")}: Sending PING request");

                    // No response, send a ping
                    pongtask = SendPingRequestAsync();

                    if (await Task.WhenAny(readtask, Task.Delay(RESPONSE_TIMEOUT)) != readtask)
                        throw new Exception("Failed to get a ping response");
                }

                await m_initialized.Task;

                ParsedMessage msg;
                try { msg = await readtask; }
                catch(Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"Failed to parse message: {ex}");
                    throw;
                }

                System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(asClient ? "C" : "S")}: Got message {msg.Type} - {msg.Command} - {msg.ID}");

                if (msg.Type == MessageType.Request || msg.Type == MessageType.Passthrough)
                {
                    var done = false;
                    foreach (var h in m_requestHandlers)
                        if (await h(msg))
                        {
                            done = true;
                            break;
                        }

                    if (done)
                        continue;


                    if (msg.Command != Command.UserData)
                    {
                        // This is not supported, report an error
                        System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(asClient ? "C" : "S")}: Got unhandled request: {msg.Command} - {msg.ID}");

                        RegisterDanglingTask(
                            Task.Run(() => SendErrorResponseAsync(msg.ID, msg.Command, new Exception($"Operation {msg.Command} was not handled, perhaps it is not supported?")))
                        );
                    }
                    else
                    {
                        // If we fall through, allow the user method to handle the request
                        System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(asClient ? "C" : "S")}: Passing message to user handler");

                        // Passthrough do not await completion
                        if (msg.Type == MessageType.Passthrough)
                        {
                            RegisterDanglingTask(
                                Task.Run(() => requestHandler(msg))
                            );
                        }
                        else
                        {
                            RegisterDanglingTask(
                                Task.Run(async () =>
                                {
                                    // Don't send error messages if the connection is broken
                                    var handled = false;
                                    try
                                    {
                                        var n = await requestHandler(msg);

                                        handled = true;
                                        await SendResponseAsync(msg.ID, msg.Command, n.Types, n.Values);
                                    }
                                    catch (Exception ex)
                                    {
                                        System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(asClient ? "C" : "S")}: User handler error on {msg.Command} - {msg.ID}: {ex}");
                                        if (handled)
                                        {
                                            this.Dispose();
                                            throw;
                                        }

                                        await SendErrorResponseAsync(msg.ID, msg.Command, ex);
                                    }
                                })
                            );
                        }
                    }
                }
                else
                {
                    await HandleResponse(msg);
                }
            }
        }

        /// <summary>
        /// Runs through the list of dangling tasks, and removes completed tasks.
        /// Throws an exception if one or more tasks are failed.
        /// </summary>
        /// <param name="main">The main task</param>
        private async Task RunDanglingTaskHelper(Task main)
        {
            while (!main.IsCompleted)
            {
                await Task.WhenAny(main, Task.Delay(TimeSpan.FromSeconds(10)));

                if (m_danglingTask.Count > 0)
                {
                    List<Exception> gx = null;
                    lock (m_lock)
                        for (var i = 0; i < m_danglingTask.Count; i++)
                        {
                            if (m_danglingTask[i].Status == TaskStatus.RanToCompletion)
                                m_danglingTask.RemoveAt(i);
                            else if (m_danglingTask[i].Status == TaskStatus.Faulted)
                            {
                                gx = (gx ?? new List<Exception>());
                                gx.Add(m_danglingTask[i].Exception);
                            }
                        }

                    if (gx != null)
                    {
                        System.Diagnostics.Trace.WriteLine($"{(m_isClient ? 'C' : 'S')}: Dangling error found, terminating");
                        if (m_initialized.Task.Status == TaskStatus.WaitingForActivation)
                            m_initialized.TrySetException(new AggregateException(gx));
                        else
                            (m_initialized = new TaskCompletionSource<bool>()).TrySetException(gx);

                        return;
                    }
                }
            }

            System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(m_isClient ? 'C' : 'S')}: Terminating dangling handler");
        }

        /// <summary>
        /// Sends a periodic ping message
        /// </summary>
        /// <returns>An awaitable task.</returns>
        public virtual Task SendPingRequestAsync()
        {
            return SendAndWaitAsync(Command.Ping, PingMessage.CreateMessage());
        }

        /// <summary>
        /// Sends a response to a previously received request
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="requestID">The ID of the request we are responding to</param>
        /// <param name="command">The command that we are responding to.</param>
        /// <param name="types">The response data types</param>
        /// <param name="arguments">The response data.</param>
        /// <typeparam name="T">The response data type parameter.</typeparam>
        public async Task SendResponseAsync(long requestID, Command command, Type[] types, object[] arguments)
        {
            System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(m_isClient ? 'C' : 'S')}: Registering intent to respond to {requestID}: {command}");
            using (await m_lock.LockAsync())
            {
                System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(m_isClient ? 'C' : 'S')}: Got lock for intent to respond to {requestID}: {command}");
                await m_initialized.Task;
                try
                {
                    System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(m_isClient ? 'C' : 'S')}: Sending response to {requestID}: {command}");
                    await InterprocessMessages.WriteResponseAsync(m_writer, requestID, command, types, arguments);
                    System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(m_isClient ? 'C' : 'S')}: Has sent response to {requestID}: {command}");
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(m_isClient ? 'C' : 'S')}: Lost the connection while responding to {requestID}: {command}, error: {ex.Message}");
                    this.Dispose();
                    throw;
                }
            }
        }

        /// <summary>
        /// Sends a response to a previously received request
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="requestID">The ID of the request we are responding to</param>
        /// <param name="command">The command that we are responding to.</param>
        /// <param name="response">The response data.</param>
        /// <typeparam name="T">The response data type parameter.</typeparam>
        public async Task SendErrorResponseAsync(long requestID, Command command, Exception response)
        {
            using (await m_lock.LockAsync())
            {
                await m_initialized.Task;
                System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(m_isClient ? 'C' : 'S')}: Sending error response to {requestID}: {response}");
                try
                {
                    await InterprocessMessages.WriteErrorResponseAsync(m_writer, requestID, command, response);
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(m_isClient ? 'C' : 'S')}: Lost the connection while responding to {requestID}: {command}, error: {ex.Message}");
                    this.Dispose();
                    throw;
                }
            }
        }

        /// <summary>
        /// Sends a response to a previously received request
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="requestID">The ID of the request we are responding to</param>
        /// <param name="command">The command that we are responding to.</param>
        /// <param name="response">The response data.</param>
        /// <typeparam name="T">The response data type parameter.</typeparam>
        public Task SendResponseAsync<T>(long requestID, Command command, T response)
        {
            return SendResponseAsync(requestID, command, new Type[] { typeof(T) }, new object[] { response });
        }

        /// <summary>
        /// Sends a response to a previously received request without content
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="requestID">The ID of the request we are responding to</param>
        /// <param name="command">The command that we are responding to.</param>
        /// <typeparam name="T">The response data type parameter.</typeparam>
        public Task SendResponseAsync(long requestID, Command command)
        {
            return SendResponseAsync(requestID, command, null, null);
        }

        /// <summary>
        /// Shutdowns the connection.
        /// </summary>
        /// <returns>An awaitable task.</returns>
        public async Task ShutdownAsync()
        {
            System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(m_isClient ? 'C' : 'S')}: Shutting down");
            try
            {
                await SendAndWaitAsync(Command.Shutdown, new ShutdownMessage());
            }
            catch(Exception ex)
            {                
                System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(m_isClient ? 'C' : 'S')}: Shutdown failed: {ex}");
            }
            System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(m_isClient ? 'C' : 'S')}: Shutdown complete, disposing");
            m_isShutDown = true;
            Dispose();
            System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(m_isClient ? 'C' : 'S')}: Shutdown and dispose done");
        }

        /// <summary>
        /// Helper method that marks all pending requests as cancelled; should be called after shutting down the main loop.
        /// </summary>
        /// <returns>An awaitable task.</returns>
        private async Task RetireAllPendingRequests()
        {
            System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(m_isClient ? "C" : "S")}: Grabbing lock to cancel all requests");
            using (await m_lock.LockAsync())
            {
                System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(m_isClient ? "C" : "S")}: Cancelling {m_requestQueue.Count} requests");
                foreach (var t in m_requestQueue.Values)
                    t.TrySetCanceled();
                foreach (var t in m_responseQueue.Values)
                    t.TrySetCanceled();
                
                System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(m_isClient ? "C" : "S")}: Done cancelling all {m_requestQueue.Count} requests");

                System.Threading.Interlocked.Exchange(ref m_activeRequests, 0);
                m_requestQueue.Clear();
                m_responseQueue.Clear();
            }
        }

        /// <summary>
        /// Releases all resource used by the <see cref="T:LeanIPC.InterProcessConnection"/> object.
        /// </summary>
        /// <remarks>Call <see cref="Dispose"/> when you are finished using the <see cref="T:LeanIPC.InterProcessConnection"/>.
        /// The <see cref="Dispose"/> method leaves the <see cref="T:LeanIPC.InterProcessConnection"/> in an unusable
        /// state. After calling <see cref="Dispose"/>, you must release all references to the
        /// <see cref="T:LeanIPC.InterProcessConnection"/> so the garbage collector can reclaim the memory that the
        /// <see cref="T:LeanIPC.InterProcessConnection"/> was occupying.</remarks>
        public void Dispose()
        {
            System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(m_isClient ? 'C' : 'S')}: Disposing instance");

            if (m_initialized.Task.Status == TaskStatus.WaitingForActivation)
                m_initialized.TrySetCanceled();
            else if (m_initialized.Task.Status == TaskStatus.RanToCompletion)
                (m_initialized = new TaskCompletionSource<bool>()).TrySetCanceled();

            System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(m_isClient ? 'C' : 'S')}: Disposing instance channels");

            m_reader?.Dispose();
            m_writer?.Dispose();

            System.Diagnostics.Trace.WriteLineIf(TRACE_TO_CONSOLE, $"{(m_isClient ? 'C' : 'S')}: Finished dispose of instance");
        }
    }
}
