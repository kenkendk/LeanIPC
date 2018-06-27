using System;
using System.IO;
using System.Threading.Tasks;

namespace LeanIPC
{
    /// <summary>
    /// A wrapper for the full <see cref="T:LeanIPC.InterProcessConnection"/> class, exposing only user message methods
    /// </summary>
    public class IPCPeer : IDisposable
    {
        /// <summary>
        /// The underlying connection
        /// </summary>
        private readonly InterProcessConnection m_connection;

        /// <summary>
        /// Gets the type serializer used for this connection.
        /// </summary>
        public TypeSerializer TypeSerializer => m_connection.TypeSerializer;

        /// <summary>
        /// The remote object handler
        /// </summary>
        public RemoteObjectHandler RemoteHandler => m_connection.RemoteHandler;

        /// <summary>
        /// Initializes a new instance of the <see cref="T:LeanIPC.IPCPeer"/> class.
        /// Use this method if the stream is independently bi-directional (i.e. a socket)
        /// </summary>
        /// <param name="stream">The stream to use for reading and writing.</param>
        /// <param name="authenticationHandler">The authentication code to use</param>
        /// <param name="serializer">The serializer to use</param>
        public IPCPeer(Stream stream, TypeSerializer serializer = null, IAuthenticationHandler authenticationHandler = null)
            : this(stream, stream, serializer, authenticationHandler)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="T:LeanIPC.IPCPeer"/> class.
        /// </summary>
        /// <param name="reader">The stream to read from.</param>
        /// <param name="reader">The stream to write to.</param>
        /// <param name="authenticationHandler">The authentication code to use</param>
        /// <param name="serializer">The serializer to use</param>
        /// <param name="remoteHandler">The remote object handler to use</param>
        public IPCPeer(Stream reader, Stream writer, RemoteObjectHandler remoteHandler, TypeSerializer serializer, IAuthenticationHandler authenticationHandler = null)
            : this(new BinaryConverterStream(reader, serializer, remoteHandler), new BinaryConverterStream(writer, serializer, remoteHandler), authenticationHandler)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="T:LeanIPC.IPCPeer"/> class.
        /// </summary>
        /// <param name="reader">The stream to read from.</param>
        /// <param name="reader">The stream to write to.</param>
        /// <param name="authenticationHandler">The authentication code to use</param>
        /// <param name="serializer">The serializer to use</param>
        public IPCPeer(Stream reader, Stream writer, TypeSerializer serializer = null, IAuthenticationHandler authenticationHandler = null)
            : this(reader, writer, new RemoteObjectHandler(), serializer ?? new TypeSerializer(true, true), authenticationHandler)
        {
        }


        /// <summary>
        /// Initializes a new instance of the <see cref="T:LeanIPC.IPCPeer"/> class.
        /// </summary>
        /// <param name="reader">The stream to read from.</param>
        /// <param name="reader">The stream to write to.</param>
        /// <param name="authenticationHandler">The authentication code to use</param>
        public IPCPeer(BinaryConverterStream reader, BinaryConverterStream writer, IAuthenticationHandler authenticationHandler = null)
        {
            m_connection = new InterProcessConnection(reader, writer, authenticationHandler);
        }    

        /// <summary>
        /// Registers a new message handler
        /// </summary>
        /// <param name="handler">The handler to use.</param>
        public void AddMessageHandler(Func<ParsedMessage, Task<bool>> handler)
        {
            m_connection.AddMessageHandler(handler);
        }

        /// <summary>
        /// Unregisters a message handler
        /// </summary>
        /// <returns><c>true</c> if the handler was removed, <c>false</c> otherwise</returns>
        /// <param name="handler">The handler to remove.</param>
        public bool RemoveMessageHandler(Func<ParsedMessage, Task<bool>> handler)
        {
            return m_connection.RemoveMessageHandler(handler);
        }

        /// <summary>
        /// Registers a handler method for a specific type
        /// </summary>
        /// <param name="handler">The method to invoke when receiving data of type <typeparamref name="T"/>.</param>
        /// <typeparam name="T">The type of data to register for parameter.</typeparam>
        public void AddUserTypeHandler<T>(Func<long, T, Task<bool>> handler)
        {
            m_connection.AddUserTypeHandler<T>(handler);
        }

        /// <summary>
        /// Unregisters the handler for the specified type
        /// </summary>
        /// <returns><c>true</c>, if the handler was removed, <c>false</c> otherwise.</returns>
        /// <typeparam name="T">The type handler to deregister.</typeparam>
        public bool RemoveUserTypeHandler<T>()
        {
            return m_connection.RemoveUserTypeHandler<T>();
        }

        /// <summary>
        /// Sends a custom message to the other process and awaits the result
        /// </summary>
        /// <param name="data">The item to transmit</param>
        /// <param name="preEmitHandler">A method invoked prior to sending the message, while the send lock is being held</param>
        /// <param name="postEmitHandler">A method invoked after sending the message, while the send lock is being held</param>
        public Task<RequestHandlerResponse> SendAndWaitAsync(Type[] types, object[] data, Func<Task> preEmitHandler = null, Func<Task> postEmitHandler = null)
        {
            return m_connection.SendAndWaitAsync(types, data, preEmitHandler, postEmitHandler);
        }

        /// <summary>
        /// Sends a custom message to the other process and awaits the result
        /// </summary>
        /// <param name="data">The item to transmit</param>
        /// <param name="preEmitHandler">A method invoked prior to sending the message, while the send lock is being held</param>
        /// <param name="postEmitHandler">A method invoked after sending the message, while the send lock is being held</param>
        public Task<TOutput> SendAndWaitAsync<TInput, TOutput>(TInput data, Func<Task> preEmitHandler = null, Func<Task> postEmitHandler = null)
        {
            return m_connection.SendAndWaitAsync<TInput, TOutput>(data, preEmitHandler, postEmitHandler);
        }

        /// <summary>
        /// Submits a request, and waits for a reply
        /// </summary>
        /// <returns>The response arguments.</returns>
        /// <param name="data">The data to send</param>
        /// <typeparam name="T">The data type to transfer</typeparam>
        public Task SendPassthroughAsync<T>(T data, Func<Task> preEmitHandler = null, Func<Task> postEmitHandler = null)
        {
            return m_connection.SendPassthroughAsync(data, preEmitHandler, postEmitHandler);
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
            return m_connection.SendPassthroughAsync(types, data, preEmitHandler, postEmitHandler);
        }

        /// <summary>
        /// Runs the main loop, parsing input messages, reporting error for unknown messages.
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="asClient">A value indicating if this instance is connecting as a client</param>
        /// <param name="authenticationHandler">An optional authentication handler</param>
        public Task RunMainLoopAsync(bool asClient, IAuthenticationHandler authenticationHandler = null)
        {
            return m_connection.RunMainLoopAsync(asClient, authenticationHandler);
        }

        /// <summary>
        /// Runs the main loop, parsing input messages.
        /// </summary>
        /// <returns>The main loop.</returns>
        /// <param name="requestHandler">A method invoked for each received message.</param>
        /// <param name="asClient">A value indicating if this instance is connecting as a client</param>
        /// <param name="authenticationHandler">An optional authentication handler</param>
        public Task RunMainLoopAsync(RequestHandler requestHandler, bool asClient, IAuthenticationHandler authenticationHandler = null)
        {
            return m_connection.RunMainLoopAsync(requestHandler, asClient, authenticationHandler);
        }

        /// <summary>
        /// Sends a response to a previously received request
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="requestID">The ID of the request we are responding to</param>
        /// <param name="types">The response data types</param>
        /// <param name="arguments">The response data.</param>
        /// <typeparam name="T">The response data type parameter.</typeparam>
        public Task SendResponseAsync(long requestID, Type[] types, object[] arguments)
        {
            return m_connection.SendResponseAsync(requestID, Command.UserData, types, arguments);
        }

        /// <summary>
        /// Sends a response to a previously received request
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="requestID">The ID of the request we are responding to</param>
        /// <param name="response">The response data.</param>
        /// <typeparam name="T">The response data type parameter.</typeparam>
        public Task SendErrorResponseAsync(long requestID, Exception response)
        {
            return m_connection.SendErrorResponseAsync(requestID, Command.UserData, response);
        }

        /// <summary>
        /// Sends a response to a previously received request
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="requestID">The ID of the request we are responding to</param>
        /// <param name="response">The response data.</param>
        /// <typeparam name="T">The response data type parameter.</typeparam>
        public Task SendResponseAsync<T>(long requestID, T response)
        {
            return m_connection.SendResponseAsync(requestID, Command.UserData, new Type[] { typeof(T) }, new object[] { response });
        }

        /// <summary>
        /// Sends a response to a previously received request without content
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="requestID">The ID of the request we are responding to</param>
        public Task SendResponseAsync(long requestID)
        {
            return m_connection.SendResponseAsync(requestID, Command.UserData, null, null);
        }

        /// <summary>
        /// Shutdowns the connection.
        /// </summary>
        /// <returns>An awaitable task.</returns>
        public Task ShutdownAsync()
        {
            return m_connection.ShutdownAsync();
        }

        /// <summary>
        /// Releases all resource used by the <see cref="T:LeanIPC.IPCPeer"/> object.
        /// </summary>
        /// <remarks>Call <see cref="Dispose"/> when you are finished using the <see cref="T:LeanIPC.IPCPeer"/>. The
        /// <see cref="Dispose"/> method leaves the <see cref="T:LeanIPC.IPCPeer"/> in an unusable state. After calling
        /// <see cref="Dispose"/>, you must release all references to the <see cref="T:LeanIPC.IPCPeer"/> so the garbage
        /// collector can reclaim the memory that the <see cref="T:LeanIPC.IPCPeer"/> was occupying.</remarks>
        public void Dispose()
        {
            m_connection.Dispose();
        }
    }
}
