using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;

namespace LeanIPC
{
    /// <summary>
    /// Encapsulation of a unix domain socket for transmitting handles via SCM_RIGHTS
    /// </summary>
    internal class UnixDomainSocket : IDisposable
    {
        /// <summary>
        /// The socket instance
        /// </summary>
        public readonly Socket Socket;

        /// <summary>
        /// A pre-allocated buffer
        /// </summary>
        private readonly byte[] m_buffer;

        /// <summary>
        /// The receive or send buffer
        /// </summary>
        private readonly IntPtr[] m_recvbuffer;

        /// <summary>
        /// Initializes a new instance of the <see cref="UnixDomainSocket"/> class.
        /// </summary>
        /// <param name="path">The path to connect to.</param>
        /// <param name="listen">If set to <c>true</c> bind socket, otherwise connect.</param>
        /// <param name="max_buffersize">The maximum number of items to send or receive</param>
        public UnixDomainSocket(string path, bool listen, int max_buffersize = 30)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentNullException(nameof(path));
            if (max_buffersize <= 0 || max_buffersize > 64 * 1024)
                throw new ArgumentOutOfRangeException(nameof(max_buffersize), max_buffersize, $"The buffer must be at least one and less than 64k");

            Socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.IP);
            if (listen)
            {
                System.Diagnostics.Debug.WriteLine("Binding socket to {0}", path);
                Socket.Bind(new UnixEndPoint(path));
                Socket.Listen(1);
            }
            else
            {
                System.Diagnostics.Debug.WriteLine("Connection socket to {0}", path);
                Socket.Connect(new UnixEndPoint(path));
            }

            // Allocate the buffer to use for the API calls
            m_buffer = UnixPInvoke.CreateAncillaryBuffer(max_buffersize);

            // Allocate the scratchpad buffer
            m_recvbuffer = new IntPtr[max_buffersize];
        }

        /// <summary>
        /// Releases all resource used by the <see cref="T:LeanIPC.UnixDomainSocket"/> object.
        /// </summary>
        /// <remarks>Call <see cref="Dispose"/> when you are finished using the <see cref="T:LeanIPC.UnixDomainSocket"/>. The
        /// <see cref="Dispose"/> method leaves the <see cref="T:LeanIPC.UnixDomainSocket"/> in an unusable state. After
        /// calling <see cref="Dispose"/>, you must release all references to the
        /// <see cref="T:LeanIPC.UnixDomainSocket"/> so the garbage collector can reclaim the memory that the
        /// <see cref="T:LeanIPC.UnixDomainSocket"/> was occupying.</remarks>
        public void Dispose()
        {
            if (Socket != null)
                Socket.Dispose();
        }

        /// <summary>
        /// Sends one or more file handles over the socket.
        /// </summary>
        /// <param name="handles">The handles to send.</param>
        /// <param name="offset">The offset into the source array</param>
        /// <param name="length">The number of handles to send</param>
        public void SendFileHandles(IntPtr[] handles, int offset, int length)
        {
            if (handles == null)
                throw new ArgumentNullException(nameof(handles));
            if (offset < 0)
                throw new ArgumentOutOfRangeException(nameof(offset), offset, $"The {nameof(offset)} must be positive");
            if (length < 0)
                throw new ArgumentOutOfRangeException(nameof(length), length, $"The {nameof(length)} must be positive");
            if (offset + length > handles.Length)
                throw new ArgumentOutOfRangeException(nameof(length), offset + length, $"The {nameof(offset)}+{nameof(length)} is larger than the size of the array");

            var remain = length;
            while (remain > 0)
            {
                var copylen = Math.Min(remain, m_recvbuffer.Length);

                if (handles != m_recvbuffer || offset != 0)
                    Array.Copy(handles, offset, m_recvbuffer, 0, copylen);

                var res = UnixPInvoke.ancil_send_fds_with_buffer(Socket.Handle, m_recvbuffer, (uint)copylen, m_buffer);
                if (res != 0)
                    throw new System.ComponentModel.Win32Exception(System.Runtime.InteropServices.Marshal.GetLastWin32Error());
                remain -= copylen;
            }
        }

        /// <summary>
        /// Sends the file handles over the socket
        /// </summary>
        /// <param name="handles">The handles to send.</param>
        public void SendFileHandles(params IntPtr[] handles)
        {
            if (handles == null)
                throw new ArgumentNullException(nameof(handles));
            SendFileHandles(handles, 0, handles.Length);
        }

        /// <summary>
        /// Receives a number of handles from the socket
        /// </summary>
        /// <returns>The recieved handles.</returns>
        public List<IntPtr> ReceiveHandles()
        {
            var res = UnixPInvoke.ancil_recv_fds_with_buffer(Socket.Handle, m_recvbuffer, (uint)m_recvbuffer.Length, m_buffer);
            if (res < 0 || res > m_recvbuffer.Length)
                throw new System.ComponentModel.Win32Exception(System.Runtime.InteropServices.Marshal.GetLastWin32Error());

            return new List<IntPtr>(m_recvbuffer.Take(res));
        }

        /// <summary>
        /// Receives a single handle from the socket
        /// </summary>
        /// <returns>The recieved handle or null.</returns>
        public IntPtr ReceiveHandle()
        {
            var res = UnixPInvoke.ancil_recv_fds_with_buffer(Socket.Handle, m_recvbuffer, 1u, m_buffer);
            if (res < 0 || res > m_recvbuffer.Length)
                throw new System.ComponentModel.Win32Exception(System.Runtime.InteropServices.Marshal.GetLastWin32Error());

            return res == 0 ? IntPtr.Zero : m_recvbuffer[0];
        }
    }
}
