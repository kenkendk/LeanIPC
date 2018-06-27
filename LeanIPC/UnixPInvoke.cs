using System;
using System.Runtime.InteropServices;

namespace LeanIPC
{
    internal static class UnixPInvoke
    {
        /// <summary>
        /// Creates a buffer suitable to transfer the given number of file descriptors
        /// </summary>
        /// <returns>A buffer large enough to hold the handles.</returns>
        /// <param name="numberOfDescriptors">Number of file descriptors to make space for.</param>
        public static byte[] CreateAncillaryBuffer(int numberOfDescriptors)
        {
            //struct cmsghdr
            var size =
                IntPtr.Size + // size_t cmsg_len;
                IntPtr.Size + //int cmsg_level;
                IntPtr.Size + //int cmsg_type;
                IntPtr.Size + //safety buffer
                (IntPtr.Size * (numberOfDescriptors)); // int fd[n]

            return new byte[size];
        }

        /// <summary>
        /// Helper methods that probes for a working libancillary.so and returns false if none was found
        /// </summary>
        /// <returns><c>true</c>, if libancillary is found, <c>false</c> otherwise.</returns>
        public static bool TestForLibAncillary()
        {
            try
            {
                var h = dlopen("libancillary.so", RTLD_LAZY | RTLD_LOCAL);
                if (h != IntPtr.Zero)
                    dlclose(h);
                return h != IntPtr.Zero;
            }
            catch(Exception ex)
            {
                System.Diagnostics.Trace.WriteLine($"Failed to load library: {ex}");
            }

            return false;
        }

        /// <summary>
        /// Sends the file descriptors to the socket.
        /// </summary>
        /// <returns>The error code, zero indicates success.</returns>
        /// <param name="socket">The socket handle to send the data on.</param>
        /// <param name="fds">The file descriptors.</param>
        /// <param name="n_fds">The number of file descriptors.</param>
        /// <param name="buffer">A writeable memory area large enough to hold the required data structures.</param>
        [DllImport("ancillary", SetLastError=true, CallingConvention=CallingConvention.StdCall, CharSet=CharSet.None)]
        public static extern int ancil_send_fds_with_buffer(IntPtr socket, IntPtr[] fds, uint n_fds, byte[] buffer);

        /// <summary>
        /// Sends the file descriptors to the socket.
        /// </summary>
        /// <returns>The error code, zero indicates success.</returns>
        /// <param name="socket">The socket handle to send the data on.</param>
        /// <param name="fds">The file descriptors.</param>
        /// <param name="n_fds">The number of file descriptors.</param>
        [DllImport("ancillary", SetLastError = true, CallingConvention = CallingConvention.StdCall, CharSet = CharSet.None)]
        public static extern int ancil_send_fds(IntPtr socket, IntPtr[] fds, uint n_fds);

        /// <summary>
        /// Sends a file descriptor to the socket.
        /// </summary>
        /// <returns>The error code, zero indicates success.</returns>
        /// <param name="socket">The socket handle to send the data on.</param>
        /// <param name="fd">The file descriptor.</param>
        [DllImport("ancillary", SetLastError = true, CallingConvention = CallingConvention.StdCall, CharSet = CharSet.None)]
        public static extern int ancil_send_fd(IntPtr socket, IntPtr fd);

        /// <summary>
        /// Receives file descriptors from the socket
        /// </summary>
        /// <returns>The error code, negative means error, otherwise the number of fil.</returns>
        /// <param name="socket">The socket handle to read data from.</param>
        /// <param name="fds">The file descriptor storage area.</param>
        /// <param name="n_fds">The maximum number of descriptors to receive.</param>
        /// <param name="buffer">A writeable memory area large enough to hold the required data structures.</param>
        [DllImport("ancillary", SetLastError = true, CallingConvention = CallingConvention.StdCall, CharSet = CharSet.None)]
        public static extern int ancil_recv_fds_with_buffer(IntPtr socket, IntPtr[] fds, uint n_fds, byte[] buffer);

        /// <summary>
        /// Receives file descriptors from the socket
        /// </summary>
        /// <returns>The error code, negative means error, otherwise the number of file descriptors returned.</returns>
        /// <param name="socket">The socket handle to read data from.</param>
        /// <param name="fds">The file descriptor storage area.</param>
        /// <param name="n_fds">The maximum number of descriptors to receive.</param>
        [DllImport("ancillary", SetLastError = true, CallingConvention = CallingConvention.StdCall, CharSet = CharSet.None)]
        public static extern int ancil_recv_fds(IntPtr socket, IntPtr[] fds, uint n_fds);

        /// <summary>
        /// Receives a file descriptor from the socket
        /// </summary>
        /// <returns>The error code, negative means error, zero means success.</returns>
        /// <param name="socket">The socket handle to read data from.</param>
        /// <param name="fd">The file descriptor storage area.</param>
        [DllImport("ancillary", SetLastError = true, CallingConvention = CallingConvention.StdCall, CharSet = CharSet.None)]
        public static extern int ancil_recv_fd(IntPtr socket, ref IntPtr fd);

        /// <summary>
        /// Opens a shared library file
        /// </summary>
        /// <returns>A handle to the library or <c>IntPtr.Zero</c> if the library failed to open.</returns>
        /// <param name="file">The file to open.</param>
        /// <param name="mode">The mode.</param>
        [DllImport("ld", SetLastError = true, CallingConvention = CallingConvention.StdCall, CharSet = CharSet.None)]
        private static extern IntPtr dlopen(string file, int mode);

        /// <summary>
        /// Closes an open shared library file
        /// </summary>
        /// <returns>The result value, zero indicates success.</returns>
        /// <param name="handle">The handle to close.</param>
        [DllImport("ld", SetLastError = true, CallingConvention = CallingConvention.StdCall, CharSet = CharSet.None)]
        private static extern int dlclose(IntPtr handle);

        /// <summary>
        /// Instructs <see cref="dlopen"/> to not load all dependencies
        /// </summary>
        private const int RTLD_LAZY = 0x1;
        /// <summary>
        /// Instructs <see cref="dlopen"/> to load all dependencies
        /// </summary>
        private const int RTLD_NOW = 0x2;
        /// <summary>
        /// Instructs <see cref="dlopen"/> to load symbols into local table
        /// </summary>
        private const int RTLD_LOCAL = 0x4;
        /// <summary>
        /// Instructs <see cref="dlopen"/> to load symbols into global table
        /// </summary>
        private const int RTLD_GLOBAL = 0x8;
    }
}
