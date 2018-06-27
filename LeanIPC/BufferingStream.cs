using System;
using System.IO;

namespace LeanIPC
{
    /// <summary>
    /// Stream that buffers all writes until a fill threshold, or flush is explicitly called.
    /// This is a slightly different implementation than <seealso cref="BufferedStream"/> in that his class just passes the reader directly through.
    /// </summary>
    public class BufferingStream : Stream
    {
        /// <summary>
        /// The parent stream
        /// </summary>
        private readonly Stream m_parent;

        /// <summary>
        /// The buffer
        /// </summary>
        private readonly byte[] m_buffer;

        /// <summary>
        /// The number of bytes in the buffer
        /// </summary>
        private int m_buffercount = 0;

        /// <summary>
        /// Initializes a new instance of the <see cref="T:LeanIPC.BufferingStream"/> class.
        /// </summary>
        /// <param name="parent">The stream we should buffer.</param>
        /// <param name="buffersize">The size of the write buffer.</param>
        public BufferingStream(Stream parent, int buffersize = 16 * 1024)
        {
            if (parent == null)
                throw new ArgumentNullException();
            if (buffersize <= 0)
                throw new ArgumentOutOfRangeException(nameof(buffersize), buffersize, $"The {nameof(buffersize)} must be at least one");

            m_parent = parent;
            m_buffer = new byte[buffersize];
        }

        /// <summary>
        /// Gets a value indicating whether this <see cref="T:LeanIPC.BufferingStream"/> can be read.
        /// </summary>
        public override bool CanRead => m_parent.CanRead;
        /// <summary>
        /// Gets a value indicating whether this <see cref="T:LeanIPC.BufferingStream"/> can seek.
        /// </summary>
        public override bool CanSeek => false;
        /// <summary>
        /// Gets a value indicating whether this <see cref="T:LeanIPC.BufferingStream"/> can be written.
        /// </summary>
        public override bool CanWrite => m_parent.CanWrite;
        /// <summary>
        /// Gets the length of the stream.
        /// </summary>
        public override long Length => throw new NotImplementedException();
        /// <summary>
        /// Gets or sets the position inside the stream.
        /// </summary>
        public override long Position { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        /// <summary>
        /// Flushes the buffer to the parent stream
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="cancellationToken">The cancellation token.</param>
        public override async System.Threading.Tasks.Task FlushAsync(System.Threading.CancellationToken cancellationToken)
        {
            await m_parent.WriteAsync(m_buffer, 0, m_buffercount);
            m_buffercount = 0;
            await m_parent.FlushAsync(cancellationToken);
        }
        /// <summary>
        /// Flush the buffer to the parent stream
        /// </summary>
        public override void Flush()
        {
            m_parent.Write(m_buffer, 0, m_buffercount);
            m_buffercount = 0;
            m_parent.Flush();
        }

        /// <summary>
        /// Reads bytes into the provided buffer
        /// </summary>
        /// <returns>The number of bytes read.</returns>
        /// <param name="buffer">The buffer to read into.</param>
        /// <param name="offset">The offset into the buffer to start reading.</param>
        /// <param name="count">The maximum number of bytes to read.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        public override System.Threading.Tasks.Task<int> ReadAsync(byte[] buffer, int offset, int count, System.Threading.CancellationToken cancellationToken)
        {
            return m_parent.ReadAsync(buffer, offset, count, cancellationToken);
        }

        /// <summary>
        /// Reads bytes into the provided buffer
        /// </summary>
        /// <returns>The number of bytes read.</returns>
        /// <param name="buffer">The buffer to read into.</param>
        /// <param name="offset">The offset into the buffer to start reading.</param>
        /// <param name="count">The maximum number of bytes to read.</param>
        public override int Read(byte[] buffer, int offset, int count)
        {
            return m_parent.Read(buffer, offset, count);
        }

        /// <summary>
        /// Seeks the stream to the given offset
        /// </summary>
        /// <returns>The new position.</returns>
        /// <param name="offset">The offset to seek.</param>
        /// <param name="origin">The origin where seeking starts.</param>
        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Sets the length of the stream.
        /// </summary>
        /// <param name="value">The new stream length.</param>
        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Writes bytes from the buffer to the stream
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="buffer">The buffer with the data to write.</param>
        /// <param name="offset">The offset into the buffer.</param>
        /// <param name="count">The number of bytes to write.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        public override async System.Threading.Tasks.Task WriteAsync(byte[] buffer, int offset, int count, System.Threading.CancellationToken cancellationToken)
        {
            while (count > 0)
            {
                var toWrite = Math.Min(count, m_buffer.Length - m_buffercount);
                Array.Copy(buffer, offset, m_buffer, m_buffercount, toWrite);
                m_buffercount += toWrite;
                offset += toWrite;
                count -= toWrite;

                // If we filled the buffer, lets flush
                if (m_buffercount == m_buffer.Length)
                    await FlushAsync(cancellationToken);
            }
        }

        /// <summary>
        /// Writes bytes from the buffer to the stream
        /// </summary>
        /// <param name="buffer">The buffer with the data to write.</param>
        /// <param name="offset">The offset into the buffer.</param>
        /// <param name="count">The number of bytes to write.</param>
        public override void Write(byte[] buffer, int offset, int count)
        {
            while (count > 0)
            {
                var toWrite = Math.Min(count, m_buffer.Length - m_buffercount);
                Array.Copy(buffer, offset, m_buffer, m_buffercount, toWrite);
                m_buffercount += toWrite;
                offset += toWrite;
                count -= toWrite;

                // If we filled the buffer, lets flush
                if (m_buffercount == m_buffer.Length)
                    Flush();
            }
        }
    }
}
