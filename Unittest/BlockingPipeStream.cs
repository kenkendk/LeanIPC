using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Unittest
{
    public class BlockingPipeStream : Stream
    {
        private const bool TRACE = false;

        private MemoryStream m_buffer = new MemoryStream();
        private readonly object m_lock = new object();
        private int m_readPos = 0;
        private TaskCompletionSource<bool> m_dataReadyTask = new TaskCompletionSource<bool>();
        private bool m_disposed;
        public readonly string Name;

        public BlockingPipeStream(string name = null)
        {
            Name = name;
        }

        public override bool CanRead => true;
        public override bool CanSeek => true;
        public override bool CanWrite => true;
        public override long Length => throw new NotImplementedException();
        public override long Position { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public override void Flush()
        {
        }

        private void ReclaimBuffers()
        {
            // If we have emptied the buffer, re-use it
            if (m_readPos == m_buffer.Length && m_buffer.Length > 1024 * 8)
            {
                System.Diagnostics.Trace.WriteLineIf(TRACE, $"* - Compacting buffer from {m_buffer.Length} bytes to zero");                
                m_buffer.SetLength(0);
                m_readPos = 0;
            }
            // If we are filling up on space, re-claim it
            else if (m_buffer.Length - m_readPos > 1024 * 1024)
            {
                var trailbytes = m_buffer.Length - m_readPos;
                System.Diagnostics.Trace.WriteLineIf(TRACE, $"* - Compacting buffer from {m_buffer.Length} bytes to {trailbytes}");

                var mnew = new MemoryStream();
                m_buffer.Position = m_readPos;
                m_buffer.CopyTo(mnew);
                if (mnew.Length != trailbytes)
                    throw new Exception("Bad copy?");
                m_buffer = mnew;
                m_readPos = 0;
            }
        }


        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, System.Threading.CancellationToken cancellationToken)
        {
            int read;
            while (!m_disposed || m_readPos != m_buffer.Length)
            {
                lock (m_lock)
                {
                    m_buffer.Position = m_readPos;
                    read = m_buffer.Read(buffer, offset, count);
                    offset += read;
                    count -= read;
                    m_readPos += read;

                    System.Diagnostics.Trace.WriteLineIf(TRACE && read != 0, $"* - Read {read} bytes of {read + count} from {Name}, remain: {m_buffer.Length - m_readPos}");

                    if (m_dataReadyTask.Task.Status != TaskStatus.WaitingForActivation)
                        m_dataReadyTask = new TaskCompletionSource<bool>();
                    
                    ReclaimBuffers();
                }

                if (read != 0)
                    return read;

                await m_dataReadyTask.Task;
            }

            return 0;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            return ReadAsync(buffer, offset, count).Result;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            lock (m_lock)
            {
                if (m_disposed)
                    throw new ObjectDisposedException(nameof(BlockingPipeStream));
                
                ReclaimBuffers();
                m_buffer.Position = m_buffer.Length;
                m_buffer.Write(buffer, offset, count);
                System.Diagnostics.Trace.WriteLineIf(TRACE, $"* - Wrote {count} bytes to {Name}, remain: {m_buffer.Length - m_readPos}");
                m_dataReadyTask.TrySetResult(true);
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (m_disposed)
                return;

            lock (m_lock)
            {
                if (m_disposed)
                    return;
                
                m_disposed = true;
                m_dataReadyTask.TrySetResult(true);
            }

            base.Dispose(disposing);
        }
    }
}
