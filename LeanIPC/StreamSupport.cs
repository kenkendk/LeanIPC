//using System;
//using System.IO;
//using System.Threading;
//using System.Threading.Tasks;

//namespace LeanIPC.StreamSupport
//{
//    /// <summary>
//    /// Interface for accessing a remote wrapped stream
//    /// </summary>
//    public interface IRemoteStream
//    {
//        long Length { get; set; }
//        long Position { get; set; }
//        bool CanRead { get; }
//        bool CanWrite { get; }
//        bool CanSeek { get; }
//        bool CanTimeout { get; }

//        int ReadTimeout { get; set; }
//        int WriteTimeout { get; set; }

//        byte[] Read(int maxlength);
//        Task<byte[]> ReadAsync(int maxlength, CancellationToken token);

//        void Write(byte[] data, int offset, int length);
//        Task WriteAsync(byte[] data, int offset, int length, CancellationToken token);

//        void Flush();
//        Task FlushAsync(CancellationToken token);

//        void Close();
//    }

//    /// <summary>
//    /// Implementation of <see cref="IRemoteStream"/> wrapping a <see cref="Stream"/> instance
//    /// </summary>
//    public class LocalStreamProxy : IRemoteStream
//    {
//        private readonly Stream m_parent;

//        public LocalStreamProxy(Stream parent)
//        {
//            m_parent = parent ?? throw new ArgumentNullException(nameof(parent));
//        }

//        public bool CanRead => m_parent.CanRead;
//        public bool CanSeek => m_parent.CanSeek;
//        public bool CanWrite => m_parent.CanWrite;
//        public bool CanTimeout => m_parent.CanTimeout;

//        public long Position { get => m_parent.Position; set => m_parent.Position = value; }
//        public int ReadTimeout { get => m_parent.ReadTimeout; set => m_parent.ReadTimeout = value; }
//        public int WriteTimeout { get => m_parent.WriteTimeout; set => m_parent.WriteTimeout = value; }
//        public long Length { get => m_parent.Length; set => m_parent.SetLength(value); }

//        public void Flush()
//        {
//            m_parent.Flush();
//        }

//        public Task FlushAsync(CancellationToken token)
//        {
//            return m_parent.FlushAsync(token);
//        }

//        public byte[] Read(int maxlength)
//        {
//            var buf = new byte[maxlength];
//            var c = m_parent.Read(buf, 0, buf.Length);
//            Array.Resize(ref buf, c);
//            return buf;
//        }

//        public async Task<byte[]> ReadAsync(int maxlength, CancellationToken token)
//        {
//            var buf = new byte[maxlength];
//            var c = await m_parent.ReadAsync(buf, 0, buf.Length, token);
//            Array.Resize(ref buf, c);
//            return buf;
//        }

//        public void Write(byte[] data, int offset, int length)
//        {
//            m_parent.Write(data, offset, length);
//        }

//        public Task WriteAsync(byte[] data, int offset, int length, CancellationToken token)
//        {
//            return WriteAsync(data, offset, length, token);
//        }

//        public void Close()
//        {
//            m_parent.Close();
//        }
//    }

//    /// <summary>
//    /// Implementation of the <see cref="Stream"/> class wrapping an <see cref="IRemoteStream"/> instance
//    /// </summary>
//    public class RemoteStreamProxy : Stream, IRemoteInstance
//    {
//        private readonly IRemoteStream m_parent;

//        public RemoteStreamProxy(IRemoteStream parent)
//        {
//            m_parent = parent ?? throw new ArgumentNullException(nameof(parent));
//        }

//        public override bool CanRead => m_parent.CanRead;
//        public override bool CanSeek => m_parent.CanSeek;
//        public override bool CanWrite => m_parent.CanWrite;
//        public override bool CanTimeout => m_parent.CanTimeout;
//        public override long Length => m_parent.Length;
//        public override long Position { get => m_parent.Position; set => m_parent.Position = value; }
//        public override int ReadTimeout { get => m_parent.ReadTimeout; set => m_parent.ReadTimeout = value; }
//        public override int WriteTimeout { get => m_parent.WriteTimeout; set => m_parent.WriteTimeout = value; }

//        long IRemoteInstance.Handle => ((IRemoteInstance)m_parent).Handle;

//        public override IAsyncResult BeginRead(byte[] buffer, int offset, int count, AsyncCallback callback, object state)
//        {
//            throw new NotSupportedException("Use the TPL async/await version instead");
//        }

//        public override IAsyncResult BeginWrite(byte[] buffer, int offset, int count, AsyncCallback callback, object state)
//        {
//            throw new NotSupportedException("Use the TPL async/await version instead");
//        }

//        public override int EndRead(IAsyncResult asyncResult)
//        {
//            throw new NotSupportedException("Use the TPL async/await version instead");
//        }

//        public override void EndWrite(IAsyncResult asyncResult)
//        {
//            throw new NotSupportedException("Use the TPL async/await version instead");
//        }

//        public override void Close()
//        {
//            m_parent.Close();
//        }

//        public override void Flush() { m_parent.Flush(); }

//        public override Task FlushAsync(CancellationToken cancellationToken) 
//        {
//            return m_parent.FlushAsync(cancellationToken);
//        }

//        public override int Read(byte[] buffer, int offset, int count)
//        {
//            var res = m_parent.Read(count);
//            Array.Copy(res, 0, buffer, offset, res.Length);
//            return res.Length;
//        }

//        public override long Seek(long offset, SeekOrigin origin)
//        {
//            if (origin == SeekOrigin.Current)
//                offset += m_parent.Position;
//            else if (origin == SeekOrigin.End)
//                offset = m_parent.Length - offset;

//            return m_parent.Position = offset;
//        }

//        public override void SetLength(long value)
//        {
//            m_parent.Length = value;
//        }

//        public override void Write(byte[] buffer, int offset, int count)
//        {
//            m_parent.Write(buffer, offset, count);
//        }

//        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
//        {
//            var res = await m_parent.ReadAsync(count, cancellationToken);
//            Array.Copy(res, 0, buffer, offset, res.Length);
//            return res.Length;
//        }

//        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
//        {
//            return m_parent.WriteAsync(buffer, offset, count, cancellationToken);
//        }

//        public override async Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
//        {
//            Task prev = null;
//            while (true)
//            {
//                var res = await m_parent.ReadAsync(bufferSize, cancellationToken);
//                if (prev != null)
//                    await prev;

//                if (res.Length == 0)
//                    return;

//                prev = m_parent.WriteAsync(res, 0, res.Length, cancellationToken);
//            }
//        }

//        protected override void Dispose(bool disposing)
//        {
//            if (m_parent is IRemoteInstance irm)
//                irm.Detach(false);

//            base.Dispose(disposing);
//        }

//        Task IRemoteInstance.Detach(bool suppressPeerCall)
//        {
//            return ((IRemoteInstance)m_parent).Detach(suppressPeerCall);
//        }
//    }
//}
