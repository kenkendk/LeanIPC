using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace LeanIPC
{
    /// <summary>
    /// Helper class to perform the tasks of the <see cref="BitConverter"/> class, 
    /// using big-endian (aka network) byte-order regardless of the machine endianess.
    /// A related class is <see cref="BinaryWriter"/> which also wraps a stream,
    /// but which is having an incomplete async implementation.
    /// Note that this stream exposes the internal buffer when reading,
    /// which can lead to unexpected results if used incorrectly.
    /// </summary>
    public class BinaryConverterStream : IDisposable
    {
        /// <summary>
        /// The cache size for the internal buffer array
        /// </summary>
        public const int CACHE_SIZE = 1024 * 32;

        /// <summary>
        /// The maximum number of bytes to read ahead in the stream
        /// </summary>
        public const int READAHEAD_BUFFER_SIZE = 1024 * 32;

        /// <summary>
        /// The internal write buffer to use
        /// </summary>
        private readonly byte[] m_writebuffer = new byte[CACHE_SIZE];

        /// <summary>
        /// The internal read-ahead buffer to use
        /// </summary>
        private readonly byte[] m_readaheadbuffer;

        /// <summary>
        /// The number of cached bytes
        /// </summary>
        private int m_writecount = 0;

        /// <summary>
        /// The number of read-ahead bytes
        /// </summary>
        private int m_readaheadcount = 0;

        /// <summary>
        /// The number of read-ahead bytes
        /// </summary>
        private int m_readaheadindex = 0;

        /// <summary>
        /// The stream to wrap
        /// </summary>
        private readonly Stream m_stream;

        /// <summary>
        /// The type serializer
        /// </summary>
        private readonly TypeSerializer m_serializer;

        /// <summary>
        /// The remote object handler
        /// </summary>
        private readonly RemoteObjectHandler m_objectHandler;

        /// <summary>
        /// Enable debug tracing
        /// </summary>
        private const bool TRACE = false;

        /// <summary>
        /// The system runtime type
        /// </summary>
        internal static readonly Type RUNTIMETYPE = Type.GetType("System.RuntimeType") ?? typeof(Type);

        /// <summary>
        /// Gets the type serializer used by this instance.
        /// </summary>
        public TypeSerializer TypeSerializer => m_serializer;

        /// <summary>
        /// Gets the remote object handler
        /// </summary>
        public RemoteObjectHandler RemoteHandler => m_objectHandler;

        /// <summary>
        /// Initializes a new instance of the <see cref="T:LeanIPC.BinaryConverterStream"/> class.
        /// </summary>
        /// <param name="stream">The stream to read or write.</param>
        /// <param name="serializer">The serializer to use</param>
        /// <param name="readBuffer">Set to <c>false</c> to disable the read-ahead buffer</param>
        public BinaryConverterStream(Stream stream, TypeSerializer serializer, bool readBuffer = true)
            : this(stream, serializer, null, readBuffer)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="T:LeanIPC.BinaryConverterStream"/> class.
        /// </summary>
        /// <param name="stream">The stream to read or write.</param>
        /// <param name="serializer">The serializer to use</param>
        /// <param name="remoteHandler">The handler for remote objects</param>
        /// <param name="readBuffer">Set to <c>false</c> to disable the read-ahead buffer</param>
        public BinaryConverterStream(Stream stream, TypeSerializer serializer, RemoteObjectHandler remoteHandler, bool readBuffer = true)
        {
            m_stream = stream ?? throw new ArgumentNullException(nameof(stream));
            m_serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            m_objectHandler = remoteHandler;
            m_readaheadbuffer = readBuffer ? new byte[READAHEAD_BUFFER_SIZE] : null;
        }

        /// <summary>
        /// Writes a boolean to the stream
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The bool value to write.</param>
        public async Task WriteBoolAsync(bool item)
        {
            if ((m_writebuffer.Length - m_writecount) < 1)
                await FlushAsync();
            
            m_writebuffer[m_writecount] = (byte)(item ? 1 : 0);
            m_writecount++;
        }

        /// <summary>
        /// Writes a char to the stream
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The char value to write.</param>
        public Task WriteCharAsync(char item)
        {
            return WriteStringAsync(item.ToString());
        }

        /// <summary>
        /// Writes an uint8 to the stream
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The uint8 value to write.</param>
        public async Task WriteUInt8Async(byte item)
        {
            if ((m_writebuffer.Length - m_writecount) < 1)
                await FlushAsync();
            
            m_writebuffer[m_writecount] = item;
            m_writecount++;
        }

        /// <summary>
        /// Writes an int8 to the stream
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The int8 value to write.</param>
        public async Task WriteInt8Async(sbyte item)
        {
            if ((m_writebuffer.Length - m_writecount) < 1)
                await FlushAsync();
            
            m_writebuffer[m_writecount] = (byte)item;
            m_writecount++;
        }

        /// <summary>
        /// Writes an int16 to the stream
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The int16 value to write.</param>
        public async Task WriteInt16Async(short item)
        {
            if ((m_writebuffer.Length - m_writecount) < 2)
                await FlushAsync();
            
            m_writebuffer[m_writecount + 0] = (byte)(item >> 8);
            m_writebuffer[m_writecount + 1] = (byte)(item);
            m_writecount += 2;
        }

        /// <summary>
        /// Writes an uint16 to the stream
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The uint16 value to write.</param>
        public async Task WriteUInt16Async(ushort item)
        {
            if ((m_writebuffer.Length - m_writecount) < 2)
                await FlushAsync();
            
            m_writebuffer[m_writecount + 0] = (byte)(item >> 8);
            m_writebuffer[m_writecount + 1] = (byte)(item >> 0);
            m_writecount += 2;
        }

        /// <summary>
        /// Writes an int32 to the stream
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The int32 value to write.</param>
        public async Task WriteInt32Async(int item)
        {
            if ((m_writebuffer.Length - m_writecount) < 4)
                await FlushAsync();
            
            m_writebuffer[m_writecount + 0] = (byte)(item >> 24);
            m_writebuffer[m_writecount + 1] = (byte)(item >> 16);
            m_writebuffer[m_writecount + 2] = (byte)(item >> 8);
            m_writebuffer[m_writecount + 3] = (byte)(item >> 0);
            m_writecount += 4;
        }

        /// <summary>
        /// Writes an uint32 to the stream
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The uint32 value to write.</param>
        public async Task WriteUInt32Async(uint item)
        {
            if ((m_writebuffer.Length - m_writecount) < 4)
                await FlushAsync();
            
            m_writebuffer[m_writecount + 0] = (byte)(item >> 24);
            m_writebuffer[m_writecount + 1] = (byte)(item >> 16);
            m_writebuffer[m_writecount + 2] = (byte)(item >> 8);
            m_writebuffer[m_writecount + 3] = (byte)(item >> 0);
            m_writecount += 4;
        }

        /// <summary>
        /// Writes an int64 to the stream
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The int64 value to write.</param>
        public async Task WriteInt64Async(long item)
        {
            if ((m_writebuffer.Length - m_writecount) < 8)
                await FlushAsync();
            
            m_writebuffer[m_writecount + 0] = (byte)(item >> 56);
            m_writebuffer[m_writecount + 1] = (byte)(item >> 48);
            m_writebuffer[m_writecount + 2] = (byte)(item >> 40);
            m_writebuffer[m_writecount + 3] = (byte)(item >> 32);
            m_writebuffer[m_writecount + 4] = (byte)(item >> 24);
            m_writebuffer[m_writecount + 5] = (byte)(item >> 16);
            m_writebuffer[m_writecount + 6] = (byte)(item >> 8);
            m_writebuffer[m_writecount + 7] = (byte)(item);
            m_writecount += 8;
        }

        /// <summary>
        /// Writes an uint64 to the stream
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The uint64 value to write.</param>
        public async Task WriteUInt64Async(ulong item)
        {
            if ((m_writebuffer.Length - m_writecount) < 8)
                await FlushAsync();
            
            m_writebuffer[m_writecount + 0] = (byte)(item >> 56);
            m_writebuffer[m_writecount + 1] = (byte)(item >> 48);
            m_writebuffer[m_writecount + 2] = (byte)(item >> 40);
            m_writebuffer[m_writecount + 3] = (byte)(item >> 32);
            m_writebuffer[m_writecount + 4] = (byte)(item >> 24);
            m_writebuffer[m_writecount + 5] = (byte)(item >> 16);
            m_writebuffer[m_writecount + 6] = (byte)(item >> 8);
            m_writebuffer[m_writecount + 7] = (byte)(item);
            m_writecount += 8;
        }

        /// <summary>
        /// Writes a byte array to the stream
        /// </summary>
        /// <returns>The byte array to write.</returns>
        /// <param name="item">The array to write.</param>
        private async Task WriteByteArray(byte[] item)
        {
            var remain = item.LongLength;
            var offset = 0L;
            while (remain > 0)
            {
                if ((m_writebuffer.Length - m_writecount) < remain)
                    await FlushAsync();
                var bytes = (int)Math.Min(remain, m_writebuffer.Length);
                Array.Copy(item, offset, m_writebuffer, m_writecount, bytes);
                m_writecount += bytes;
                offset += bytes;
                remain -= bytes;
            }
        }

        /// <summary>
        /// Writes a string to the stream
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The string value to write.</param>
        public async Task WriteStringAsync(string item)
        {
            if (item == null)
                await WriteInt32Async(-1);
            else
            {
                var count = System.Text.Encoding.UTF8.GetByteCount(item);
                await WriteInt32Async(count);

                // We assume that this mostly fits in the buffer
                if ((m_writebuffer.Length - m_writecount) >= count)
                {
                    m_writecount += System.Text.Encoding.UTF8.GetBytes(item, 0, item.Length, m_writebuffer, m_writecount);
                }
                else
                {
                    // If not, flush the current buffer
                    await FlushAsync();

                    // If we have space now, store it in the buffer
                    if ((m_writebuffer.Length - m_writecount) >= count)
                        m_writecount += System.Text.Encoding.UTF8.GetBytes(item, 0, item.Length, m_writebuffer, m_writecount);
                    else
                        await m_stream.WriteAsync(System.Text.Encoding.UTF8.GetBytes(item), 0, count);
                }

            }
        }

        /// <summary>
        /// Writes an object to the stream
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The object value to write.</param>
        /// <param name="treatAs">The type to treat the object as</param>
        public async Task WriteObjectAsync(object item, Type treatAs)
        {
            if (item == null)
                await WriteUInt8Async(0);
            else
            {
                long handle;
                if (m_objectHandler != null && item is IRemoteInstance)
                {
                    await WriteUInt8Async(1);
                    await WriteInt64Async(((IRemoteInstance)item).Handle);
                }
                else if (m_objectHandler != null && m_objectHandler.TryGetLocalHandle(item, out handle))
                {
                    await WriteUInt8Async(2);
                    await WriteInt64Async(handle);
                }
                else
                {
                    var parts = m_serializer.SerializeObject(item, treatAs);

                    await WriteUInt8Async(3);
                    await WriteStringAsync(m_serializer.GetShortTypeName(parts.Item1));
                    await WriteStringAsync(m_serializer.GetShortTypeDefinition(parts.Item2));
                    for (var i = 0; i < parts.Item2.Length; i++)
                        await WriteAnyAsync(parts.Item3[i], parts.Item2[i]);
                }
            }
        }

        /// <summary>
        /// Writes a float32 to the stream
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The float32 value to write.</param>
        public async Task WriteFloat32Async(float item)
        {
            var res = BitConverter.GetBytes(item);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(res);

            if ((m_writebuffer.Length - m_writecount) < 4)
                await FlushAsync();

            Array.Copy(res, 0, m_writebuffer, m_writecount, 4);
            m_writecount += 4;
        }

        /// <summary>
        /// Writes a float64 to the 
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The float64 value to write.</param>
        public async Task WriteFloat64Async(double item)
        {
            var res = BitConverter.GetBytes(item);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(res);
            
            if ((m_writebuffer.Length - m_writecount) < 8)
                await FlushAsync();

            Array.Copy(res, 0, m_writebuffer, m_writecount, 8);
            m_writecount += 8;
        }

        /// <summary>
        /// Writes a stream without writing a type header
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The type value to write.</param>
        public Task WriteTypeAsync(Type item)
        {
            return WriteStringAsync(m_serializer.GetShortTypeName(item));
        }

        /// <summary>
        /// Writes a stream without writing a type header
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The DateTime value to write.</param>
        public Task WriteDateTimeAsync(DateTime item)
        {
            return WriteInt64Async(item.ToUniversalTime().Ticks);
        }

        /// <summary>
        /// Writes a stream without writing a type header
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The DateTime value to write.</param>
        public Task WriteTimeSpanAsync(TimeSpan item)
        {
            return WriteInt64Async(item.Ticks);
        }

        /// <summary>
        /// Writes a stream without writing a type header
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The string to write.</param>
        public async Task WriteExceptionAsync(Exception item)
        {
            if (item == null)
            {
                await WriteStringAsync((string)null);
                await WriteStringAsync((string)null);
            }
            else
            {
                await WriteStringAsync(item.Message);
                await WriteStringAsync(item.GetType().FullName);
            }
        }

        /// <summary>
        /// Writes a stream without writing a type header
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The array to write.</param>
        public async Task WriteArrayAsync<T>(T[] item)
        {
            if (item == null)
            {
                await WriteInt64Async(-1);
            }
            else
            {
                System.Diagnostics.Trace.WriteLineIf(TRACE, $"Array-Write: {item.LongLength} items");

                await WriteInt64Async(item.LongLength);

                // Optimize writing byte arrays
                if (typeof(T) == typeof(byte) && item.LongLength > 0)
                {
                    await WriteByteArray((byte[])(object)item);
                }
                else
                {
                    foreach (var n in item)
                        await WriteAnyAsync<T>(n);
                }

                System.Diagnostics.Trace.WriteLineIf(TRACE, $"Array-Wrote: {item.LongLength} items");
            }
        }

        /// <summary>
        /// Writes a stream without writing a type header
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The dictionary to write.</param>
        public async Task WriteDictionaryAsync<TKey, TValue>(IDictionary<TKey, TValue> item)
        {
            if (item == null)
            {
                await WriteInt32Async(-1);
            }
            else
            {
                await WriteInt32Async(item.Count);
                foreach (var n in item)
                    await WriteKeyValuePairAsync(n);
            }
        }

        /// <summary>
        /// Writes a stream without writing a type header
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The sequence to write.</param>
        public async Task WriteEnumerableAsync<T>(IEnumerable<T> item)
        {
            if (item == null)
            {
                await WriteInt32Async(-1);
            }
            else
            {
                await WriteInt32Async(item.Count());
                foreach (var n in item)
                    await WriteAnyAsync<T>(n);
            }
        }

        /// <summary>
        /// Writes a stream without writing a type header
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The dictionary to write.</param>
        public async Task WriteListAsync<T>(IList<T> item)
        {
            if (item == null)
            {
                await WriteInt32Async(-1);
            }
            else
            {
                await WriteInt32Async(item.Count);
                foreach (var n in item)
                    await WriteAnyAsync<T>(n);
            }
        }

        /// <summary>
        /// Writes a stream without writing a type header
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The tuple to write.</param>
        public async Task WriteTuple1Async<T>(Tuple<T> item)
        {
            if (item == null)
            {
                await WriteUInt8Async(0);
            }
            else
            {
                await WriteUInt8Async(1);
                await WriteAnyAsync<T>(item.Item1);
            }
        }

        /// <summary>
        /// Writes a stream without writing a type header
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The tuple to write.</param>
        public async Task WriteTuple2Async<T1, T2>(Tuple<T1, T2> item)
        {
            if (item == null)
            {
                await WriteUInt8Async(0);
            }
            else
            {
                await WriteUInt8Async(2);
                await WriteAnyAsync<T1>(item.Item1);
                await WriteAnyAsync<T2>(item.Item2);
            }
        }

        /// <summary>
        /// Writes a stream without writing a type header
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The tuple to write.</param>
        public async Task WriteTuple3Async<T1, T2, T3>(Tuple<T1, T2, T3> item)
        {
            if (item == null)
            {
                await WriteUInt8Async(0);
            }
            else
            {
                await WriteUInt8Async(3);
                await WriteAnyAsync<T1>(item.Item1);
                await WriteAnyAsync<T2>(item.Item2);
                await WriteAnyAsync<T3>(item.Item3);
            }
        }

        /// <summary>
        /// Writes a stream without writing a type header
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The tuple to write.</param>
        public async Task WriteTuple4Async<T1, T2, T3, T4>(Tuple<T1, T2, T3, T4> item)
        {
            if (item == null)
            {
                await WriteUInt8Async(0);
            }
            else
            {
                await WriteUInt8Async(4);
                await WriteAnyAsync<T1>(item.Item1);
                await WriteAnyAsync<T2>(item.Item2);
                await WriteAnyAsync<T3>(item.Item3);
                await WriteAnyAsync<T4>(item.Item4);
            }
        }

        /// <summary>
        /// Writes a stream without writing a type header
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The tuple to write.</param>
        public async Task WriteTuple5Async<T1, T2, T3, T4, T5>(Tuple<T1, T2, T3, T4, T5> item)
        {
            if (item == null)
            {
                await WriteUInt8Async(0);
            }
            else
            {
                await WriteUInt8Async(6);
                await WriteAnyAsync<T1>(item.Item1);
                await WriteAnyAsync<T2>(item.Item2);
                await WriteAnyAsync<T3>(item.Item3);
                await WriteAnyAsync<T4>(item.Item4);
                await WriteAnyAsync<T5>(item.Item5);
            }
        }

        /// <summary>
        /// Writes a stream without writing a type header
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The tuple to write.</param>
        public async Task WriteTuple6Async<T1, T2, T3, T4, T5, T6>(Tuple<T1, T2, T3, T4, T5, T6> item)
        {
            if (item == null)
            {
                await WriteUInt8Async(0);
            }
            else
            {
                await WriteUInt8Async(6);
                await WriteAnyAsync<T1>(item.Item1);
                await WriteAnyAsync<T2>(item.Item2);
                await WriteAnyAsync<T3>(item.Item3);
                await WriteAnyAsync<T4>(item.Item4);
                await WriteAnyAsync<T5>(item.Item5);
                await WriteAnyAsync<T6>(item.Item6);
            }
        }

        /// <summary>
        /// Writes a stream without writing a type header
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The tuple to write.</param>
        public async Task WriteTuple7Async<T1, T2, T3, T4, T5, T6, T7>(Tuple<T1, T2, T3, T4, T5, T6, T7> item)
        {
            if (item == null)
            {
                await WriteUInt8Async(0);
            }
            else
            {
                await WriteUInt8Async(7);
                await WriteAnyAsync<T1>(item.Item1);
                await WriteAnyAsync<T2>(item.Item2);
                await WriteAnyAsync<T3>(item.Item3);
                await WriteAnyAsync<T4>(item.Item4);
                await WriteAnyAsync<T5>(item.Item5);
                await WriteAnyAsync<T6>(item.Item6);
                await WriteAnyAsync<T7>(item.Item7);
            }
        }

        /// <summary>
        /// Writes a stream without writing a type header
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The tuple to write.</param>
        public async Task WriteTuple8Async<T1, T2, T3, T4, T5, T6, T7, TRest>(Tuple<T1, T2, T3, T4, T5, T6, T7, TRest> item)
        {
            if (item == null)
            {
                await WriteUInt8Async(0);
            }
            else
            {
                await WriteUInt8Async(8);
                await WriteAnyAsync<T1>(item.Item1);
                await WriteAnyAsync<T2>(item.Item2);
                await WriteAnyAsync<T3>(item.Item3);
                await WriteAnyAsync<T4>(item.Item4);
                await WriteAnyAsync<T5>(item.Item5);
                await WriteAnyAsync<T6>(item.Item6);
                await WriteAnyAsync<T7>(item.Item7);
                await WriteAnyAsync<TRest>(item.Rest);
            }
        }
        /// <summary>
        /// Writes a stream without writing a type header
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The keyvaluepair to write.</param>
        public async Task WriteKeyValuePairAsync<TKey, TValue>(KeyValuePair<TKey, TValue> item)
        {
            await WriteAnyAsync<TKey>(item.Key);
            await WriteAnyAsync<TValue>(item.Value);
        }

        /// <summary>
        /// Writes a run-length encoded integer, re-packaging the
        /// value into 7-bit fields, where the highest bit indicates more data
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="value">The value to write.</param>
        public async Task WriteVarLengthInteger(long value)
        {
            var v = (ulong)value; 
            while (v >= 0x80)
            {
                await WriteUInt8Async((byte)(v | 0x80));
                v = v >> 7;
            }
            await WriteUInt8Async((byte)v);
        }

        /// <summary>
        /// Writes a stream without writing a type header
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The item to write.</param>
        /// <typeparam name="T">The type of the object to write</typeparam>
        public Task WriteAnyAsync<T>(object item)
        {
            return WriteAnyAsync(item, typeof(T));
        }

        /// <summary>
        /// Writes a stream without writing a type header
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The item to write.</param>
        /// <param name="type">The type of the element to write</param>
        public Task WriteAnyAsync(object item, Type type)
        {
            if (type == typeof(bool))
                return WriteBoolAsync((bool)item);
            if (type == typeof(char))
                return WriteCharAsync((char)item);
            if (type == typeof(sbyte))
                return WriteInt8Async((sbyte)item);
            if (type == typeof(byte))
                return WriteUInt8Async((byte)item);
            if (type == typeof(short))
                return WriteInt16Async((short)item);
            if (type == typeof(ushort))
                return WriteUInt16Async((ushort)item);
            if (type == typeof(int))
                return WriteInt32Async((int)item);
            if (type == typeof(uint))
                return WriteUInt32Async((uint)item);
            if (type == typeof(long))
                return WriteInt64Async((long)item);
            if (type == typeof(ulong))
                return WriteUInt64Async((ulong)item);
            if (type == typeof(float))
                return WriteFloat32Async((float)item);
            if (type == typeof(double))
                return WriteFloat64Async((double)item);
            if (type == typeof(string))
                return WriteStringAsync((string)item);
            if (type == RUNTIMETYPE || type == typeof(Type))
                return WriteTypeAsync((Type)item);
            if (type == typeof(TimeSpan))
                return WriteTimeSpanAsync((TimeSpan)item);
            if (type == typeof(DateTime))
                return WriteDateTimeAsync((DateTime)item);
            if (typeof(Exception).IsAssignableFrom(type))
                return WriteExceptionAsync((Exception)item);
            if (type == typeof(object))
                return WriteObjectAsync((object)item, type);
            if (type.IsEnum)
                return WriteAnyAsync(item, Enum.GetUnderlyingType(type));

            if (type.IsArray)
            {
                return
                    (Task)
                    typeof(BinaryConverterStream)
                    .GetMethod(nameof(WriteArrayAsync), System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public)
                    .MakeGenericMethod(new Type[] { type.GetElementType() })
                    .Invoke(this, new object[] { item });
            }

            if (type.IsGenericType)
            {
                var gt = type.GetGenericTypeDefinition();
                var ga = type.GetGenericArguments();
                if (gt == typeof(KeyValuePair<,>))
                {
                    return
                        (Task)
                        typeof(BinaryConverterStream)
                        .GetMethod(nameof(WriteKeyValuePairAsync), System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public)
                        .MakeGenericMethod(ga)
                        .Invoke(this, new object[] { item });
                }
                else if (
                    gt == typeof(Tuple<>) ||
                    gt == typeof(Tuple<,>) ||
                    gt == typeof(Tuple<,,>) ||
                    gt == typeof(Tuple<,,,>) ||
                    gt == typeof(Tuple<,,,,>) ||
                    gt == typeof(Tuple<,,,,,>) ||
                    gt == typeof(Tuple<,,,,,,>) ||
                    gt == typeof(Tuple<,,,,,,,>))
                {
                    var name = nameof(WriteTuple1Async).Replace("1", ga.Length.ToString());
                    return
                        (Task)
                        typeof(BinaryConverterStream)
                        .GetMethod(name, System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public)
                        .MakeGenericMethod(ga)
                        .Invoke(this, new object[] { item });

                }
                else if (gt == typeof(List<>) || gt == typeof(IList<>))
                {
                    return
                        (Task)
                        typeof(BinaryConverterStream)
                        .GetMethod(nameof(WriteListAsync), System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public)
                        .MakeGenericMethod(ga)
                        .Invoke(this, new object[] { item });
                }
                else if (gt == typeof(IEnumerable<>))
                {
                    return
                        (Task)
                        typeof(BinaryConverterStream)
                        .GetMethod(nameof(WriteEnumerableAsync), System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public)
                        .MakeGenericMethod(ga)
                        .Invoke(this, new object[] { item });
                }
                else if (gt == typeof(Dictionary<,>) || gt == typeof(IDictionary<,>))
                {
                    return
                        (Task)
                        typeof(BinaryConverterStream)
                        .GetMethod(nameof(WriteDictionaryAsync), System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public)
                        .MakeGenericMethod(ga)
                        .Invoke(this, new object[] { item });
                }
            }

            var action = m_serializer.GetAction(type);
            if (action != SerializationAction.Fail)
                return WriteObjectAsync((object)item, type);

            System.Diagnostics.Trace.WriteLine($"Unsupported type: {type.FullName}");
            throw new Exception($"Unsupported type: {type.FullName}");
        }

        /// <summary>
        /// Flushes the output buffer
        /// </summary>
        public async Task FlushAsync()
        {
            if (m_writecount > 0)
            {
                await m_stream.WriteAsync(m_writebuffer, 0, m_writecount);
                await m_stream.FlushAsync();
                m_writecount = 0;
            }
        }

        /// <summary>
        /// Copies data from the read-ahead buffer into the target array
        /// </summary>
        /// <param name="target">The array to copy into.</param>
        /// <param name="offset">The offset into the target array</param>
        /// <param name="maxlength">The maximum number of bytes to copy</param>
        private int ReadFromBuffer(byte[] target, int offset, int maxlength)
        {
            // If we can satisfy any of the request from the buffer, use that first
            var size = Math.Min(m_readaheadcount, maxlength);
            if (size > 0)
            {
                System.Diagnostics.Trace.WriteLineIf(TRACE, $"Serving {size} of {maxlength} from read-ahead buffer");
                Array.Copy(m_readaheadbuffer, m_readaheadindex, target, offset, size);

                m_readaheadcount -= size;
                if (m_readaheadcount == 0)
                    m_readaheadindex = 0;
                else
                    m_readaheadindex += size;
            }
            return size;
        }

        /// <summary>
        /// Repeatedly reads bytes from the stream until <paramref name="length"/> bytes have been read.
        /// Note that this method can expose the internal buffer array, and that
        /// the returned array can be larger than the requested size
        /// </summary>
        /// <returns>The read bytes.</returns>
        /// <param name="length">The number of bytes to read.</param>
        private async Task<byte[]> ForceReadAsync(int length)
        {
            // If we can serve the response straight from the buffer,
            // we just return the buffer
            if (m_readaheadbuffer != null && m_readaheadcount >= length && m_readaheadindex == 0)
            {
                System.Diagnostics.Trace.WriteLineIf(TRACE, $"Serving {length} bytes from front of read-ahead buffer");
                m_readaheadcount -= length;
                if (m_readaheadcount == 0)
                    m_readaheadindex = 0;
                else
                    m_readaheadindex += length;

                return m_readaheadbuffer;
            }

            // If the write buffer is not in use, store the results there
            var res = length <= m_writebuffer.Length && m_writecount == 0 ? m_writebuffer : new byte[length];
            var offset = 0;
            var remain = length;

            // Satisfy what we can from the internal buffer
            if (m_readaheadbuffer != null)
            {
                var p = ReadFromBuffer(res, offset, remain);
                offset += p;
                remain -= p;
            }

            while (remain > 0)
            {
                var maxread = m_readaheadbuffer == null ? remain : res.Length - offset;
                System.Diagnostics.Trace.WriteLineIf(TRACE, $"ForceRead {remain} of {length} (real: {maxread})");
                var r = await m_stream.ReadAsync(res, offset, maxread);
                offset += r;
                remain -= r;
                if (r == 0)
                {
                    System.Diagnostics.Trace.WriteLineIf(TRACE, $"ForceRead failed during {remain} of {length}");
                    throw new EndOfStreamException("Unexpected end-of-stream");
                }
            }

            // If we got more than we needed, store it in a buffer
            if (remain < 0 && m_readaheadbuffer != null)
            {
                var extra = Math.Abs(remain);
                System.Diagnostics.Trace.WriteLineIf(TRACE, $"Stashing {extra} bytes in read-ahead buffer");

                Array.Copy(res, offset - extra, m_readaheadbuffer, m_readaheadcount, extra);
                m_readaheadcount += extra;
            }

            return res;
        }

        /// <summary>
        /// Reads a boolean value from the stream without reading a header
        /// </summary>
        /// <returns>The boolean value.</returns>

        public async Task<bool> ReadBoolAsync()
        {
            return (await ForceReadAsync(1))[0] == 1;
        }

        /// <summary>
        /// Reads a boolean value from the stream without reading a header
        /// </summary>
        /// <returns>The boolean value.</returns>
        public async Task<char> ReadCharAsync()
        {
            return (await ReadStringAsync())[0];
        }

        /// <summary>
        /// Reads a int8 value from the stream without reading a header
        /// </summary>
        /// <returns>The int8 value.</returns>
        public async Task<sbyte> ReadInt8Async()
        {
            return (sbyte)(await ForceReadAsync(1))[0];
        }

        /// <summary>
        /// Reads a uint8 value from the stream without reading a header
        /// </summary>
        /// <returns>The uint8 value.</returns>

        public async Task<byte> ReadUInt8Async()
        {
            return (await ForceReadAsync(1))[0];
        }

        /// <summary>
        /// Reads a int16 value from the stream without reading a header
        /// </summary>
        /// <returns>The int16 value.</returns>

        public async Task<short> ReadInt16Async()
        {
            var res = await ForceReadAsync(2);
            return (short)
                (
                  (res[0] << 8) |
                  (res[1] << 0)
                );
        }

        /// <summary>
        /// Reads a uint16 value from the stream without reading a header
        /// </summary>
        /// <returns>The uint16 value.</returns>

        public async Task<ushort> ReadUInt16Async()
        {
            var res = await ForceReadAsync(2);
            return (ushort)
                (
                  (res[0] << 8) |
                  (res[0] << 0)
                );
        }

        /// <summary>
        /// Reads a int32 value from the stream without reading a header
        /// </summary>
        /// <returns>The int32 value.</returns>

        public async Task<int> ReadInt32Async()
        {
            var res = await ForceReadAsync(4);
            return (int)
                (
                  (res[0] << 24) |
                  (res[1] << 16) |
                  (res[2] << 8) |
                  (res[3] << 0)
                );
        }

        /// <summary>
        /// Reads a uint32 value from the stream without reading a header
        /// </summary>
        /// <returns>The uint32 value.</returns>

        public async Task<uint> ReadUInt32Async()
        {
            var res = await ForceReadAsync(4);
            return (uint)
                (
                  (res[0] << 24) |
                  (res[1] << 16) |
                  (res[2] << 8) |
                  (res[3] << 0)
                );
        }

        /// <summary>
        /// Reads a int64 value from the stream without reading a header
        /// </summary>
        /// <returns>The int64 value.</returns>

        public async Task<long> ReadInt64Async()
        {
            var res = await ForceReadAsync(8);
            var high = (uint)
                (
                  (res[0] << 24) |
                  (res[1] << 16) |
                  (res[2] << 8) |
                  (res[3] << 0)
                );
            
            var low = (uint)
                (
                  (res[4] << 24) |
                  (res[5] << 16) |
                  (res[6] << 8) |
                  (res[7] << 0)
                );

            return (long)(((ulong)high) << 32 | low);
        }

        /// <summary>
        /// Reads a uint64 value from the stream without reading a header
        /// </summary>
        /// <returns>The uint64 value.</returns>

        public async Task<ulong> ReadUInt64Async()
        {
            var res = await ForceReadAsync(8);
            var high = (uint)
                (
                  (res[0] << 24) |
                  (res[1] << 16) |
                  (res[2] << 8) |
                  (res[3] << 0)
                );

            var low = (uint)
                (
                  (res[4] << 24) |
                  (res[5] << 16) |
                  (res[6] << 8) |
                  (res[7] << 0)
                );

            return (((ulong)high) << 32 | low);
        }

        /// <summary>
        /// Reads a float32 value from the stream without reading a header
        /// </summary>
        /// <returns>The float32 value.</returns>

        public async Task<float> ReadFloat32Async()
        {
            var res = await ForceReadAsync(4);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(res, 0, 4);
            
            return BitConverter.ToSingle(res, 0);
        }

        /// <summary>
        /// Reads a float64 value from the stream without reading a header
        /// </summary>
        /// <returns>The float64 value.</returns>

        public async Task<double> ReadFloat64Async()
        {
            var res = await ForceReadAsync(8);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(res, 0, 8);

            return BitConverter.ToDouble(res, 0);
        }

        /// <summary>
        /// Reads a Type value from the stream without reading a header
        /// </summary>
        /// <returns>The Type value.</returns>

        public async Task<Type> ReadTypeAsync()
        {
            return m_serializer.ParseShortTypeName(await ReadStringAsync());
        }

        /// <summary>
        /// Reads a DateTime value from the stream without reading a header
        /// </summary>
        /// <returns>The DateTime value.</returns>

        public async Task<DateTime> ReadDateTimeAsync()
        {
            return new DateTime(await ReadInt64Async(), DateTimeKind.Utc);
        }

        /// <summary>
        /// Reads a TimeSpan value from the stream without reading a header
        /// </summary>
        /// <returns>The TimeSpan value.</returns>

        public async Task<TimeSpan> ReadTimeSpanAsync()
        {
            return new TimeSpan(await ReadInt64Async());
        }

        /// <summary>
        /// Reads a string value from the stream without reading a header
        /// </summary>
        /// <returns>The string value.</returns>

        public async Task<string> ReadStringAsync()
        {
            var len = await ReadInt32Async();
            if (len < 0)
                return null;
            System.Diagnostics.Trace.WriteLineIf(TRACE, $"Reading a string with {len} bytes");
            return System.Text.Encoding.UTF8.GetString(await ForceReadAsync(len), 0, len);
        }

        /// <summary>
        /// Reads an object value from the stream without reading a header
        /// </summary>
        /// <returns>The object value.</returns>
        public async Task<object> ReadObjectAsync()
        {
            var type = await ReadUInt8Async();
            if (type == 0)
                return null;

            if (type == 1)
                return m_objectHandler.GetLocalObject(await ReadInt64Async());
            if (type == 2)
                return m_objectHandler.GetRemoteObject(await ReadInt64Async());

            var typename = await ReadStringAsync();
            var fieldtypes = await ReadStringAsync();

            var fields = m_serializer.ParseShortTypeDefinition(fieldtypes);
            var elements = new object[fields.Length];
            for (var i = 0; i < elements.Length; i++)
                elements[i] = await ReadAnyAsync(fields[i]);

            return m_serializer.DeserializeObject(m_serializer.ParseShortTypeName(typename), elements);
        }

        /// <summary>
        /// Reads a string value from the stream without reading a header
        /// </summary>
        /// <returns>The string value.</returns>
        public async Task<Exception> ReadExceptionAsync()
        {
            var message = await ReadStringAsync();
            var typename = await ReadStringAsync();
            if (typename == null)
                return null;
            return new RemoteException(message, typename);
        }

        /// <summary>
        /// Reads an array value from the stream without reading a header
        /// </summary>
        /// <returns>The string value.</returns>
        public async Task<T[]> ReadArrayAsync<T>()
        {
            var length = await ReadInt64Async();
            if (length < 0)
                return null;

            var res = new T[length];
            if (typeof(T) == typeof(byte) && length < int.MaxValue)
            {
                var tmp = await ForceReadAsync((int)length);
                Array.Copy(tmp, res, length);
            }
            else
            {
                for (var i = 0; i < length; i++)
                    res[i] = await ReadAnyAsync<T>();
            }

            return res;
        }

        /// <summary>
        /// Reads an array value from the stream without reading a header
        /// </summary>
        /// <returns>The string value.</returns>

        public async Task<List<T>> ReadListAsync<T>()
        {
            var length = await ReadInt32Async();
            if (length < 0)
                return null;

            var res = new List<T>(length);
            for (var i = 0; i < length; i++)
                res.Add(await ReadAnyAsync<T>());

            return res;
        }

        /// <summary>
        /// Reads an array value from the stream without reading a header
        /// </summary>
        /// <returns>The string value.</returns>

        public async Task<Dictionary<TKey, TValue>> ReadDictionaryAsync<TKey, TValue>()
        {
            var length = await ReadInt32Async();
            if (length < 0)
                return null;

            var res = new Dictionary<TKey, TValue>(length);
            for (var i = 0; i < length; i++)
            {
                var key = await ReadAnyAsync<TKey>();
                var val = await ReadAnyAsync<TValue>();
                res[key] = val;
            }

            return res;
        }

        /// <summary>
        /// Reads an array value from the stream without reading a header
        /// </summary>
        /// <returns>The string value.</returns>

        public async Task<KeyValuePair<TKey, TValue>> ReadKeyValuePairAsync<TKey, TValue>()
        {
            var key = await ReadAnyAsync<TKey>();
            var val = await ReadAnyAsync<TValue>();
            return new KeyValuePair<TKey, TValue>(key, val);
        }

        /// <summary>
        /// Reads a tuple value from the stream without reading a header
        /// </summary>
        /// <returns>The tuple value.</returns>

        public async Task<Tuple<T1>> ReadTuple1Async<T1>()
        {
            var size = await ReadUInt8Async();
            if (size == 0)
                return null;

            var v1 = await ReadAnyAsync<T1>();

            return new Tuple<T1>(v1);
        }

        /// <summary>
        /// Reads a tuple value from the stream without reading a header
        /// </summary>
        /// <returns>The tuple value.</returns>

        public async Task<Tuple<T1, T2>> ReadTuple2Async<T1, T2>()
        {
            var size = await ReadUInt8Async();
            if (size == 0)
                return null;

            var v1 = await ReadAnyAsync<T1>();
            var v2 = await ReadAnyAsync<T2>();

            return new Tuple<T1, T2>(v1, v2);
        }

        /// <summary>
        /// Reads a tuple value from the stream without reading a header
        /// </summary>
        /// <returns>The tuple value.</returns>

        public async Task<Tuple<T1, T2, T3>> ReadTuple3Async<T1, T2, T3>()
        {
            var size = await ReadUInt8Async();
            if (size == 0)
                return null;

            var v1 = await ReadAnyAsync<T1>();
            var v2 = await ReadAnyAsync<T2>();
            var v3 = await ReadAnyAsync<T3>();

            return new Tuple<T1, T2, T3>(v1, v2, v3);
        }

        /// <summary>
        /// Reads a tuple value from the stream without reading a header
        /// </summary>
        /// <returns>The tuple value.</returns>

        public async Task<Tuple<T1, T2, T3, T4>> ReadTuple4Async<T1, T2, T3, T4>()
        {
            var size = await ReadUInt8Async();
            if (size == 0)
                return null;

            var v1 = await ReadAnyAsync<T1>();
            var v2 = await ReadAnyAsync<T2>();
            var v3 = await ReadAnyAsync<T3>();
            var v4 = await ReadAnyAsync<T4>();

            return new Tuple<T1, T2, T3, T4>(v1, v2, v3, v4);
        }

        /// <summary>
        /// Reads a tuple value from the stream without reading a header
        /// </summary>
        /// <returns>The tuple value.</returns>

        public async Task<Tuple<T1, T2, T3, T4, T5>> ReadTuple5Async<T1, T2, T3, T4, T5>()
        {
            var size = await ReadUInt8Async();
            if (size == 0)
                return null;

            var v1 = await ReadAnyAsync<T1>();
            var v2 = await ReadAnyAsync<T2>();
            var v3 = await ReadAnyAsync<T3>();
            var v4 = await ReadAnyAsync<T4>();
            var v5 = await ReadAnyAsync<T5>();

            return new Tuple<T1, T2, T3, T4, T5>(v1, v2, v3, v4, v5);
        }

        /// <summary>
        /// Reads a tuple value from the stream without reading a header
        /// </summary>
        /// <returns>The tuple value.</returns>

        public async Task<Tuple<T1, T2, T3, T4, T5, T6>> ReadTuple6Async<T1, T2, T3, T4, T5, T6>()
        {
            var size = await ReadUInt8Async();
            if (size == 0)
                return null;

            var v1 = await ReadAnyAsync<T1>();
            var v2 = await ReadAnyAsync<T2>();
            var v3 = await ReadAnyAsync<T3>();
            var v4 = await ReadAnyAsync<T4>();
            var v5 = await ReadAnyAsync<T5>();
            var v6 = await ReadAnyAsync<T6>();

            return new Tuple<T1, T2, T3, T4, T5, T6>(v1, v2, v3, v4, v5, v6);
        }

        /// <summary>
        /// Reads a tuple value from the stream without reading a header
        /// </summary>
        /// <returns>The tuple value.</returns>

        public async Task<Tuple<T1, T2, T3, T4, T5, T6, T7>> ReadTuple7Async<T1, T2, T3, T4, T5, T6, T7>()
        {
            var size = await ReadUInt8Async();
            if (size == 0)
                return null;

            var v1 = await ReadAnyAsync<T1>();
            var v2 = await ReadAnyAsync<T2>();
            var v3 = await ReadAnyAsync<T3>();
            var v4 = await ReadAnyAsync<T4>();
            var v5 = await ReadAnyAsync<T5>();
            var v6 = await ReadAnyAsync<T6>();
            var v7 = await ReadAnyAsync<T7>();

            return new Tuple<T1, T2, T3, T4, T5, T6, T7>(v1, v2, v3, v4, v5, v6, v7);
        }

        /// <summary>
        /// Reads a tuple value from the stream without reading a header
        /// </summary>
        /// <returns>The tuple value.</returns>

        public async Task<Tuple<T1, T2, T3, T4, T5, T6, T7, T8>> ReadTuple8Async<T1, T2, T3, T4, T5, T6, T7, T8>()
        {
            var size = await ReadUInt8Async();
            if (size == 0)
                return null;

            var v1 = await ReadAnyAsync<T1>();
            var v2 = await ReadAnyAsync<T2>();
            var v3 = await ReadAnyAsync<T3>();
            var v4 = await ReadAnyAsync<T4>();
            var v5 = await ReadAnyAsync<T5>();
            var v6 = await ReadAnyAsync<T6>();
            var v7 = await ReadAnyAsync<T7>();
            var v8 = await ReadAnyAsync<T8>();

            return new Tuple<T1, T2, T3, T4, T5, T6, T7, T8>(v1, v2, v3, v4, v5, v6, v7, v8);
        }

        /// <summary>
        /// Helper method to wrap the result of a task as an <seealso cref="object"/>.
        /// </summary>
        /// <returns>The casted task.</returns>
        /// <param name="item">The task to cast.</param>
        private Task<object> TypeCastTask(Task item)
        {
            if (item == null)
                throw new ArgumentNullException(nameof(item));
            if (!item.GetType().IsGenericType)
                throw new ArgumentException("The input is not a result type", nameof(item));

            var rag = item.GetType().GetGenericArguments();

            return
                (Task<object>)
                typeof(BinaryConverterStream)
                .GetMethod(nameof(TypeCastResultHelper), System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic)
                .MakeGenericMethod(rag)
                .Invoke(null, new object[] { item });
        }


        /// <summary>
        /// Helper method that returns the result as an object, regardless of the real type
        /// </summary>
        /// <returns>The result cast as the real type.</returns>
        /// <param name="item">The task to cast.</param>
        /// <typeparam name="T">The task type parameter.</typeparam>
        private static async Task<object> TypeCastResultHelper<T>(Task<T> item)
        {
            var n = await item;
            return n;
        }

        /// <summary>
        /// Reads a string value from the stream without reading a header
        /// </summary>
        /// <returns>The string value.</returns>
        public async Task<T> ReadAnyAsync<T>()
        {
            return (T)(await ReadAnyAsync(typeof(T)));
        }

        /// <summary>
        /// Reads a variable length integer, by reading chunks of 7 bits.
        /// The highest bit (8th bit) indicates continuation
        /// </summary>
        /// <returns>The variable length integer async.</returns>
        public async Task<long> ReadVarLengthIntegerAsync()
        {
            var res = 0uL;
            var shift_factor = 0;

            byte b;
            do
            {
                b = await ReadUInt8Async();

                res |= (ulong)(b & 0x7F) << shift_factor;
                shift_factor += 7;

                // Stop if we get weird data
                if (shift_factor >= ((64 + 63) / 7))
                    throw new InvalidDataException("Variable length integer is too large");

            } while ((b & 0x80) != 0);

            return (long)res;
        }

        /// <summary>
        /// Reads a string value from the stream without reading a header
        /// </summary>
        /// <returns>The string value.</returns>
        public async Task<object> ReadAnyAsync(Type type)
        {
            if (type == typeof(bool))
                return await ReadBoolAsync();
            if (type == typeof(char))
                return await ReadCharAsync();
            if (type == typeof(sbyte))
                return await ReadInt8Async();
            if (type == typeof(byte))
                return await ReadUInt8Async();
            if (type == typeof(short))
                return await ReadInt16Async();
            if (type == typeof(ushort))
                return await ReadUInt16Async();
            if (type == typeof(int))
                return await ReadInt32Async();
            if (type == typeof(uint))
                return await ReadUInt32Async();
            if (type == typeof(long))
                return await ReadInt64Async();
            if (type == typeof(ulong))
                return await ReadUInt64Async();
            if (type == typeof(float))
                return await ReadFloat32Async();
            if (type == typeof(double))
                return await ReadFloat64Async();
            if (type == typeof(string))
                return await ReadStringAsync();
            if (type == RUNTIMETYPE || type == typeof(Type))
                return await ReadTypeAsync();
            if (type == typeof(TimeSpan))
                return await ReadTimeSpanAsync();
            if (type == typeof(DateTime))
                return await ReadDateTimeAsync();
            if (typeof(Exception).IsAssignableFrom(type))
                return await ReadExceptionAsync();
            if (type == typeof(object))
                return await ReadObjectAsync();
            if (type.IsEnum)
                return await ReadAnyAsync(Enum.GetUnderlyingType(type.UnderlyingSystemType));

            if (type.IsArray)
            {
                return
                    await
                    TypeCastTask(
                        (Task)
                        typeof(BinaryConverterStream)
                        .GetMethod(nameof(ReadArrayAsync), System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public)
                        .MakeGenericMethod(new Type[] { type.GetElementType() })
                        .Invoke(this, null)
                    );
            }

            if (type.IsGenericType)
            {
                var gt = type.GetGenericTypeDefinition();
                var ga = type.GetGenericArguments();
                if (gt == typeof(KeyValuePair<,>))
                {
                    return
                        await
                        TypeCastTask(
                            (Task)
                            typeof(BinaryConverterStream)
                            .GetMethod(nameof(ReadKeyValuePairAsync), System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public)
                            .MakeGenericMethod(ga)
                            .Invoke(this, null)
                        );
                }
                else if (
                    gt == typeof(Tuple<>) ||
                    gt == typeof(Tuple<,>) ||
                    gt == typeof(Tuple<,,>) ||
                    gt == typeof(Tuple<,,,>) ||
                    gt == typeof(Tuple<,,,,>) ||
                    gt == typeof(Tuple<,,,,,>) ||
                    gt == typeof(Tuple<,,,,,,>) ||
                    gt == typeof(Tuple<,,,,,,,>))
                {
                    var name = nameof(ReadTuple1Async).Replace("1", ga.Length.ToString());
                    return
                        await
                        TypeCastTask(
                            (Task)
                            typeof(BinaryConverterStream)
                            .GetMethod(name, System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public)
                            .MakeGenericMethod(ga)
                            .Invoke(this, null)
                        );
                }
                else if (gt == typeof(List<>) || gt == typeof(IList<>) || gt == typeof(IEnumerable<>))
                {
                    return
                        await
                        TypeCastTask(
                            (Task)
                            typeof(BinaryConverterStream)
                            .GetMethod(nameof(ReadListAsync), System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public)
                            .MakeGenericMethod(ga)
                            .Invoke(this, null)
                        );
                }
                else if (gt == typeof(Dictionary<,>) || gt == typeof(IDictionary<,>))
                {
                    return
                        await
                        TypeCastTask(
                            (Task)
                            typeof(BinaryConverterStream)
                            .GetMethod(nameof(ReadDictionaryAsync), System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public)
                            .MakeGenericMethod(ga)
                            .Invoke(this, null)
                        );
                }
            }

            return await ReadObjectAsync();
        }


        /// <summary>
        /// Writes a record to the stream, optionally emitting a header first
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="values">The values to write.</param>
        /// <param name="types">The types of the values.</param>
        /// <param name="writeHeader">If set to <c>true</c> emit header first, then data.</param>
        public async Task WriteRecordAsync(object[] values, Type[] types, bool writeHeader)
        {
            if (values == null)
                return;
            if (types == null || types.Length != values.Length)
                throw new ArgumentException("Types must not be null, and must be of the same length as the value array");

            if (writeHeader)
            {
                System.Diagnostics.Trace.WriteLineIf(TRACE, $"Writing type header {m_serializer.GetShortTypeDefinition(types)}");
                await WriteStringAsync(m_serializer.GetShortTypeDefinition(types));
            }

            for (var i = 0; i < types.Length; i++)
            {
                System.Diagnostics.Trace.WriteLineIf(TRACE, $"Record-Write {i} type: {types[i]}, value: {values[i]}");
                await WriteAnyAsync(values[i], types[i]);
                System.Diagnostics.Trace.WriteLineIf(TRACE, $"Record-Wrote {i} type: {types[i]}, value: {values[i]}");
            }
        }

        /// <summary>
        /// Reads a record by first extracting the definition, and then reading the values
        /// </summary>
        /// <returns>The record items.</returns>
        public async Task<object[]> ReadRecordAsync()
        {
            var typedef = await ReadStringAsync();
            return await ReadRecordAsync(typedef);
        }

        /// <summary>
        /// Reads a record by parsing the definition, and then reading the values
        /// </summary>
        /// <returns>The record items.</returns>
        /// <param name="definition">The field definition</param>
        public Task<object[]> ReadRecordAsync(string definition)
        {
            return ReadRecordAsync(m_serializer.ParseShortTypeDefinition(definition));
        }

        /// <summary>
        /// Reads a record with the given types
        /// </summary>
        /// <returns>The record items.</returns>
        /// <param name="types">The types to read</param>
        public async Task<object[]> ReadRecordAsync(Type[] types)
        {
            var res = new object[types.Length];
            for (var i = 0; i < types.Length; i++)
            {
                System.Diagnostics.Trace.WriteLineIf(TRACE, $"Record-Reading {i} type: {types[i]}");
                res[i] = await ReadAnyAsync(types[i]);
                System.Diagnostics.Trace.WriteLineIf(TRACE, $"Record-Read {i} type: {types[i]}, value: {res[i]}");
            }

            return res;
        }

        /// <summary>
        /// Releases all resource used by the <see cref="T:LeanIPC.BinaryConverterStream"/> object.
        /// </summary>
        /// <remarks>Call <see cref="Dispose"/> when you are finished using the <see cref="T:LeanIPC.BinaryConverterStream"/>.
        /// The <see cref="Dispose"/> method leaves the <see cref="T:LeanIPC.BinaryConverterStream"/> in an unusable
        /// state. After calling <see cref="Dispose"/>, you must release all references to the
        /// <see cref="T:LeanIPC.BinaryConverterStream"/> so the garbage collector can reclaim the memory that the
        /// <see cref="T:LeanIPC.BinaryConverterStream"/> was occupying.</remarks>
        public void Dispose()
        {
            m_stream?.Dispose();
        }
    }
}
