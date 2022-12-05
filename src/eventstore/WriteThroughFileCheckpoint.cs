/* This file is taken from Event Store codebase
   https://github.com/EventStore/EventStore/blob/master/src/EventStore.Core/TransactionLog/Checkpoint/WriteThroughFileCheckpoint.cs
   As such we should not add a Corsham Science copyright file header */
#pragma warning disable CA1001, CA1060, CA2101

namespace CorshamScience.MessageDispatch.EventStore
{
    using System;
    using System.IO;
    using System.Runtime.InteropServices;
    using System.Threading;
    using Microsoft.Win32.SafeHandles;

    /// <summary>
    /// Writes a checkpoint to a file pulled from event store.
    /// </summary>
    internal class WriteThroughFileCheckpoint
    {
        private readonly FileStream _stream;
        private readonly bool _cached;
        private readonly BinaryWriter _writer;
        private readonly BinaryReader _reader;
        private readonly MemoryStream _memStream;
        private readonly byte[] _buffer;

        private long _last;
        private long _lastFlushed;

        /// <summary>
        /// Initializes a new instance of the <see cref="WriteThroughFileCheckpoint"/> class.
        /// </summary>
        /// <param name="filename">The file to write a checkpoint to.</param>
        /// <param name="name">The name of the checkpoint to write.</param>
        /// <param name="cached">Indicates if the checkpoint has been cached.</param>
        /// <param name="initValue">The initial value to write.</param>
        public WriteThroughFileCheckpoint(string filename, string name, bool cached, long initValue = 0)
        {
            Name = name;
            _cached = cached;
            _buffer = new byte[4096];
            _memStream = new MemoryStream(_buffer);

            var handle = Filenative.CreateFile(
                filename,
                (uint)FileAccess.ReadWrite,
                (uint)FileShare.ReadWrite,
                IntPtr.Zero,
                (uint)FileMode.OpenOrCreate,
                Filenative.FileFlagNoBuffering | (int)FileOptions.WriteThrough,
                IntPtr.Zero);

            _stream = new FileStream(handle, FileAccess.ReadWrite, 4096);
            var exists = _stream.Length == 4096;
            _stream.SetLength(4096);
            _reader = new BinaryReader(_stream);
            _writer = new BinaryWriter(_memStream);

            if (!exists)
            {
                Write(initValue);
                Flush();
            }

            _last = _lastFlushed = ReadCurrent();
        }

        /// <summary>
        /// Gets the name.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Writes the checkpoint.
        /// </summary>
        /// <param name="checkpoint">Represents the new checkpoint.</param>
        public void Write(long checkpoint)
        {
            Interlocked.Exchange(ref _last, checkpoint);
        }

        /// <summary>
        /// Flushes the checkpoint streams.
        /// </summary>
        public void Flush()
        {
            _memStream.Seek(0, SeekOrigin.Begin);
            _stream.Seek(0, SeekOrigin.Begin);
            var last = Interlocked.Read(ref _last);
            _writer.Write(last);
            _stream.Write(_buffer, 0, _buffer.Length);

            Interlocked.Exchange(ref _lastFlushed, last);
        }

        /// <summary>
        /// Reads the current checkpoint.
        /// </summary>
        /// <returns>The current checkpoint.</returns>
        public long Read()
        {
            return _cached ? Interlocked.Read(ref _lastFlushed) : ReadCurrent();
        }

        private long ReadCurrent()
        {
            _stream.Seek(0, SeekOrigin.Begin);
            return _reader.ReadInt64();
        }

        private static class Filenative
        {
            public const int FileFlagNoBuffering = 0x20000000;

            [DllImport("kernel32", SetLastError = true)]
            internal static extern SafeFileHandle CreateFile(
                string fileName,
                uint desiredAccess,
                uint shareMode,
                IntPtr securityAttributes,
                uint creationDisposition,
                int flagsAndAttributes,
                IntPtr hTemplate);
        }
    }
}
#pragma warning restore CA1001, CA1060, CA2101
