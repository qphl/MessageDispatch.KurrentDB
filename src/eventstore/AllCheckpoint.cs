/* This file is taken from Event Store codebase
   https://github.com/EventStore/EventStore/blob/master/src/EventStore.Core/TransactionLog/Checkpoint/WriteThroughFileCheckpoint.cs
   As such we should not add a Corsham Science copyright file header */

namespace CorshamScience.MessageDispatch.EventStore
{
    using System;
    using System.IO;
    using System.Runtime.InteropServices;
    using global::EventStore.Client;
    using Microsoft.Win32.SafeHandles;

    /// <summary>
    /// Writes a checkpoint to a file pulled from event store.
    /// </summary>
    internal class AllCheckpoint
    {
        private readonly object _lastLock = new ();

        private readonly FileStream _stream;
        private readonly bool _cached;
        private readonly BinaryWriter _writer;
        private readonly BinaryReader _reader;
        private readonly MemoryStream _memStream;
        private readonly byte[] _buffer;

        private Position _last;
        private Position _lastFlushed;

        /// <summary>
        /// Initializes a new instance of the <see cref="AllCheckpoint"/> class.
        /// </summary>
        /// <param name="filename">The file to write a checkpoint to.</param>
        /// <param name="name">The name of the checkpoint to write.</param>
        /// <param name="cached">Indicates if the checkpoint has been cached.</param>
        /// <param name="initValue">The initial value to write.</param>
        public AllCheckpoint(string filename, string name, bool cached, Position initValue)
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
        public void Write(Position checkpoint)
        {
            lock (_lastLock)
            {
                _last = checkpoint;
            }
        }

        /// <summary>
        /// Flushes the checkpoint streams.
        /// </summary>
        public void Flush()
        {
            _memStream.Seek(0, SeekOrigin.Begin);
            _stream.Seek(0, SeekOrigin.Begin);

            lock (_lastLock)
            {
                _writer.Write(_last.CommitPosition);
                _writer.Write(_last.PreparePosition);
                _stream.Write(_buffer, 0, _buffer.Length);

                _lastFlushed = _last;
            }
        }

        /// <summary>
        /// Reads the current checkpoint.
        /// </summary>
        /// <returns>The current checkpoint.</returns>
        public Position Read()
        {
            lock (_lastLock)
            {
                return _cached ? _lastFlushed : ReadCurrent();
            }
        }

        private Position ReadCurrent()
        {
            _stream.Seek(0, SeekOrigin.Begin);

            var commitPosition = _reader.ReadUInt64();
            var preparePosition = _reader.ReadUInt64();

            return new Position(commitPosition, preparePosition);
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
