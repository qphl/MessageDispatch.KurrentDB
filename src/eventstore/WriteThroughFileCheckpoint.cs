// <copyright file="WriteThroughFileCheckpoint.cs" company="Corsham Science">
// Copyright (c) Corsham Science. All rights reserved.
// </copyright>
#pragma warning disable CA1001

namespace CorshamScience.MessageDispatch.EventStore
{
    using System.IO;

    /// <summary>
    /// Writes a checkpoint to a file pulled from event store.
    /// </summary>
    public class WriteThroughFileCheckpoint
    {
        private readonly object _streamLock = new ();
        private readonly FileStream _fileStream;
        private readonly BinaryReader _reader;
        private readonly BinaryWriter _writer;
        private long? _lastWritten;

        /// <summary>
        /// Initializes a new instance of the <see cref="WriteThroughFileCheckpoint"/> class.
        /// </summary>
        /// <param name="filePath">The file to write a checkpoint to.</param>
        /// <param name="initValue">The initial value to write.</param>
        public WriteThroughFileCheckpoint(string filePath, long initValue = 0L)
        {
            var alreadyExisted = File.Exists(filePath);

            _fileStream = new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);

            if (_fileStream.Length != sizeof(long))
            {
                _fileStream.SetLength(8);
            }

            _reader = new BinaryReader(_fileStream);
            _writer = new BinaryWriter(_fileStream);

            if (!alreadyExisted)
            {
                Write(initValue);
            }
        }

        /// <summary>
        /// Reads the current checkpoint.
        /// </summary>
        /// <returns>The current checkpoint.</returns>
        public long Read()
        {
            lock (_streamLock)
            {
                if (_lastWritten.HasValue)
                {
                    return _lastWritten.Value;
                }

                _fileStream.Seek(0, SeekOrigin.Begin);
                return _reader.ReadInt64();
            }
        }

        /// <summary>
        /// Writes the checkpoint value and optionally flushes the underlying stream.
        /// </summary>
        /// <param name="checkpoint">The checkpoint value to write.</param>
        public void Write(long checkpoint)
        {
            lock (_streamLock)
            {
                _fileStream.Seek(0, SeekOrigin.Begin);
                _writer.Write(checkpoint);
                _lastWritten = checkpoint;
                _fileStream.Flush(flushToDisk: true);
            }
        }
    }
}
