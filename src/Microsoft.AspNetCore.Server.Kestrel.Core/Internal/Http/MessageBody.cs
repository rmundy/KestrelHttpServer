// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core.Internal.Infrastructure;
using Microsoft.AspNetCore.Server.Kestrel.Internal.System.IO.Pipelines;

namespace Microsoft.AspNetCore.Server.Kestrel.Core.Internal.Http
{
    public abstract class MessageBody
    {
        protected delegate int OnRead<TState>(ReadableBuffer readableBuffer, TState state, out ReadCursor consumed, out ReadCursor examined);

        private static readonly MessageBody _zeroContentLengthClose = new ForZeroContentLength(keepAlive: false);
        private static readonly MessageBody _zeroContentLengthKeepAlive = new ForZeroContentLength(keepAlive: true);

        private readonly Frame _context;
        private bool _send100Continue = true;

        protected MessageBody(Frame context)
        {
            _context = context;
        }

        protected abstract bool Consumed { get; }

        public static MessageBody ZeroContentLengthClose => _zeroContentLengthClose;

        public bool RequestKeepAlive { get; protected set; }

        public bool RequestUpgrade { get; protected set; }

        public async Task<int> ReadAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken = default(CancellationToken))
        {
            while (!Consumed)
            {
                var awaitable = _context.Input.ReadAsync();

                if (!awaitable.IsCompleted)
                {
                    TryProduceContinue();
                }

                _context.TimeoutControl.SetTimeout(TimeSpan.FromSeconds(5).Ticks, TimeoutAction.AbortConnection);
                var result = await awaitable;
                _context.TimeoutControl.CancelTimeout();

                var readableBuffer = result.Buffer;
                var consumed = readableBuffer.Start;
                var examined = readableBuffer.End;

                try
                {
                    if (!readableBuffer.IsEmpty)
                    {
                        var read = Read(readableBuffer, Copy, buffer, out consumed, out examined);

                        if (read > 0 || Consumed)
                        {
                            return read;
                        }
                    }
                    else if (result.IsCompleted)
                    {
                        _context.RejectRequest(RequestRejectionReason.UnexpectedEndOfRequestContent);
                    }
                }
                finally
                {
                    _context.Input.Advance(consumed, examined);
                }
            }

            return 0;
        }

        public async Task CopyToAsync(Stream destination, CancellationToken cancellationToken = default(CancellationToken))
        {
            while (!Consumed)
            {
                var awaitable = _context.Input.ReadAsync();

                if (!awaitable.IsCompleted)
                {
                    TryProduceContinue();
                }

                var result = await awaitable;
                var readableBuffer = result.Buffer;
                var consumed = readableBuffer.Start;
                var examined = readableBuffer.End;

                try
                {
                    if (!readableBuffer.IsEmpty)
                    {
                        Read(readableBuffer, CopyTo, destination, out consumed, out examined);

                        if (Consumed)
                        {
                            return;
                        }
                    }
                    else if (result.IsCompleted)
                    {
                        _context.RejectRequest(RequestRejectionReason.UnexpectedEndOfRequestContent);
                    }
                }
                finally
                {
                    _context.Input.Advance(consumed, examined);
                }
            }
        }

        public async Task ConsumeAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            while (!Consumed)
            {
                var awaitable = _context.Input.ReadAsync();

                if (!awaitable.IsCompleted)
                {
                    TryProduceContinue();
                }

                var result = await awaitable;
                var readableBuffer = result.Buffer;
                var consumed = readableBuffer.Start;
                var examined = readableBuffer.End;

                try
                {
                    if (!readableBuffer.IsEmpty)
                    {
                        Read<object>(readableBuffer, Consume, null, out consumed, out examined);
                    }
                    else if (result.IsCompleted)
                    {
                        _context.RejectRequest(RequestRejectionReason.UnexpectedEndOfRequestContent);
                    }
                }
                finally
                {
                    _context.Input.Advance(consumed, examined);
                }
            }
        }

        protected int Copy(ReadableBuffer readableBuffer, ArraySegment<byte> buffer, out ReadCursor consumed, out ReadCursor examined)
        {
            var actual = Math.Min(readableBuffer.Length, buffer.Count);
            readableBuffer.Slice(0, actual).CopyTo(buffer);
            examined = consumed = readableBuffer.Move(readableBuffer.Start, actual);
            return actual;
        }

        protected int CopyTo(ReadableBuffer readableBuffer, Stream stream, out ReadCursor consumed, out ReadCursor examined)
        {
            foreach (var memory in readableBuffer)
            {
                var array = memory.GetArray();
                stream.Write(array.Array, array.Offset, array.Count);
            }

            examined = consumed = readableBuffer.End;
            return readableBuffer.Length;
        }

        protected int Consume(ReadableBuffer readableBuffer, object state, out ReadCursor consumed, out ReadCursor examined)
        {
            examined = consumed = readableBuffer.End;
            return readableBuffer.Length;
        }

        private void TryProduceContinue()
        {
            if (_send100Continue)
            {
                _context.FrameControl.ProduceContinue();
                _send100Continue = false;
            }
        }

        protected abstract int Read<TState>(ReadableBuffer readableBuffer, OnRead<TState> onRead, TState state, out ReadCursor consumed, out ReadCursor examined);

        public static MessageBody For(
            HttpVersion httpVersion,
            FrameRequestHeaders headers,
            Frame context)
        {
            // see also http://tools.ietf.org/html/rfc2616#section-4.4
            var keepAlive = httpVersion != HttpVersion.Http10;

            var connection = headers.HeaderConnection;
            var upgrade = false;
            if (connection.Count > 0)
            {
                var connectionOptions = FrameHeaders.ParseConnection(connection);

                upgrade = (connectionOptions & ConnectionOptions.Upgrade) == ConnectionOptions.Upgrade;
                keepAlive = (connectionOptions & ConnectionOptions.KeepAlive) == ConnectionOptions.KeepAlive;
            }

            var transferEncoding = headers.HeaderTransferEncoding;
            if (transferEncoding.Count > 0)
            {
                var transferCoding = FrameHeaders.GetFinalTransferCoding(headers.HeaderTransferEncoding);

                // https://tools.ietf.org/html/rfc7230#section-3.3.3
                // If a Transfer-Encoding header field
                // is present in a request and the chunked transfer coding is not
                // the final encoding, the message body length cannot be determined
                // reliably; the server MUST respond with the 400 (Bad Request)
                // status code and then close the connection.
                if (transferCoding != TransferCoding.Chunked)
                {
                    context.RejectRequest(RequestRejectionReason.FinalTransferCodingNotChunked, transferEncoding.ToString());
                }

                if (upgrade)
                {
                    context.RejectRequest(RequestRejectionReason.UpgradeRequestCannotHavePayload);
                }

                return new ForChunkedEncoding(keepAlive, headers, context);
            }

            if (headers.ContentLength.HasValue)
            {
                var contentLength = headers.ContentLength.Value;

                if (contentLength == 0)
                {
                    return keepAlive ? _zeroContentLengthKeepAlive : _zeroContentLengthClose;
                }
                else if (upgrade)
                {
                    context.RejectRequest(RequestRejectionReason.UpgradeRequestCannotHavePayload);
                }

                return new ForContentLength(keepAlive, contentLength, context);
            }

            // Avoid slowing down most common case
            if (!object.ReferenceEquals(context.Method, HttpMethods.Get))
            {
                // If we got here, request contains no Content-Length or Transfer-Encoding header.
                // Reject with 411 Length Required.
                if (HttpMethods.IsPost(context.Method) || HttpMethods.IsPut(context.Method))
                {
                    var requestRejectionReason = httpVersion == HttpVersion.Http11 ? RequestRejectionReason.LengthRequired : RequestRejectionReason.LengthRequiredHttp10;
                    context.RejectRequest(requestRejectionReason, context.Method);
                }
            }

            if (upgrade)
            {
                return new ForUpgrade(context);
            }

            return keepAlive ? _zeroContentLengthKeepAlive : _zeroContentLengthClose;
        }

        private class ForUpgrade : MessageBody
        {
            public ForUpgrade(Frame context)
                : base(context)
            {
                RequestUpgrade = true;
            }

            protected override bool Consumed => false;

            protected override int Read<TState>(ReadableBuffer readableBuffer, OnRead<TState> onRead, TState state, out ReadCursor consumed, out ReadCursor examined)
            {
                return onRead(readableBuffer, state, out consumed, out examined);
            }
        }

        private class ForZeroContentLength : MessageBody
        {
            public ForZeroContentLength(bool keepAlive)
                : base(null)
            {
                RequestKeepAlive = keepAlive;
            }

            protected override bool Consumed => true;

            protected override int Read<TState>(ReadableBuffer readableBuffer, OnRead<TState> onRead, TState state, out ReadCursor consumed, out ReadCursor examined)
            {
                throw new NotImplementedException();
            }
        }

        private class ForContentLength : MessageBody
        {
            private long _inputLength;

            public ForContentLength(bool keepAlive, long contentLength, Frame context)
                : base(context)
            {
                RequestKeepAlive = keepAlive;
                _inputLength = contentLength;
            }

            protected override bool Consumed => _inputLength == 0;

            protected override int Read<TState>(ReadableBuffer readableBuffer, OnRead<TState> onRead, TState state, out ReadCursor consumed, out ReadCursor examined)
            {
                if (_inputLength == 0)
                {
                    consumed = examined = readableBuffer.Start;
                    return 0;
                }

                var available = (int)Math.Min(_inputLength, readableBuffer.Length);
                var actual = onRead(readableBuffer.Slice(0, available), state, out consumed, out examined);

                _inputLength -= actual;

                return actual;
            }
        }

        /// <summary>
        ///   http://tools.ietf.org/html/rfc2616#section-3.6.1
        /// </summary>
        private class ForChunkedEncoding : MessageBody
        {
            // byte consts don't have a data type annotation so we pre-cast it
            private const byte ByteCR = (byte)'\r';

            private readonly IPipeReader _input;
            private readonly FrameRequestHeaders _requestHeaders;
            private int _inputLength;

            private Mode _mode = Mode.Prefix;

            public ForChunkedEncoding(bool keepAlive, FrameRequestHeaders headers, Frame context)
                : base(context)
            {
                RequestKeepAlive = keepAlive;
                _input = _context.Input;
                _requestHeaders = headers;
            }

            protected override bool Consumed => _mode == Mode.Complete;

            protected override int Read<TState>(ReadableBuffer readableBuffer, OnRead<TState> onRead, TState state, out ReadCursor consumed, out ReadCursor examined)
            {
                consumed = default(ReadCursor);
                examined = default(ReadCursor);

                while (_mode < Mode.Trailer)
                {
                    if (_mode == Mode.Prefix)
                    {
                        ParseChunkedPrefix(readableBuffer, out consumed, out examined);

                        if (_mode == Mode.Prefix)
                        {
                            return 0;
                        }

                        readableBuffer = readableBuffer.Slice(consumed);
                    }

                    if (_mode == Mode.Extension)
                    {
                        ParseExtension(readableBuffer, out consumed, out examined);

                        if (_mode == Mode.Extension)
                        {
                            return 0;
                        }

                        readableBuffer = readableBuffer.Slice(consumed);
                    }

                    if (_mode == Mode.Data)
                    {
                        return ReadChunkedData(readableBuffer, onRead, state, out consumed, out examined);
                    }

                    if (_mode == Mode.Suffix)
                    {
                        ParseChunkedSuffix(readableBuffer, out consumed, out examined);

                        if (_mode == Mode.Suffix)
                        {
                            return 0;
                        }

                        readableBuffer = readableBuffer.Slice(consumed);
                    }
                }

                // Chunks finished, parse trailers
                if (_mode == Mode.Trailer)
                {
                    ParseChunkedTrailer(readableBuffer, out consumed, out examined);

                    if (_mode == Mode.Trailer)
                    {
                        return 0;
                    }

                    readableBuffer = readableBuffer.Slice(consumed);
                }

                if (_mode == Mode.TrailerHeaders)
                {
                    if (_context.TakeMessageHeaders(readableBuffer, out consumed, out examined))
                    {
                        _mode = Mode.Complete;
                    }
                }

                return 0;
            }

            private void ParseChunkedPrefix(ReadableBuffer buffer, out ReadCursor consumed, out ReadCursor examined)
            {
                consumed = buffer.Start;
                examined = buffer.Start;
                var reader = new ReadableBufferReader(buffer);
                var ch1 = reader.Take();
                var ch2 = reader.Take();

                if (ch1 == -1 || ch2 == -1)
                {
                    examined = reader.Cursor;
                    return;
                }

                var chunkSize = CalculateChunkSize(ch1, 0);
                ch1 = ch2;

                do
                {
                    if (ch1 == ';')
                    {
                        consumed = reader.Cursor;
                        examined = reader.Cursor;

                        _inputLength = chunkSize;
                        _mode = Mode.Extension;
                        return;
                    }

                    ch2 = reader.Take();
                    if (ch2 == -1)
                    {
                        examined = reader.Cursor;
                        return;
                    }

                    if (ch1 == '\r' && ch2 == '\n')
                    {
                        consumed = reader.Cursor;
                        examined = reader.Cursor;

                        _inputLength = chunkSize;

                        if (chunkSize > 0)
                        {
                            _mode = Mode.Data;
                        }
                        else
                        {
                            _mode = Mode.Trailer;
                        }

                        return;
                    }

                    chunkSize = CalculateChunkSize(ch1, chunkSize);
                    ch1 = ch2;
                } while (ch1 != -1);
            }

            private void ParseExtension(ReadableBuffer buffer, out ReadCursor consumed, out ReadCursor examined)
            {
                // Chunk-extensions not currently parsed
                // Just drain the data
                consumed = buffer.Start;
                examined = buffer.Start;
                do
                {
                    ReadCursor extensionCursor;
                    if (ReadCursorOperations.Seek(buffer.Start, buffer.End, out extensionCursor, ByteCR) == -1)
                    {
                        // End marker not found yet
                        examined = buffer.End;
                        return;
                    };

                    var sufixBuffer = buffer.Slice(extensionCursor);
                    if (sufixBuffer.Length < 2)
                    {
                        examined = buffer.End;
                        return;
                    }

                    sufixBuffer = sufixBuffer.Slice(0, 2);
                    var sufixSpan = sufixBuffer.ToSpan();


                    if (sufixSpan[1] == '\n')
                    {
                        consumed = sufixBuffer.End;
                        examined = sufixBuffer.End;
                        if (_inputLength > 0)
                        {
                            _mode = Mode.Data;
                        }
                        else
                        {
                            _mode = Mode.Trailer;
                        }
                    }
                } while (_mode == Mode.Extension);
            }

            private int ReadChunkedData<TState>(ReadableBuffer readableBuffer, OnRead<TState> onRead, TState state, out ReadCursor consumed, out ReadCursor examined)
            {
                var available = Math.Min(_inputLength, readableBuffer.Length);
                var actual = onRead(readableBuffer.Slice(0, available), state, out consumed, out examined);

                _inputLength -= actual;

                if (_inputLength == 0)
                {
                    _mode = Mode.Suffix;
                }

                return actual;
            }

            private void ParseChunkedSuffix(ReadableBuffer buffer, out ReadCursor consumed, out ReadCursor examined)
            {
                consumed = buffer.Start;
                examined = buffer.Start;

                if (buffer.Length < 2)
                {
                    examined = buffer.End;
                    return;
                }

                var suffixBuffer = buffer.Slice(0, 2);
                var suffixSpan = suffixBuffer.ToSpan();
                if (suffixSpan[0] == '\r' && suffixSpan[1] == '\n')
                {
                    consumed = suffixBuffer.End;
                    examined = suffixBuffer.End;
                    _mode = Mode.Prefix;
                }
                else
                {
                    _context.RejectRequest(RequestRejectionReason.BadChunkSuffix);
                }
            }

            private void ParseChunkedTrailer(ReadableBuffer buffer, out ReadCursor consumed, out ReadCursor examined)
            {
                consumed = buffer.Start;
                examined = buffer.Start;

                if (buffer.Length < 2)
                {
                    examined = buffer.End;
                    return;
                }

                var trailerBuffer = buffer.Slice(0, 2);
                var trailerSpan = trailerBuffer.ToSpan();

                if (trailerSpan[0] == '\r' && trailerSpan[1] == '\n')
                {
                    consumed = trailerBuffer.End;
                    examined = trailerBuffer.End;
                    _mode = Mode.Complete;
                }
                else
                {
                    _mode = Mode.TrailerHeaders;
                }
            }

            private int CalculateChunkSize(int extraHexDigit, int currentParsedSize)
            {
                checked
                {
                    if (extraHexDigit >= '0' && extraHexDigit <= '9')
                    {
                        return currentParsedSize * 0x10 + (extraHexDigit - '0');
                    }
                    else if (extraHexDigit >= 'A' && extraHexDigit <= 'F')
                    {
                        return currentParsedSize * 0x10 + (extraHexDigit - ('A' - 10));
                    }
                    else if (extraHexDigit >= 'a' && extraHexDigit <= 'f')
                    {
                        return currentParsedSize * 0x10 + (extraHexDigit - ('a' - 10));
                    }
                }

                _context.RejectRequest(RequestRejectionReason.BadChunkSizeData);
                return -1; // can't happen, but compiler complains
            }

            private enum Mode
            {
                Prefix,
                Extension,
                Data,
                Suffix,
                Trailer,
                TrailerHeaders,
                Complete
            };
        }
    }
}
