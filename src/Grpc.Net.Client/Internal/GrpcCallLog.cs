#region Copyright notice and license

// Copyright 2019 The gRPC Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#endregion

using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace Grpc.Net.Client.Internal
{
    internal static class GrpcCallLog
    {
        private static readonly Action<ILogger, Guid, Guid, int, MethodType, Uri, Exception?> _startingCall =
            LoggerMessage.Define<Guid, Guid, int, MethodType, Uri>(LogLevel.Debug, new EventId(1, "StartingCall"), "Starting gRPC call. CallId: '{CallId}', ChannelId: '{ChannelId}', Attempt: '{Attempt}', Method type: '{MethodType}', URI: '{Uri}'.");

        private static readonly Action<ILogger, Guid, Exception?> _responseHeadersReceived =
            LoggerMessage.Define<Guid>(LogLevel.Trace, new EventId(2, "ResponseHeadersReceived"), "Response headers received. CallId: '{CallId}'");

        private static readonly Action<ILogger, Guid, StatusCode, string, Exception?> _grpcStatusError =
            LoggerMessage.Define<Guid, StatusCode, string>(LogLevel.Information, new EventId(3, "GrpcStatusError"), "Call failed with gRPC error status. CallId: '{CallId}', Status code: '{StatusCode}', Message: '{StatusMessage}'.");

        private static readonly Action<ILogger, Guid, Exception?> _finishedCall =
            LoggerMessage.Define<Guid>(LogLevel.Debug, new EventId(4, "FinishedCall"), "Finished gRPC call. CallId: '{CallId}'");

        private static readonly Action<ILogger, Guid, TimeSpan, Exception?> _startingDeadlineTimeout =
            LoggerMessage.Define<Guid, TimeSpan>(LogLevel.Trace, new EventId(5, "StartingDeadlineTimeout"), "Starting deadline timeout. CallId: '{CallId}', Duration: {DeadlineTimeout}.");

        private static readonly Action<ILogger, Guid, Exception?> _errorStartingCall =
            LoggerMessage.Define<Guid>(LogLevel.Error, new EventId(6, "ErrorStartingCall"), "Error starting gRPC call. CallId: '{CallId}'");

        private static readonly Action<ILogger, Guid, Exception?> _deadlineExceeded =
            LoggerMessage.Define<Guid>(LogLevel.Warning, new EventId(7, "DeadlineExceeded"), "gRPC call deadline exceeded. CallId: '{CallId}'");

        private static readonly Action<ILogger, Guid, Exception?> _canceledCall =
            LoggerMessage.Define<Guid>(LogLevel.Debug, new EventId(8, "CanceledCall"), "gRPC call canceled. CallId: '{CallId}'");

        private static readonly Action<ILogger, Guid, Exception?> _messageNotReturned =
            LoggerMessage.Define<Guid>(LogLevel.Error, new EventId(9, "MessageNotReturned"), "Message not returned from unary or client streaming call. CallId: '{CallId}'");

        private static readonly Action<ILogger, Guid, Exception?> _errorValidatingResponseHeaders =
            LoggerMessage.Define<Guid>(LogLevel.Error, new EventId(10, "ErrorValidatingResponseHeaders"), "Error validating response headers. CallId: '{CallId}'");

        private static readonly Action<ILogger, Guid, Exception?> _errorFetchingGrpcStatus =
            LoggerMessage.Define<Guid>(LogLevel.Error, new EventId(11, "ErrorFetchingGrpcStatus"), "Error fetching gRPC status. CallId: '{CallId}'");

        private static readonly Action<ILogger, Exception?> _callCredentialsNotUsed =
            LoggerMessage.Define(LogLevel.Warning, new EventId(12, "CallCredentialsNotUsed"), "The configured CallCredentials were not used because the call does not use TLS.");

        private static readonly Action<ILogger, Guid, int, Exception?> _readingMessage =
            LoggerMessage.Define<Guid, int>(LogLevel.Debug, new EventId(13, "ReadingMessage"), "Reading message. CallId: '{CallId}', MessageId: '{MessageId}'.");

        private static readonly Action<ILogger, Guid, int, Exception?> _noMessageReturned =
            LoggerMessage.Define<Guid, int>(LogLevel.Trace, new EventId(14, "NoMessageReturned"), "No message returned. CallId: '{CallId}', MessageId: '{MessageId}'.");

        private static readonly Action<ILogger, Guid, int, int, Type, Exception?> _deserializingMessage =
            LoggerMessage.Define<Guid, int, int, Type>(LogLevel.Trace, new EventId(15, "DeserializingMessage"), "CallId: '{CallId}', MessageId: '{MessageId}', Deserializing {MessageLength} byte message to '{MessageType}'.");

        private static readonly Action<ILogger, Guid, int, int, Type, Exception?> _receivedMessage =
            LoggerMessage.Define<Guid, int, int, Type>(LogLevel.Trace, new EventId(16, "ReceivedMessage"), "Received message. CallId: '{CallId}', MessageId: '{MessageId}', Length: '{MessageLength}', Type: '{MessageType}'.");

        private static readonly Action<ILogger, Guid, int, Exception> _errorReadingMessage =
            LoggerMessage.Define<Guid, int>(LogLevel.Information, new EventId(17, "ErrorReadingMessage"), "Error reading message. CallId: '{CallId}', MessageId: '{MessageId}'.");

        private static readonly Action<ILogger, Guid, int, Exception?> _sendingMessage =
            LoggerMessage.Define<Guid, int>(LogLevel.Debug, new EventId(18, "SendingMessage"), "Sending message. CallId: '{CallId}', MessageId: '{MessageId}'.");

        private static readonly Action<ILogger, Guid, int, Exception?> _messageSent =
            LoggerMessage.Define<Guid, int>(LogLevel.Trace, new EventId(19, "MessageSent"), "Message sent. CallId: '{CallId}', MessageId: '{MessageId}'.");

        private static readonly Action<ILogger, Guid, int, Exception> _errorSendingMessage =
            LoggerMessage.Define<Guid, int>(LogLevel.Information, new EventId(20, "ErrorSendingMessage"), "Error sending message. CallId: '{CallId}', MessageId: '{MessageId}'.");

        private static readonly Action<ILogger, Guid, Type, int, Exception?> _serializedMessage =
            LoggerMessage.Define<Guid, Type, int>(LogLevel.Trace, new EventId(21, "SerializedMessage"), "CallId: '{CallId}'. Serialized '{MessageType}' to {MessageLength} byte message.");

        private static readonly Action<ILogger, Guid, string, Exception?> _compressingMessage =
            LoggerMessage.Define<Guid, string>(LogLevel.Trace, new EventId(22, "CompressingMessage"), "CallId: '{CallId}'. Compressing message with '{MessageEncoding}' encoding.");

        private static readonly Action<ILogger, string, Exception?> _decompressingMessage =
            LoggerMessage.Define<string>(LogLevel.Trace, new EventId(23, "DecompressingMessage"), "Decompressing message with '{MessageEncoding}' encoding.");

        private static readonly Action<ILogger, TimeSpan, Exception?> _deadlineTimeoutTooLong =
            LoggerMessage.Define<TimeSpan>(LogLevel.Debug, new EventId(24, "DeadlineTimeoutTooLong"), "Deadline timeout {Timeout} is above maximum allowed timeout of 99999999 seconds. Maximum timeout will be used.");

        private static readonly Action<ILogger, TimeSpan, Exception?> _deadlineTimerRescheduled =
            LoggerMessage.Define<TimeSpan>(LogLevel.Trace, new EventId(25, "DeadlineTimerRescheduled"), "Deadline timer triggered but {Remaining} remaining before deadline exceeded. Deadline timer rescheduled.");

        private static readonly Action<ILogger, Guid, Exception> _errorParsingTrailers =
            LoggerMessage.Define<Guid>(LogLevel.Error, new EventId(26, "ErrorParsingTrailers"), "Error parsing trailers. CallId: '{CallId}'");

        private static readonly Action<ILogger, Guid, Exception> _errorExceedingDeadline =
            LoggerMessage.Define<Guid>(LogLevel.Error, new EventId(27, "ErrorExceedingDeadline"), "Error exceeding deadline. CallId: '{CallId}'");

        public static void StartingCall(ILogger logger, Guid callId, Guid channelId, int attempt, MethodType methodType, Uri uri)
        {
            _startingCall(logger, callId, channelId, attempt, methodType, uri, null);
        }

        public static void ResponseHeadersReceived(ILogger logger, Guid callId)
        {
            _responseHeadersReceived(logger, callId, null);
        }

        public static void GrpcStatusError(ILogger logger, Guid callId, StatusCode status, string message)
        {
            _grpcStatusError(logger, callId, status, message, null);
        }

        public static void FinishedCall(ILogger logger, Guid callId)
        {
            _finishedCall(logger, callId, null);
        }

        public static void StartingDeadlineTimeout(ILogger logger, Guid callId, TimeSpan deadlineTimeout)
        {
            _startingDeadlineTimeout(logger, callId, deadlineTimeout, null);
        }

        public static void ErrorStartingCall(ILogger logger, Guid callId, Exception ex)
        {
            _errorStartingCall(logger, callId, ex);
        }

        public static void DeadlineExceeded(ILogger logger, Guid callId)
        {
            _deadlineExceeded(logger, callId, null);
        }

        public static void CanceledCall(ILogger logger, Guid callId)
        {
            _canceledCall(logger, callId, null);
        }

        public static void MessageNotReturned(ILogger logger, Guid callId)
        {
            _messageNotReturned(logger, callId, null);
        }

        public static void ErrorValidatingResponseHeaders(ILogger logger, Guid callId, Exception ex)
        {
            _errorValidatingResponseHeaders(logger, callId, ex);
        }

        public static void ErrorFetchingGrpcStatus(ILogger logger, Guid callId, Exception ex)
        {
            _errorFetchingGrpcStatus(logger, callId, ex);
        }

        public static void CallCredentialsNotUsed(ILogger logger)
        {
            _callCredentialsNotUsed(logger, null);
        }

        public static void ReadingMessage(ILogger logger, Guid callId, int msgId)
        {
            _readingMessage(logger, callId, msgId, null);
        }

        public static void NoMessageReturned(ILogger logger, Guid callId, int msgId)
        {
            _noMessageReturned(logger, callId, msgId, null);
        }

        public static void DeserializingMessage(ILogger logger, Guid callId, int msgId, int messageLength, Type messageType)
        {
            _deserializingMessage(logger, callId, msgId, messageLength, messageType, null);
        }

        public static void ReceivedMessage(ILogger logger, Guid callId, int msgId, int length, Type messageType)
        {
            _receivedMessage(logger, callId, msgId, length, messageType, null);
        }

        public static void ErrorReadingMessage(ILogger logger, Guid callId, int msgId, Exception ex)
        {
            _errorReadingMessage(logger, callId, msgId, ex);
        }

        public static void SendingMessage(ILogger logger, Guid callId, int msgId)
        {
            _sendingMessage(logger, callId, msgId, null);
        }

        public static void MessageSent(ILogger logger, Guid callId, int msgId)
        {
            _messageSent(logger, callId, msgId, null);
        }

        public static void ErrorSendingMessage(ILogger logger, Guid callId, int msgId, Exception ex)
        {
            _errorSendingMessage(logger, callId, msgId, ex);
        }

        public static void SerializedMessage(ILogger logger, Guid callId, Type messageType, int messageLength)
        {
            _serializedMessage(logger, callId, messageType, messageLength, null);
        }

        public static void CompressingMessage(ILogger logger, Guid callId, string messageEncoding)
        {
            _compressingMessage(logger, callId, messageEncoding, null);
        }

        public static void DecompressingMessage(ILogger logger, string messageEncoding)
        {
            _decompressingMessage(logger, messageEncoding, null);
        }

        public static void DeadlineTimeoutTooLong(ILogger logger, TimeSpan timeout)
        {
            _deadlineTimeoutTooLong(logger, timeout, null);
        }

        public static void DeadlineTimerRescheduled(ILogger logger, TimeSpan remaining)
        {
            _deadlineTimerRescheduled(logger, remaining, null);
        }

        public static void ErrorParsingTrailers(ILogger logger, Guid callId, Exception ex)
        {
            _errorParsingTrailers(logger, callId, ex);
        }

        public static void ErrorExceedingDeadline(ILogger logger, Guid callId, Exception ex)
        {
            _errorExceedingDeadline(logger, callId, ex);
        }
    }
}
