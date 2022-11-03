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

#if SUPPORT_LOAD_BALANCING
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Net.Client.Balancer.Internal;
using Microsoft.Extensions.Logging;

namespace Grpc.Net.Client.Balancer
{
    /// <summary>
    /// Represents a logical connection. A subchannel is created with one or more addresses to equivalent servers.
    /// <para>
    /// A subchannel maintains at most one physical connection (aka transport) for sending new gRPC calls.
    /// If there isn't an active transport, and a call is assigned to the subchannel, it will create
    /// a new transport. A transport won't be created otherwise unless <see cref="RequestConnection"/>
    /// is called to create a transport if there isn't any.
    /// </para>
    /// <para>
    /// Note: Experimental API that can change or be removed without any prior notice.
    /// </para>
    /// </summary>
    public sealed class Subchannel : IDisposable
    {
        internal readonly List<BalancerAddress> _addresses;
        internal readonly object Lock;
        internal ISubchannelTransport Transport { get; set; } = default!;
        internal int Id { get; }
        internal Guid ChannelId { get; }

        /// <summary>
        /// Connectivity state is internal rather than public because it can be updated by multiple threads while
        /// a load balancer is building the picker.
        /// Load balancers that care about multiple subchannels should track state by subscribing to
        /// Subchannel.OnStateChanged and storing results.
        /// </summary>
        internal ConnectivityState State => _state;

        internal readonly ConnectionManager _manager;
        private readonly ILogger _logger;

        private ConnectContext? _connectContext;
        private ConnectivityState _state;
        private TaskCompletionSource<object?>? _delayInterruptTcs;
        private int _currentRegistrationId;

        /// <summary>
        /// Gets the current connected address.
        /// </summary>
        public BalancerAddress? CurrentAddress => Transport.CurrentAddress;

        /// <summary>
        /// Gets the metadata attributes.
        /// </summary>
        public BalancerAttributes Attributes { get; }

        internal Subchannel(ConnectionManager manager, IReadOnlyList<BalancerAddress> addresses)
        {
            Lock = new object();
            _logger = manager.LoggerFactory.CreateLogger(GetType());

            Id = manager.GetNextId();
            _addresses = addresses.ToList();
            _manager = manager;
            Attributes = new BalancerAttributes();
            this.ChannelId = _manager.ChannelId;

            SubchannelLog.SubchannelCreated(_logger, this.ChannelId, Id, addresses);
        }

        private readonly List<StateChangedRegistration> _stateChangedRegistrations = new List<StateChangedRegistration>();

        /// <summary>
        /// Registers a callback that will be invoked this subchannel's state changes.
        /// </summary>
        /// <param name="callback">The callback that will be invoked when the subchannel's state changes.</param>
        /// <returns>A subscription that can be disposed to unsubscribe from state changes.</returns>
        public IDisposable OnStateChanged(Action<SubchannelState> callback)
        {
            var registration = new StateChangedRegistration(this, callback);
            _stateChangedRegistrations.Add(registration);

            return registration;
        }

        private string GetNextRegistrationId()
        {
            var registrationId = Interlocked.Increment(ref _currentRegistrationId);
            return Id + "-" + registrationId;
        }

        private sealed class StateChangedRegistration : IDisposable
        {
            private readonly Guid channelId;
            private readonly Subchannel _subchannel;
            private readonly Action<SubchannelState> _callback;

            public string RegistrationId { get; }

            public StateChangedRegistration(Subchannel subchannel, Action<SubchannelState> callback)
            {
                this.channelId = subchannel.ChannelId;
                _subchannel = subchannel;
                _callback = callback;
                RegistrationId = subchannel.GetNextRegistrationId();

                SubchannelLog.StateChangedRegistrationCreated(_subchannel._logger, this.channelId, _subchannel.Id, RegistrationId);
            }

            public void Invoke(SubchannelState state)
            {
                SubchannelLog.ExecutingStateChangedRegistration(_subchannel._logger, this.channelId, _subchannel.Id, RegistrationId);
                _callback(state);
            }

            public void Dispose()
            {
                if (_subchannel._stateChangedRegistrations.Remove(this))
                {
                    SubchannelLog.StateChangedRegistrationRemoved(_subchannel._logger, this.channelId, _subchannel.Id, RegistrationId);
                }
            }
        }

        /// <summary>
        /// Replaces the existing addresses used with this <see cref="Subchannel"/>.
        /// <para>
        /// If the subchannel has an active connection and the new addresses contain the connected address
        /// then the connection is reused. Otherwise the subchannel will reconnect.
        /// </para>
        /// </summary>
        /// <param name="addresses"></param>
        public void UpdateAddresses(IReadOnlyList<BalancerAddress> addresses)
        {
            var requireReconnect = false;
            lock (Lock)
            {
                if (_addresses.SequenceEqual(addresses, BalancerAddressEqualityComparer.Instance))
                {
                    // Don't do anything if new addresses match existing addresses.
                    return;
                }

                _addresses.Clear();
                _addresses.AddRange(addresses);

                switch (_state)
                {
                    case ConnectivityState.Idle:
                        break;
                    case ConnectivityState.Connecting:
                    case ConnectivityState.TransientFailure:
                        SubchannelLog.AddressesUpdatedWhileConnecting(_logger, this.ChannelId, Id);
                        requireReconnect = true;
                        break;
                    case ConnectivityState.Ready:
                        // Transport uses the subchannel lock but take copy in an abundance of caution.
                        var currentAddress = CurrentAddress;
                        if (currentAddress != null && !_addresses.Contains(currentAddress))
                        {
                            requireReconnect = true;
                            SubchannelLog.ConnectedAddressNotInUpdatedAddresses(_logger, this.ChannelId, Id, currentAddress);
                        }
                        break;
                    case ConnectivityState.Shutdown:
                        throw new InvalidOperationException($"Subchannel id '{Id}' has been shutdown.");
                    default:
                        throw new ArgumentOutOfRangeException("state", _state, "Unexpected state.");
                }
            }

            if (requireReconnect)
            {
                CancelInProgressConnect();
                Transport.Disconnect();
                RequestConnection();
            }
        }

        /// <summary>
        /// Creates a connection (aka transport), if there isn't an active one.
        /// </summary>
        public void RequestConnection()
        {
            lock (Lock)
            {
                switch (_state)
                {
                    case ConnectivityState.Idle:
                        SubchannelLog.ConnectionRequested(_logger, this.ChannelId, Id);

                        // Only start connecting underlying transport if in an idle state.
                        UpdateConnectivityState(ConnectivityState.Connecting, "Connection requested.");
                        break;
                    case ConnectivityState.Connecting:
                    case ConnectivityState.Ready:
                    case ConnectivityState.TransientFailure:
                        SubchannelLog.ConnectionRequestedInNonIdleState(_logger, this.ChannelId, Id, _state);

                        // We're already attempting to connect to the transport.
                        // If the connection is waiting in a delayed backoff then interrupt
                        // the delay and immediately retry connection.
                        _delayInterruptTcs?.TrySetResult(null);
                        return;
                    case ConnectivityState.Shutdown:
                        throw new InvalidOperationException($"Subchannel id '{Id}' has been shutdown.");
                    default:
                        throw new ArgumentOutOfRangeException("state", _state, "Unexpected state.");
                }
            }

            _ = ConnectTransportAsync();
        }

        private void CancelInProgressConnect()
        {
            var connectContext = _connectContext;
            if (connectContext != null)
            {
                lock (Lock)
                {
                    // Cancel connect cancellation token.
                    connectContext.CancelConnect();
                    connectContext.Dispose();
                }
            }
        }

        private async Task ConnectTransportAsync()
        {
            // There shouldn't be a previous connect in progress, but cancel the CTS to ensure they're no longer running.
            CancelInProgressConnect();

            var connectContext = _connectContext = new ConnectContext(Transport.ConnectTimeout ?? Timeout.InfiniteTimeSpan);

            var backoffPolicy = _manager.BackoffPolicyFactory.Create();

            try
            {
                SubchannelLog.ConnectingTransport(_logger, this.ChannelId, Id);

                for (var attempt = 0; ; attempt++)
                {
                    lock (Lock)
                    {
                        if (_state == ConnectivityState.Shutdown)
                        {
                            return;
                        }
                    }

                    if (await Transport.TryConnectAsync(connectContext).ConfigureAwait(false))
                    {
                        return;
                    }

                    connectContext.CancellationToken.ThrowIfCancellationRequested();

                    _delayInterruptTcs = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
                    var delayCts = new CancellationTokenSource();

                    var backoffTicks = backoffPolicy.NextBackoff().Ticks;
                    // Task.Delay supports up to Int32.MaxValue milliseconds.
                    // Note that even if the maximum backoff is configured to this maximum, the jitter could push it over the limit.
                    // Force an upper bound here to ensure an unsupported backoff is never used.
                    backoffTicks = Math.Min(backoffTicks, TimeSpan.TicksPerMillisecond * int.MaxValue);
                    
                    var backkoff = TimeSpan.FromTicks(backoffTicks);
                    SubchannelLog.StartingConnectBackoff(_logger, this.ChannelId, Id, backkoff);
                    var completedTask = await Task.WhenAny(Task.Delay(backkoff, delayCts.Token), _delayInterruptTcs.Task).ConfigureAwait(false);

                    if (completedTask != _delayInterruptTcs.Task)
                    {
                        // Task.Delay won. Check CTS to see if it won because of cancellation.
                        delayCts.Token.ThrowIfCancellationRequested();
                    }
                    else
                    {
                        SubchannelLog.ConnectBackoffInterrupted(_logger, this.ChannelId, Id);

                        // Delay interrupt was triggered. Reset back-off.
                        backoffPolicy = _manager.BackoffPolicyFactory.Create();

                        // Cancel the Task.Delay that's no longer needed.
                        // https://github.com/davidfowl/AspNetCoreDiagnosticScenarios/blob/519ef7d231c01116f02bc04354816a735f2a36b6/AsyncGuidance.md#using-a-timeout
                        delayCts.Cancel();
                    }
                }
            }
            catch (OperationCanceledException)
            {
                SubchannelLog.ConnectCanceled(_logger, this.ChannelId, Id);
            }
            catch (Exception ex)
            {
                SubchannelLog.ConnectError(_logger, this.ChannelId, Id, ex);

                UpdateConnectivityState(ConnectivityState.TransientFailure, "Error connecting to subchannel.");
            }
            finally
            {
                lock (Lock)
                {
                    // Dispose context because it might have been created with a connect timeout.
                    // Want to clean up the connect timeout timer.
                    connectContext.Dispose();
                }
            }
        }

        internal bool UpdateConnectivityState(ConnectivityState state, string successDetail)
        {
            return UpdateConnectivityState(state, new Status(StatusCode.OK, successDetail));
        }
        
        internal bool UpdateConnectivityState(ConnectivityState state, Status status)
        {
            lock (Lock)
            {
                // Don't update subchannel state if the state is the same or the subchannel has been shutdown.
                //
                // This could happen when:
                // 1. Start trying to connect with a subchannel.
                // 2. Address resolver updates and subchannel address is no longer there and subchannel is shutdown.
                // 3. Connection attempt fails and tries to update subchannel state.
                if (_state == state || _state == ConnectivityState.Shutdown)
                {
                    return false;
                }
                _state = state;
            }
            
            // Notify channel outside of lock to avoid deadlocks.
            _manager.OnSubchannelStateChange(this, state, status);
            return true;
        }

        internal void RaiseStateChanged(ConnectivityState state, Status status)
        {
            SubchannelLog.SubchannelStateChanged(_logger, this.ChannelId, Id, state, status);

            if (_stateChangedRegistrations.Count > 0)
            {
                var subchannelState = new SubchannelState(state, status);
                foreach (var registration in _stateChangedRegistrations)
                {
                    registration.Invoke(subchannelState);
                }
            }
            else
            {
                SubchannelLog.NoStateChangedRegistrations(_logger, this.ChannelId, Id);
            }
        }

        /// <inheritdocs />
        public override string ToString()
        {
            lock (Lock)
            {
                return $"Id: {Id}, Addresses: {string.Join(", ", _addresses)}, State: {_state}, Current address: {CurrentAddress}";
            }
        }

        /// <summary>
        /// Returns the addresses that this subchannel is bound to.
        /// </summary>
        /// <returns>The addresses that this subchannel is bound to.</returns>
        public IReadOnlyList<BalancerAddress> GetAddresses()
        {
            lock (Lock)
            {
                return _addresses.ToArray();
            }
        }

        /// <summary>
        /// Disposes the <see cref="Subchannel"/>.
        /// The subchannel state is updated to <see cref="ConnectivityState.Shutdown"/>.
        /// After dispose the subchannel should no longer be returned by the latest <see cref="SubchannelPicker"/>.
        /// </summary>
        public void Dispose()
        {
            UpdateConnectivityState(ConnectivityState.Shutdown, "Subchannel disposed.");

            foreach (var registration in _stateChangedRegistrations)
            {
                SubchannelLog.StateChangedRegistrationRemoved(_logger, this.ChannelId, Id, registration.RegistrationId);
            }
            _stateChangedRegistrations.Clear();

            CancelInProgressConnect();
            Transport.Dispose();
        }
    }

    internal static class SubchannelLog
    {
        private static readonly Action<ILogger, Guid, int, string, Exception?> _subchannelCreated =
            LoggerMessage.Define< Guid, int, string>(LogLevel.Debug, new EventId(1, "SubchannelCreated"), "ChannelId: '{ChannelId}'. Subchannel id '{SubchannelId}' created with addresses: {Addresses}");

        private static readonly Action<ILogger, Guid, int, Exception?> _addressesUpdatedWhileConnecting =
            LoggerMessage.Define<Guid, int>(LogLevel.Debug, new EventId(2, "AddressesUpdatedWhileConnecting"), "ChannelId: '{ChannelId}'. Subchannel id '{SubchannelId}' is connecting when its addresses are updated. Restarting connect.");

        private static readonly Action<ILogger, Guid, int, BalancerAddress, Exception?> _connectedAddressNotInUpdatedAddresses =
            LoggerMessage.Define<Guid, int, BalancerAddress>(LogLevel.Debug, new EventId(3, "ConnectedAddressNotInUpdatedAddresses"), "ChannelId: '{ChannelId}'. Subchannel id '{SubchannelId}' current address '{CurrentAddress}' is not in the updated addresses.");

        private static readonly Action<ILogger, Guid, int, Exception?> _connectionRequested =
            LoggerMessage.Define<Guid, int>(LogLevel.Trace, new EventId(4, "ConnectionRequested"), "ChannelId: '{ChannelId}'. Subchannel id '{SubchannelId}' connection requested.");

        private static readonly Action<ILogger, Guid, int, ConnectivityState, Exception?> _connectionRequestedInNonIdleState =
            LoggerMessage.Define<Guid, int, ConnectivityState>(LogLevel.Debug, new EventId(5, "ConnectionRequestedInNonIdleState"), "ChannelId: '{ChannelId}'. Subchannel id '{SubchannelId}' connection requested in non-idle state of {State}.");

        private static readonly Action<ILogger, Guid, int, Exception?> _connectingTransport =
            LoggerMessage.Define<Guid, int>(LogLevel.Trace, new EventId(6, "ConnectingTransport"), "ChannelId: '{ChannelId}'. Subchannel id '{SubchannelId}' connecting to transport.");

        private static readonly Action<ILogger, Guid, int, TimeSpan, Exception?> _startingConnectBackoff =
            LoggerMessage.Define<Guid, int, TimeSpan>(LogLevel.Trace, new EventId(7, "StartingConnectBackoff"), "ChannelId: '{ChannelId}'. Subchannel id '{SubchannelId}' starting connect backoff of {BackoffDuration}.");

        private static readonly Action<ILogger, Guid, int, Exception?> _connectBackoffInterrupted =
            LoggerMessage.Define<Guid, int>(LogLevel.Trace, new EventId(8, "ConnectBackoffInterrupted"), "ChannelId: '{ChannelId}'. Subchannel id '{SubchannelId}' connect backoff interrupted.");

        private static readonly Action<ILogger, Guid, int, Exception?> _connectCanceled =
            LoggerMessage.Define<Guid, int>(LogLevel.Trace, new EventId(9, "ConnectCanceled"), "ChannelId: '{ChannelId}'. Subchannel id '{SubchannelId}' connect canceled.");

        private static readonly Action<ILogger, Guid, int, Exception?> _connectError =
            LoggerMessage.Define<Guid, int>(LogLevel.Debug, new EventId(10, "ConnectError"), "ChannelId: '{ChannelId}'. Subchannel id '{SubchannelId}' error while connecting to transport.");

        private static readonly Action<ILogger, Guid, int, ConnectivityState, string, Exception?> _subchannelStateChanged =
            LoggerMessage.Define<Guid, int, ConnectivityState, string>(LogLevel.Debug, new EventId(11, "SubchannelStateChanged"), "ChannelId: '{ChannelId}'. Subchannel id '{SubchannelId}' state changed to {State}. Detail: '{Detail}'.");

        private static readonly Action<ILogger, Guid, int, string, Exception?> _stateChangedRegistrationCreated =
            LoggerMessage.Define<Guid, int, string>(LogLevel.Trace, new EventId(12, "StateChangedRegistrationCreated"), "ChannelId: '{ChannelId}'. Subchannel id '{SubchannelId}' state changed registration '{RegistrationId}' created.");

        private static readonly Action<ILogger, Guid, int, string, Exception?> _stateChangedRegistrationRemoved =
            LoggerMessage.Define<Guid, int, string>(LogLevel.Trace, new EventId(13, "StateChangedRegistrationRemoved"), "ChannelId: '{ChannelId}'. Subchannel id '{SubchannelId}' state changed registration '{RegistrationId}' removed.");

        private static readonly Action<ILogger, Guid, int, string, Exception?> _executingStateChangedRegistration =
            LoggerMessage.Define<Guid, int, string>(LogLevel.Trace, new EventId(14, "ExecutingStateChangedRegistration"), "ChannelId: '{ChannelId}'. Subchannel id '{SubchannelId}' executing state changed registration '{RegistrationId}'.");

        private static readonly Action<ILogger, Guid, int, Exception?> _noStateChangedRegistrations =
            LoggerMessage.Define<Guid, int>(LogLevel.Trace, new EventId(15, "NoStateChangedRegistrations"), "ChannelId: '{ChannelId}'. Subchannel id '{SubchannelId}' has no state changed registrations.");

        private static readonly Action<ILogger, Guid, int, BalancerAddress, Exception?> _subchannelPreserved =
            LoggerMessage.Define<Guid, int, BalancerAddress>(LogLevel.Trace, new EventId(16, "SubchannelPreserved"), "ChannelId: '{ChannelId}'. Subchannel id '{SubchannelId}' matches address '{Address}' and is preserved.");

        public static void SubchannelCreated(ILogger logger, Guid channelId, int subchannelId, IReadOnlyList<BalancerAddress> addresses)
        {
            if (logger.IsEnabled(LogLevel.Debug))
            {
                var addressesText = string.Join(", ", addresses.Select(a => a.EndPoint.Host + ":" + a.EndPoint.Port));
                _subchannelCreated(logger, channelId, subchannelId, addressesText, null);
            }
        }

        public static void AddressesUpdatedWhileConnecting(ILogger logger, Guid channelId, int subchannelId)
        {
            _addressesUpdatedWhileConnecting(logger, channelId, subchannelId, null);
        }

        public static void ConnectedAddressNotInUpdatedAddresses(ILogger logger, Guid channelId, int subchannelId, BalancerAddress currentAddress)
        {
            _connectedAddressNotInUpdatedAddresses(logger, channelId, subchannelId, currentAddress, null);
        }

        public static void ConnectionRequested(ILogger logger, Guid channelId, int subchannelId)
        {
            _connectionRequested(logger, channelId, subchannelId, null);
        }

        public static void ConnectionRequestedInNonIdleState(ILogger logger, Guid channelId, int subchannelId, ConnectivityState state)
        {
            _connectionRequestedInNonIdleState(logger, channelId, subchannelId, state, null);
        }

        public static void ConnectingTransport(ILogger logger, Guid channelId, int subchannelId)
        {
            _connectingTransport(logger, channelId, subchannelId, null);
        }

        public static void StartingConnectBackoff(ILogger logger, Guid channelId, int subchannelId, TimeSpan delay)
        {
            _startingConnectBackoff(logger, channelId, subchannelId, delay, null);
        }

        public static void ConnectBackoffInterrupted(ILogger logger, Guid channelId, int subchannelId)
        {
            _connectBackoffInterrupted(logger, channelId, subchannelId, null);
        }

        public static void ConnectCanceled(ILogger logger, Guid channelId, int subchannelId)
        {
            _connectCanceled(logger, channelId, subchannelId, null);
        }

        public static void ConnectError(ILogger logger, Guid channelId, int subchannelId, Exception ex)
        {
            _connectError(logger, channelId, subchannelId, ex);
        }

        public static void SubchannelStateChanged(ILogger logger, Guid channelId, int subchannelId, ConnectivityState state, Status status)
        {
            _subchannelStateChanged(logger, channelId, subchannelId, state, status.Detail, status.DebugException);
        }

        public static void ExecutingStateChangedRegistration(ILogger logger, Guid channelId, int subchannelId, string registrationId)
        {
            _executingStateChangedRegistration(logger, channelId, subchannelId, registrationId, null);
        }

        public static void NoStateChangedRegistrations(ILogger logger, Guid channelId, int subchannelId)
        {
            _noStateChangedRegistrations(logger, channelId, subchannelId, null);
        }

        public static void StateChangedRegistrationCreated(ILogger logger, Guid channelId, int subchannelId, string registrationId)
        {
            _stateChangedRegistrationCreated(logger, channelId, subchannelId, registrationId, null);
        }

        public static void StateChangedRegistrationRemoved(ILogger logger, Guid channelId, int subchannelId, string registrationId)
        {
            _stateChangedRegistrationRemoved(logger, channelId, subchannelId, registrationId, null);
        }

        public static void SubchannelPreserved(ILogger logger, Guid channelId, int subchannelId, BalancerAddress address)
        {
            _subchannelPreserved(logger, channelId, subchannelId, address, null);
        }
    }
}
#endif
