using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;

namespace Daishi.Pluralsight.EventHub
{
    /// <summary>
    ///     <see cref="EventReceiver" /> interfaces with Event Hub, subscribing to, and
    ///     downloading events as they are output.
    /// </summary>
    public sealed class EventReceiver : IEventProcessor
    {
        /// <summary>
        ///     <see cref="EventHandler" /> is a function pointer that facilitates both
        ///     <see cref="EventReceiver.EventReceived" /> and
        ///     <see cref="Notification" /> events.
        /// </summary>
        /// <param name="sender">The instance that invoked the event.</param>
        /// <param name="e">The <see cref="EventReceiverEventArgs" /> instance.</param>
        public delegate void EventHandler(
            object sender,
            EventReceiverEventArgs e);

        private readonly TimeSpan _checkPointInterval;

        private Stopwatch _checkpointStopWatch;

        public EventReceiver(TimeSpan checkPointInterval)
        {
            _checkPointInterval = checkPointInterval;
        }

        /// <summary>
        ///     <see cref="IEventProcessor.CloseAsync" /> un-subscribes this instance from
        ///     an Event Hub partition.
        /// </summary>
        /// <param name="context">
        ///     <see cref="context" /> is the
        ///     <see cref="PartitionContext" /> from which this instance will be
        ///     un-subscribed.
        /// </param>
        /// <param name="reason">
        ///     <see cref="reason" /> outlines the reason for
        ///     un-subscription.
        /// </param>
        /// <returns><see cref="Task{TResult}" /> (an empty async response).</returns>
        public async Task CloseAsync(
            PartitionContext context,
            CloseReason reason)
        {
            if (reason == CloseReason.Shutdown)
            {
                await context.CheckpointAsync();
            }
            OnNotification(new EventReceiverEventArgs
            {
                Message = "Event Receiver Shut-down. " +
                          $"Partition: '{context.Lease.PartitionId}', " +
                          $"Reason: '{reason}'."
            });
        }

        /// <summary>
        ///     <see cref="IEventProcessor.OpenAsync" /> is invoked upon connecting to an
        ///     Event Hub partition.
        /// </summary>
        /// <param name="context">
        ///     <see cref="context" /> is the
        ///     <see cref="PartitionContext" /> to which this instance will subscribe.
        /// </param>
        /// <returns><see cref="Task{TResult}" /> (an empty async response).</returns>
        public Task OpenAsync(PartitionContext context)
        {
            _checkpointStopWatch = new Stopwatch();
            _checkpointStopWatch.Start();

            OnNotification(new EventReceiverEventArgs
            {
                Message = "Event Receiver initialized. " +
                          $"Partition: '{context.Lease.PartitionId}', " +
                          $"Offset: '{context.Lease.Offset}'."
            });
            return Task.FromResult<object>(null);
        }

        /// <summary>
        ///     <see cref="IEventProcessor.ProcessEventsAsync" /> is invoked when an event,
        ///     or collection of events, is retrieved from the Event Hub partition(s) to
        ///     which this instance is subscribed.
        /// </summary>
        /// <param name="context">
        ///     <see cref="context" /> is the
        ///     <see cref="PartitionContext" /> to which this instance is subscribed.
        /// </param>
        /// <param name="messages">
        ///     <see cref="messages" /> is a collection of
        ///     <see cref="EventData" /> instances retrieved from the Event Hub
        ///     partition(s) to which this instance is subscribed.
        /// </param>
        /// <returns><see cref="Task{TResult}" /> (an empty async response).</returns>
        /// <remarks>
        ///     <see cref="IEventProcessor.ProcessEventsAsync" /> raises
        ///     <see cref="Notification" /> and <see cref="EventReceived" /> events.
        /// </remarks>
        public async Task ProcessEventsAsync(PartitionContext context,
            IEnumerable<EventData> messages)
        {
            foreach (var @event in messages
                .Select(eventData => Encoding.UTF8.GetString(eventData.GetBytes())))
            {
                OnEventReceived(new EventReceiverEventArgs
                {
                    Message = @event
                });
                OnNotification(new EventReceiverEventArgs
                {
                    Message = "Message received. " +
                              $"Partition: '{context.Lease.PartitionId}', " +
                              $"Data: '{@event}'."
                });
            }
            if (_checkpointStopWatch.Elapsed > _checkPointInterval)
            {
                await context.CheckpointAsync();
                _checkpointStopWatch.Restart();
            }
        }

        /// <summary>
        ///     <see cref="Notification" /> is raised when an event is downloaded from an
        ///     Event Hub partition. This is a notification mechanism only, and does not
        ///     contain information pertaining to the event itself.
        /// </summary>
        public event EventHandler Notification;

        /// <summary>
        ///     <see cref="EventReceived" /> is raised when an event is downloaded from
        ///     an Event Hub partition, and contains the event itself in serialised,
        ///     UTF-8-format.
        /// </summary>
        public event EventHandler EventReceived;

        /// <summary>
        ///     <see cref="OnNotification" /> invokes any subscribers to
        ///     <see cref="Notification" />.
        /// </summary>
        /// <param name="e">
        ///     <see cref="e" /> is an <see cref="EventReceiverEventArgs" />
        ///     instance containing a notification message.
        /// </param>
        private void OnNotification(EventReceiverEventArgs e)
        {
            Notification?.Invoke(this, e);
        }

        /// <summary>
        ///     <see cref="OnEventReceived" /> invokes any subscribers to
        ///     <see cref="EventReceived" />.
        /// </summary>
        /// <param name="e">
        ///     <see cref="e" /> is an <see cref="EventReceiverEventArgs" />
        ///     instance containing the event itself in serialised, UTF-8-format.
        /// </param>
        private void OnEventReceived(EventReceiverEventArgs e)
        {
            EventReceived?.Invoke(this, e);
        }
    }
}