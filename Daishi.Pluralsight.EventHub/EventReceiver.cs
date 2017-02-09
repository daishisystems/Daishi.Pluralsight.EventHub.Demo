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
        ///     <see cref="EventReceivedEventHandler" /> is a function pointer that
        ///     facilitates
        ///     <see cref="EventReceived" />.
        /// </summary>
        /// <param name="sender">The <see cref="object" /> that invoked the event.</param>
        /// <param name="e">The <see cref="EventReceivedEventArgs" /> instance.</param>
        public delegate void EventReceivedEventHandler(
            object sender,
            EventReceivedEventArgs e);

        /// <summary>
        ///     <see cref="NotificactionReceivedEventHandler" /> is a function pointer that
        ///     facilitates
        ///     <see cref="NotificationReceived" />.
        /// </summary>
        /// <param name="sender">The <see cref="object" /> that invoked the event.</param>
        /// <param name="e">The <see cref="EventReceivedEventArgs" /> instance.</param>
        public delegate void NotificactionReceivedEventHandler(
            object sender,
            NotificationReceivedEventArgs e);

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
            OnNotification(new NotificationReceivedEventArgs
            {
                Notificaction = $"Event Hub connection closed: {reason}",
                NotificationSource = NotificationSource.Close,
                PartitionId = context.Lease.PartitionId
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

            OnNotification(new NotificationReceivedEventArgs
            {
                Notificaction = "Event Hub connection established.",
                NotificationSource = NotificationSource.Open,
                PartitionId = context.Lease.PartitionId
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
        ///     <see cref="NotificationReceived" /> and <see cref="EventReceived" />
        ///     events.
        /// </remarks>
        public async Task ProcessEventsAsync(PartitionContext context,
            IEnumerable<EventData> messages)
        {
            foreach (var @event in messages
                .Select(eventData => Encoding.UTF8.GetString(eventData.GetBytes())))
            {
                OnEventReceived(new EventReceivedEventArgs
                {
                    Event = @event,
                    PartitionId = context.Lease.PartitionId
                });
                OnNotification(new NotificationReceivedEventArgs
                {
                    Notificaction = "Event received.",
                    NotificationSource = NotificationSource.ProcessEvents,
                    PartitionId = context.Lease.PartitionId
                });
            }
            if (_checkpointStopWatch.Elapsed > _checkPointInterval)
            {
                await context.CheckpointAsync();
                _checkpointStopWatch.Restart();
            }
        }

        /// <summary>
        ///     <see cref="NotificationReceived" /> is raised when an operational event of
        ///     interest occurs during this instance' life-cycle.
        /// </summary>
        public event NotificactionReceivedEventHandler NotificationReceived;

        /// <summary>
        ///     <see cref="EventReceived" /> is raised when an event is downloaded from
        ///     an Event Hub partition, and contains the event itself in serialised,
        ///     UTF-8-format.
        /// </summary>
        public event EventReceivedEventHandler EventReceived;

        /// <summary>
        ///     <see cref="OnNotification" /> invokes any subscribers to
        ///     <see cref="NotificationReceived" />.
        /// </summary>
        /// <param name="e">
        ///     <see cref="e" /> is an <see cref="NotificationReceivedEventArgs" />
        ///     instance containing a notification metadata.
        /// </param>
        private void OnNotification(NotificationReceivedEventArgs e)
        {
            NotificationReceived?.Invoke(this, e);
        }

        /// <summary>
        ///     <see cref="OnEventReceived" /> invokes any subscribers to
        ///     <see cref="EventReceived" />.
        /// </summary>
        /// <param name="e">
        ///     <see cref="e" /> is an <see cref="EventReceivedEventArgs" />
        ///     instance containing event metadata.
        /// </param>
        private void OnEventReceived(EventReceivedEventArgs e)
        {
            EventReceived?.Invoke(this, e);
        }
    }
}