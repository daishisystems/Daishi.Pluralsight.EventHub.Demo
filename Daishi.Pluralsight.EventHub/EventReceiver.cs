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
    public class EventReceiver : IEventProcessor
    {
        /// <summary>
        ///     <see cref="EventHandler" /> is a function pointer that facilitates both
        ///     <see cref="MessageReceived" /> and
        ///     <see cref="Notification" /> events.
        /// </summary>
        /// <param name="sender">The instance that invoked the event.</param>
        /// <param name="e">The <see cref="EventReceiverEventArgs" /> instance.</param>
        public delegate void EventHandler(
            object sender,
            EventReceiverEventArgs e);

        private Stopwatch _checkpointStopWatch;

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
        /// <returns><see cref="Task{TResult}" />(an empty async response).</returns>
        async Task IEventProcessor.CloseAsync(
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
        /// todo: Start here.
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        Task IEventProcessor.OpenAsync(PartitionContext context)
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

        async Task IEventProcessor.ProcessEventsAsync(PartitionContext context,
            IEnumerable<EventData> messages)
        {
            foreach (var @event in messages
                .Select(eventData => Encoding.UTF8.GetString(eventData.GetBytes())))
            {
                OnMessageReceived(new EventReceiverEventArgs
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
            if (_checkpointStopWatch.Elapsed > TimeSpan.FromMinutes(5))
            {
                await context.CheckpointAsync();
                _checkpointStopWatch.Restart();
            }
        }

        public event EventHandler Notification;
        public event EventHandler MessageReceived;

        protected virtual void OnNotification(EventReceiverEventArgs e)
        {
            Notification?.Invoke(this, e);
        }

        protected virtual void OnMessageReceived(EventReceiverEventArgs e)
        {
            MessageReceived?.Invoke(this, e);
        }
    }
}