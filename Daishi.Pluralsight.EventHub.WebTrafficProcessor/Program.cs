using System;
using System.Configuration;
using Microsoft.ServiceBus.Messaging;

namespace Daishi.Pluralsight.EventHub.WebTrafficProcessor
{
    internal class Program
    {
        private static volatile int _eventCount;
        private static DateTime _startTime;
        private static double _elapsedTime;
        private static int _totalNumExpectedEvents;

        private static void Main(string[] args)
        {
            _totalNumExpectedEvents =
                int.Parse(ConfigurationManager.AppSettings["TotalNumExpectedEvents"]);
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine(@"Press Ctrl-C to stop the processor.");
            Console.WriteLine(@"Press Enter to start now...");
            Console.ReadLine();

            try
            {
                Console.Write(@"Subscribing to Event Hub...");

                var eventHubConnectionString =
                    ConfigurationManager.AppSettings["EventHubConnectionString"];
                var eventHubName =
                    ConfigurationManager.AppSettings["EventHubName"];
                var storageAccountName =
                    ConfigurationManager.AppSettings["StorageAccountName"];
                var storageAccountKey =
                    ConfigurationManager.AppSettings["StorageAccountKey"];

                var eventReceiver = new EventReceiver(TimeSpan.FromMinutes(5));
                eventReceiver.NotificationReceived += EventReceiver_NotificationReceived;
                eventReceiver.EventReceived += EventReceiver_EventReceived;

                var eventProcessorOptions = new EventProcessorOptions();
                eventProcessorOptions.ExceptionReceived += EventProcessorOptions_ExceptionReceived;

                EventHubToolbox.Instance.Subscribe(
                    "Pluralsight Demo",
                    eventHubConnectionString,
                    eventHubName,
                    storageAccountName,
                    storageAccountKey,
                    eventReceiver,
                    EventHubToolbox.UnRegister,
                    EventHubToolbox.Register,
                    false,
                    eventProcessorOptions);

                Console.Write(@"OK");
                Console.WriteLine();
                Console.ReadLine();

                EventHubToolbox.Instance.UnsubscribeAll(EventHubToolbox.UnRegister);
            }
            catch (Exception exception)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(exception.Message);
                if (exception.InnerException != null)
                {
                    Console.WriteLine(exception.InnerException.Message);
                }
                Console.ReadLine();
            }
        }

        private static void EventReceiver_NotificationReceived(
            object sender,
            NotificationReceivedEventArgs e)
        {
            if (!(e.NotificationSource.HasFlag(NotificationSource.Open) |
                  e.NotificationSource.HasFlag(NotificationSource.Close))) return;

            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"Notification: {e.Notificaction}");
            Console.WriteLine($"Partition: {e.PartitionId}");
            Console.WriteLine($"Source: {e.NotificationSource}");
        }

        private static void EventProcessorOptions_ExceptionReceived(
            object sender,
            ExceptionReceivedEventArgs e)
        {
            Console.Clear();
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(e.Exception.Message);
            if (e.Exception.InnerException != null)
            {
                Console.WriteLine(e.Exception.InnerException.Message);
            }
            Console.ReadLine();
        }

        private static void EventReceiver_EventReceived(
            object sender,
            EventReceivedEventArgs e)
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"Event received: {e.Event} on partition {e.PartitionId}.");
            if (_eventCount++.Equals(0))
            {
                _startTime = DateTime.UtcNow;
            }
            else if (_eventCount.Equals(_totalNumExpectedEvents))
            {
                _elapsedTime = DateTime.UtcNow.Subtract(_startTime).TotalMilliseconds;
                Console.ForegroundColor = ConsoleColor.Gray;
                Console.WriteLine($"Elapsed time: {_elapsedTime}ms.");
            }
        }
    }
}