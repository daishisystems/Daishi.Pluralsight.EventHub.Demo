#region Includes

using System;
using System.Configuration;
using Microsoft.ServiceBus.Messaging;

#endregion

namespace Daishi.Pluralsight.EventHub.WebTrafficProcessor {
    internal static class Program {
        private static volatile int eventCount;
        private static DateTime startTime;
        private static double elapsedTime;
        private static int totalNumExpectedEvents;

        private static void Main(string[] args) {
            totalNumExpectedEvents =
                int.Parse(ConfigurationManager.AppSettings["TotalNumExpectedEvents"]);
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine(@"Press Ctrl-C to stop the processor.");
            Console.WriteLine(@"Press Enter to start now...");
            Console.ReadLine();

            try {
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
                    Guid.NewGuid().ToString(),
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
            catch (Exception exception) {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(exception.Message);
                if (exception.InnerException != null) {
                    Console.WriteLine(exception.InnerException.Message);
                    if (exception.InnerException.InnerException != null) {
                        Console.WriteLine(exception.InnerException.InnerException.Message);
                    }
                }
                Console.ReadLine();
            }
        }

        private static void EventReceiver_NotificationReceived(
            object sender,
            NotificationReceivedEventArgs e) {
            if (!(e.NotificationSource.HasFlag(NotificationSource.Open) |
                  e.NotificationSource.HasFlag(NotificationSource.Close))) return;

            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($@"Notification: {e.Notificaction}");
            Console.WriteLine($@"Partition: {e.PartitionId}");
            Console.WriteLine($@"Source: {e.NotificationSource}");
        }

        private static void EventProcessorOptions_ExceptionReceived(
            object sender,
            ExceptionReceivedEventArgs e) {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(e.Exception.Message);
            if (e.Exception.InnerException != null) {
                Console.WriteLine(e.Exception.InnerException.Message);
                if (e.Exception.InnerException.InnerException != null) {
                    Console.WriteLine(e.Exception.InnerException.InnerException.Message);
                }
            }
            Console.ReadLine();
        }

        private static void EventReceiver_EventReceived(
            object sender,
            EventReceivedEventArgs e) {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($@"Event received: {e.Event} on partition {e.PartitionId}.");
            if (eventCount++.Equals(0)) {
                startTime = DateTime.UtcNow;
            }
            else if (eventCount.Equals(totalNumExpectedEvents)) {
                elapsedTime = DateTime.UtcNow.Subtract(startTime).TotalMilliseconds;
                Console.ForegroundColor = ConsoleColor.Gray;
                Console.WriteLine($@"Elapsed time: {elapsedTime}ms.");
            }
        }
    }
}