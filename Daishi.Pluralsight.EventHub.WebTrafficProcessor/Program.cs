﻿using System;
using System.Configuration;
using Microsoft.ServiceBus.Messaging;

namespace Daishi.Pluralsight.EventHub.WebTrafficProcessor
{
    internal class Program
    {
        private static void Main(string[] args)
        {
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
                eventReceiver.Notification += EventReceiver_Notification;
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
            EventReceiverEventArgs e)
        {
            Console.WriteLine($"Event received: {e.Message}");
        }

        private static void EventReceiver_Notification(
            object sender,
            EventReceiverEventArgs e)
        {
            // todo: Include PartitionId, Source (Close/Open).
            //Console.ForegroundColor = ConsoleColor.Green;
            //Console.WriteLine($"Notification received: {e.Message}");
        }
    }
}