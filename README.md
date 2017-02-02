<a href="http://insidethecpu.com">![Image of insidethecpu](https://dl.dropboxusercontent.com/u/26042707/Daishi%20Systems%20Icon%20with%20Text%20%28really%20tiny%20with%20photo%29.png)</a>
# Getting Started with Azure Event Hub with C&#35;
[![Join the chat at https://gitter.im/Daishi-Pluralsight-EventHub-Demo](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/Daishi-Pluralsight-EventHub-Demo/Lobby?utm_source=share-link&utm_medium=link&utm_campaign=share-link)
[![Build status](https://ci.appveyor.com/api/projects/status/fflciv7os94nxl9u?svg=true)](https://ci.appveyor.com/project/daishisystems/daishi-pluralsight-eventhub-demo)
![Course Image](https://dl.dropboxusercontent.com/u/26042707/Fotolia_107425663_S%20-%20Copy.jpg)
## Overview
This repository contains supplemental material to compliment the **Pluralsight course** **Getting Started with Azure Event Hub with C#**. This includes a custom **C# library**, designed to interface with Event Hub, a series of **Unit Tests**, and a sample **Console Application**.
## Connecting to Event Hub
```cs
var eventHubConnectionString =
ConfigurationManager.AppSettings["EventHubConnectionString"];

EventHubToolbox.Instance.Connect(eventHubConnectionString);

if (EventHubToolbox.Instance.IsConnected)
{
    Console.WriteLine(@"Connected OK!");
}
```
## Publishing Events
```cs
await EventHubToolbox.Instance.SendAsync("TEST");
```
## Subscribing to Event Hub
```cs
var eventReceiver = new EventReceiver(TimeSpan.FromMinutes(5));
eventReceiver.Notification += EventReceiver_Notification;
eventReceiver.EventReceived += EventReceiverEventReceived;

var eventProcessorOptions = new EventProcessorOptions();
eventProcessorOptions.ExceptionReceived += EventProcessorOptions_ExceptionReceived;

await EventHubToolbox.Instance.SubscribeAsync(
    "MyEventHost",
    eventHubConnectionStringShort,
    eventHubName,
    storageAccountName,
    storageAccountKey,
    eventReceiver,
    EventHubToolbox.UnRegisterAsync,
    EventHubToolbox.RegisterAsync,
    false,
    eventProcessorOptions);

if (EventHubToolbox.Instance.IsSubscribedToAny)
{
    Console.WriteLine(@"Subscribed!");
}

private static void EventReceiver_Notification(object sender,
    EventReceiverEventArgs e)
{
    Console.WriteLine(@"Notification received: {0}", e.Message);
}

private static void EventReceiverEventReceived(object sender,
    EventReceiverEventArgs e)
{
    Console.WriteLine(@"Event received: {0}", e.Message);
}

private static void EventProcessorOptions_ExceptionReceived(
    object sender,
    ExceptionReceivedEventArgs e)
{
    Console.ForegroundColor = ConsoleColor.Red;
    Console.WriteLine(e.Exception.Message);
    Console.ForegroundColor = ConsoleColor.Green;
}
```
## Contact the Developer
Please reach out and contact me for questions, suggestions, or to just talk tech in general.


<a href="http://insidethecpu.com/feed/">![RSS](https://dl.dropboxusercontent.com/u/26042707/rss.png)</a><a href="https://twitter.com/daishisystems">![Twitter](https://dl.dropboxusercontent.com/u/26042707/twitter.png)</a><a href="https://www.linkedin.com/in/daishisystems">![LinkedIn](https://dl.dropboxusercontent.com/u/26042707/linkedin.png)</a><a href="https://plus.google.com/102806071104797194504/posts">![Google+](https://dl.dropboxusercontent.com/u/26042707/g.png)</a><a href="https://www.youtube.com/user/daishisystems">![YouTube](https://dl.dropboxusercontent.com/u/26042707/youtube.png)</a>
