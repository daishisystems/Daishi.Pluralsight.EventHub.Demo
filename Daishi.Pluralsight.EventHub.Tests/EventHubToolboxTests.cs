using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Daishi.Pluralsight.EventHub.Tests
{
    [TestClass]
    public class EventHubToolboxTests
    {
        [TestCleanup]
        public void CleanUp()
        {
            EventHubToolbox.Instance.UnsubscribeAll(eventProcessorHost => { });
        }

        [TestMethod]
        public void SubscribeCachesEventProcessorHostInstance()
        {
            EventHubToolbox.Instance.Subscribe(
                "HOSTNAME",
                "CONNECTIONSTRING",
                "EVENTHUBNAME",
                "STORAGEACCOUNTNAME",
                "STORAGEACCOUNTKEY",
                new EventReceiver(TimeSpan.MinValue),
                host => { },
                (host,
                    factory,
                    options) =>
                { },
                true);

            Assert.AreEqual(1, EventHubToolbox.Instance.EventProcessorHosts.Count);
        }

        [TestMethod]
        public void UnSubscribeClearsAllManagedEventProcessorHostInstances()
        {
            EventHubToolbox.Instance.Subscribe(
                "HOSTNAME",
                "CONNECTIONSTRING",
                "EVENTHUBNAME",
                "STORAGEACCOUNTNAME",
                "STORAGEACCOUNTKEY",
                new EventReceiver(TimeSpan.MinValue),
                host => { },
                (host,
                    factory,
                    options) =>
                { },
                true);

            EventHubToolbox.Instance.UnsubscribeAll(eventProcessorHost => { });
            Assert.AreEqual(0, EventHubToolbox.Instance.EventProcessorHosts.Count);
        }

        [TestMethod]
        public void UnSubscribeRemovesEventProcessorHost()
        {
            var eventProcessorHostName = "HOSTNAME";

            EventHubToolbox.Instance.Subscribe(
                eventProcessorHostName,
                "CONNECTIONSTRING",
                "EVENTHUBNAME",
                "STORAGEACCOUNTNAME",
                "STORAGEACCOUNTKEY",
                new EventReceiver(TimeSpan.MinValue),
                host => { },
                (host,
                    factory,
                    options) =>
                { },
                true);

            EventHubToolbox.Instance.UnSubscribe(eventProcessorHostName, host => { });
            Assert.AreEqual(0, EventHubToolbox.Instance.EventProcessorHosts.Count);
        }
    }
}