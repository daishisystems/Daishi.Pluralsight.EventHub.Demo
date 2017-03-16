#region Includes

using Microsoft.ServiceBus.Messaging;

#endregion

namespace Daishi.Pluralsight.EventHub {
    public class BridgeEventProcessorFactory : IEventProcessorFactory {
        private readonly IEventProcessor _eventReceiver;

        public BridgeEventProcessorFactory(IEventProcessor eventReceiver) {
            _eventReceiver = eventReceiver;
        }

        public IEventProcessor CreateEventProcessor(PartitionContext context) {
            return _eventReceiver;
        }
    }
}