using EventBus.Base;
using EventBus.Base.Abstraction;
using EventBus.Factory;
using EventBus.UnitTest.Events.EventHandlers;
using EventBus.UnitTest.Events.Events;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace EventBus.UnitTest
{
    [TestClass]
    public class EventBusTests
    {
        private ServiceCollection services;

        public EventBusTests()
        {
            services = new ServiceCollection();
            services.AddLogging(configure => configure.AddConsole());
        }

        [TestMethod]
        public void Subscribe_Event_On_RabbitMQ_Test()
        {
            services.AddSingleton<IEventBus>(serviceProvider =>
            {

                return EventBusFactory.Create(GetRabbitMQConfig(), serviceProvider);
            });

            ServiceProvider serviceProvider = services.BuildServiceProvider();
            IEventBus? eventBus = serviceProvider.GetService<IEventBus>();

            eventBus.Subscribe<OrderCreatedIntegrationEvent, OrderCreatedIntegratedEventHandler>();
            //eventBus.UnSubscribe<OrderCreatedIntegrationEvent, OrderCreatedIntegratedEventHandler>();

        }

        [TestMethod]
        public void Subscribe_Event_On_Azure_Test()
        {
            services.AddSingleton<IEventBus>(serviceProvider =>
            {
                return EventBusFactory.Create(GetAzureConfig(), serviceProvider);
            });

            ServiceProvider serviceProvider = services.BuildServiceProvider();
            IEventBus? eventBus = serviceProvider.GetService<IEventBus>();

            eventBus.Subscribe<OrderCreatedIntegrationEvent, OrderCreatedIntegratedEventHandler>();
            //eventBus.UnSubscribe<OrderCreatedIntegrationEvent, OrderCreatedIntegratedEventHandler>();

        }

        [TestMethod]
        public void Send_Message_To_RabbitMQ_Test()
        {
            services.AddSingleton<IEventBus>(sp =>
            {
                return EventBusFactory.Create(GetRabbitMQConfig(), sp);
            });

            ServiceProvider serviceProvider = services.BuildServiceProvider();
            IEventBus eventBus = serviceProvider.GetRequiredService<IEventBus>();
            eventBus.Publish(new OrderCreatedIntegrationEvent(1));
        }

        [TestMethod]
        public void Send_Message_To_Azure_Test()
        {
            services.AddSingleton<IEventBus>(sp =>
            {
                return EventBusFactory.Create(GetAzureConfig(), sp);
            });

            ServiceProvider serviceProvider = services.BuildServiceProvider();
            IEventBus eventBus = serviceProvider.GetRequiredService<IEventBus>();
            eventBus.Publish(new OrderCreatedIntegrationEvent(2));
        }


        private EventBusConfig GetAzureConfig()
        {
            return new EventBusConfig()
            {
                ConnectionRetryCount = 5,
                SubscriberClientAppName = "EventBus.UnitTest",
                DefaultTopicName = "SellingBuddyTopicName",
                EventBusType = EventBusType.AzureServiceBus,
                EventNameSuffix = "IntegrationEvent",
                EventBusConnectionString = ""
            };
        }
        private EventBusConfig GetRabbitMQConfig()
        {
            return new EventBusConfig()
            {
                ConnectionRetryCount = 5,
                SubscriberClientAppName = "EventBus.UnitTest",
                DefaultTopicName = "SellingBuddyTopicName",
                EventBusType = EventBusType.RabbitMQ,
                EventNameSuffix = "IntegrationEvent"
            }
    }
    }