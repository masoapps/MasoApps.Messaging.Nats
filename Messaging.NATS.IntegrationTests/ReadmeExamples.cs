using System.Text.Json;
using System.Text.Json.Serialization;
using Messaging.NATS.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

// The code samples of README.md, copied here so they keep compiling. Keep the two in step: a README change that
// does not compile here is a broken README. The handler classes carry attributes but no test scans this assembly.
namespace Messaging.NATS.IntegrationTests.ReadmeExamples;

public sealed record OrderPlaced(string OrderId, decimal Amount);

public sealed record PaymentCaptured(string OrderId, decimal Amount);

public sealed record JourneyCheck(string JourneyId);

public sealed record NotificationDue(string NotificationId);

public sealed record SessionArriving(string SessionId);

public sealed record LegacyOrder(string OrderId);

public sealed record PriceRequest(string Sku);

public sealed record PriceResponse(string Sku, decimal Price);

public sealed record Stock(int Count);

public interface IOrderService
{
    Task ReserveStockAsync(OrderPlaced order, string idempotencyKey, CancellationToken cancellationToken);

    Task CaptureAsync(PaymentCaptured payment, CancellationToken cancellationToken);
}

public interface INotificationStore
{
    Task<string?> CurrentScheduleIdAsync(string notificationId, CancellationToken cancellationToken);

    Task SaveScheduleIdAsync(string notificationId, string scheduleId, CancellationToken cancellationToken);
}

public interface INotificationSender
{
    Task SendAsync(string notificationId, CancellationToken cancellationToken);
}

public static class QuickStart
{
    public static void Register(HostApplicationBuilder builder)
    {
        builder.Services.AddNatsMessaging(options =>
        {
            options.NatsUrl = "nats://localhost:4222";
            options.ServiceName = "orders-worker";
            options.HandlerTypes = [typeof(OrderPlacedHandler)]; // or Assemblies = [typeof(Program).Assembly]
            options.StreamDefinitions.Add(new StreamDefinition
            {
                Name = "ORDERS",
                Subjects = ["orders.>"],
                Replicas = 3
            });
        });

        builder.Services.AddHealthChecks().AddNatsMessagingCheck(tags: ["ready"]);
    }
}

[JsonSerializable(typeof(OrderPlaced))]
public partial class AppJsonContext : JsonSerializerContext;

public static class Serialisation
{
    public static void Configure(NatsMessagingOptions options)
    {
        // Reflection-based, with your options
        options.JsonSerializerOptions = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

        // Source-generated
        options.SerializerContext = AppJsonContext.Default;
    }
}

[NatsConsumer(Stream = "ORDERS", Subject = "orders.placed")]
public sealed class OrderPlacedHandler(IOrderService orders) : INatsHandler<OrderPlaced>
{
    public Task HandleAsync(MessageContext<OrderPlaced> context, CancellationToken cancellationToken) =>
        orders.ReserveStockAsync(context.Message, context.MessageId, cancellationToken);
}

public sealed class Checkout(IPublisher publisher, ILogger<Checkout> logger)
{
    public async Task PlaceAsync(OrderPlaced order)
    {
        try
        {
            var result = await publisher.PublishAsync("orders.placed", order,
                new PublishOptions { MessageId = order.OrderId });
            logger.LogInformation("Stored as {Stream}:{Sequence}", result.Stream, result.Sequence);
        }
        catch (NatsPublishException e) when (e.OutcomeUnknown)
        {
            // The message may be stored. Retrying with the same MessageId is safe: the stream drops a duplicate.
            throw;
        }
    }

    public async Task PlaceAllAsync(IEnumerable<OrderPlaced> orders)
    {
        await Task.WhenAll(orders.Select(order =>
            publisher.PublishAsync("orders.placed", order, new PublishOptions { MessageId = order.OrderId })));
    }

    public Task MetricAsync(OrderPlaced order) =>
        publisher.PublishAsync("orders.metrics", order, new PublishOptions { FireAndForget = true });
}

[NatsConsumer(Stream = "ORDERS", Subject = "orders.placed", MessageType = typeof(OrderPlaced), MaxConcurrency = 8)]
[NatsConsumer(Stream = "ORDERS", Subject = "orders.paid", MessageType = typeof(PaymentCaptured),
    MaxDeliveryAttempts = 10, RetryDelaysSeconds = [5, 30, 300])]
public sealed class OrderActivity(IOrderService orders) : INatsHandler<OrderPlaced>, INatsHandler<PaymentCaptured>
{
    public Task HandleAsync(MessageContext<OrderPlaced> context, CancellationToken cancellationToken) =>
        orders.ReserveStockAsync(context.Message, context.MessageId, cancellationToken);

    public Task HandleAsync(MessageContext<PaymentCaptured> context, CancellationToken cancellationToken) =>
        orders.CaptureAsync(context.Message, cancellationToken);
}

[NatsConsumer(Stream = "ORDERS", Subject = "orders.legacy", QueueGroup = "order-worker")]
public sealed class LegacyOrderConsumer : INatsConsumer<LegacyOrder>
{
    public async Task HandleMessageAsync(INatsJSMsg<LegacyOrder> message, CancellationToken cancellationToken)
    {
        if (message.Data is null)
        {
            await message.AckTerminateAsync(cancellationToken: cancellationToken);
            return;
        }

        // Returning without a settle acks the message.
    }
}

[NatsConsumer(Stream = "ORDERS", Subject = "orders.import")]
public sealed class ImportHandler : INatsHandler<OrderPlaced>
{
    public Task HandleAsync(MessageContext<OrderPlaced> context, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(context.Message.OrderId))
        {
            throw new PoisonMessageException("OrderId is missing");
        }

        return Task.CompletedTask;
    }
}

public static class Streams
{
    public static void Register(IServiceCollection services)
    {
        services.AddNatsMessaging(options =>
        {
            options.NatsUrl = "nats://localhost:4222";
            options.ServiceName = "timers-api";
            options.StreamProvisioning = StreamProvisioning.CreateOrUpdate;

            var timers = StreamDefinition.Scheduled("TIMERS", "timers.>");
            timers.Replicas = 3;
            options.StreamDefinitions.Add(timers);

            options.StreamDefinitions.Add(new StreamDefinition
            {
                Name = "EVENTS",
                Subjects = ["events.>"],
                Retention = StreamConfigRetention.Limits,
                MaxAge = (int)TimeSpan.FromDays(7).TotalSeconds,
                Replicas = 3,
                DuplicateWindow = TimeSpan.FromMinutes(10)
            });

            options.DeadLetterStream!.Replicas = 3;
        });
    }
}

public sealed class Timers(IPublisher publisher, INotificationStore store)
{
    public Task<ScheduleResult> FollowUpAsync(string journeyId) =>
        publisher.ScheduleAsync("timers.journey.check", new JourneyCheck(journeyId),
            DateTimeOffset.UtcNow.AddMinutes(5), new ScheduleOptions { Key = journeyId });

    public Task<ScheduleResult> ArrivingAsync(string sessionId) =>
        publisher.ScheduleAsync("timers.session.arriving", new SessionArriving(sessionId),
            DateTimeOffset.UtcNow.AddSeconds(90),
            new ScheduleOptions { Key = sessionId, Mode = ScheduleMode.CreateIfAbsent });

    public async Task NotifyLaterAsync(string notificationId, DateTimeOffset at, CancellationToken cancellationToken)
    {
        var result = await publisher.ScheduleAsync("timers.notification.due", new NotificationDue(notificationId), at,
            new ScheduleOptions { Key = notificationId }, cancellationToken);
        await store.SaveScheduleIdAsync(notificationId, result.ScheduleId, cancellationToken);
    }

    public Task<bool> CancelAsync(string notificationId) =>
        publisher.CancelScheduleAsync("timers.notification.due", notificationId);
}

[NatsConsumer(Stream = "TIMERS", Subject = "timers.notification.due")]
public sealed class NotificationDueHandler(INotificationStore store, INotificationSender sender)
    : INatsHandler<NotificationDue>
{
    public async Task HandleAsync(MessageContext<NotificationDue> context, CancellationToken cancellationToken)
    {
        var id = context.Message.NotificationId;
        if (context.ScheduleId != await store.CurrentScheduleIdAsync(id, cancellationToken))
        {
            return; // Replaced or cancelled after this fire started: not the latest schedule.
        }

        await sender.SendAsync(id, cancellationToken);
    }
}

public static class Health
{
    public static void Register(IServiceCollection services)
    {
        services.AddNatsMessaging(options =>
        {
            options.NatsUrl = "nats://localhost:4222";
            options.ServiceName = "api";
            options.OnComponentFaulted = FaultAction.Report;
        });

        services.AddHealthChecks()
            .AddNatsMessagingCheck(tags: ["ready"], componentFilter: NatsHealthFilters.ExcludeConsumers);
    }

    public static void GateStart(NatsMessagingOptions options)
    {
        options.WaitUntilStarted = true;
        options.StartTimeout = TimeSpan.FromSeconds(30);
    }

    public static ComponentState? OrderConsumerState(NatsMessagingStatus status) =>
        status.Get("consumer:ORDERS/orders-worker_OrderPlacedHandler")?.State;

    public static string Describe(NatsMessagingStatus status) =>
        string.Join(Environment.NewLine,
            status.Components.Select(c => $"{c.Name}: {c.State} {c.Error} {c.Warning}"));
}

public static class DeadLetterReplay
{
    public static async Task<int> ReplayOrdersAsync(NatsDeadLetters deadLetters, CancellationToken cancellationToken)
    {
        var result = await deadLetters.ReplayAsync(new ReplayOptions
        {
            SubjectFilter = "orders.>",
            Consumer = null,
            Targeted = true,
            Max = 100
        }, cancellationToken);

        foreach (var skip in result.Skipped)
        {
            Console.WriteLine($"kept {skip.DeadLetterSequence}: {skip.Reason}");
        }

        return result.Replayed;
    }
}

public static class Telemetry
{
    public static void Register(IServiceCollection services)
    {
        services.AddOpenTelemetry()
            .WithTracing(t => t.AddSource(NatsMessagingTelemetry.SourceName, "NATS.Net"))
            .WithMetrics(m => m.AddMeter(NatsMessagingTelemetry.SourceName));
    }
}

[NatsRequestHandler(Subject = "prices.get")]
public sealed class PriceHandler : INatsRequestHandler<PriceRequest, PriceResponse>
{
    public Task<PriceResponse> HandleRequestAsync(string subject, PriceRequest request) =>
        Task.FromResult(new PriceResponse(request.Sku, 9.99m));
}

public sealed class Prices(IPublisher publisher, NatsCacheFactory caches)
{
    public Task<PriceResponse?> GetAsync(string sku) =>
        publisher.RequestAsync<PriceRequest, PriceResponse>("prices.get", new PriceRequest(sku),
            timeout: TimeSpan.FromSeconds(2));

    public async Task<int> StockAsync(string sku, CancellationToken cancellationToken)
    {
        var cache = await caches.CreateAsync("stock", TimeSpan.FromMinutes(10), cancellationToken);
        var entry = await cache.GetAsync<Stock>(sku, cancellationToken);
        return entry.Value?.Count ?? 0;
    }
}

[TestClass]
public class ReadmeExamplesTests
{
    [TestMethod]
    public void QuickStart_RegistersWithoutAServer()
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
        builder.Services.AddLogging();
        builder.Services.AddSingleton<IOrderService, FakeOrderService>();

        QuickStart.Register(builder);

        using var host = builder.Build();
        Assert.IsInstanceOfType<NatsPublisher>(host.Services.GetRequiredService<IPublisher>());
    }

    [TestMethod]
    public void StreamsAndHealth_RegisterWithoutAServer()
    {
        var streams = new ServiceCollection();
        Streams.Register(streams);
        var health = new ServiceCollection();
        Health.Register(health);

        var options = streams.BuildServiceProvider().GetRequiredService<NatsMessagingOptions>();
        CollectionAssert.Contains(options.StreamDefinitions[0].ToStreamConfig().Subjects!.ToList(), "_sched.TIMERS.>");
        Assert.AreEqual(3, options.DeadLetterStream!.Replicas);
    }

    // The test-double sample from the README.
    [TestMethod]
    public async Task Checkout_PublishesTheOrder()
    {
        var publisher = new InMemoryPublisher();
        var checkout = new Checkout(publisher, Microsoft.Extensions.Logging.Abstractions.NullLogger<Checkout>.Instance);

        await checkout.PlaceAsync(new OrderPlaced("o-1", 10m));

        var order = publisher.MessagesOf<OrderPlaced>("orders.placed").Single();
        Assert.AreEqual("o-1", order.OrderId);
        Assert.AreEqual("o-1", publisher.Published.Single().Options!.MessageId);
    }

    [TestMethod]
    public async Task Checkout_WhenNoStreamCapturesTheSubject_Throws()
    {
        var publisher = new InMemoryPublisher();
        var checkout = new Checkout(publisher, Microsoft.Extensions.Logging.Abstractions.NullLogger<Checkout>.Instance);

        publisher.FailOn("orders.placed",
            new NatsPublishException(PublishFailure.NoStream, "orders.placed", null, null, "No stream"));
        publisher.FailWhen(call => call.IsScheduled ? new NatsMessagingConfigurationException("no schedules") : null);

        var error = await Assert.ThrowsExactlyAsync<NatsPublishException>(() => checkout.PlaceAsync(new OrderPlaced("o-1", 10m)));
        Assert.AreEqual(PublishFailure.NoStream, error.Failure);
        Assert.HasCount(0, publisher.Published);
    }

    [TestMethod]
    public async Task NotificationDueHandler_IgnoresAStaleFire()
    {
        var store = new FakeNotificationStore { Current = "schedule-2" };
        var sender = new FakeSender();
        var handler = new NotificationDueHandler(store, sender);

        await handler.HandleAsync(TestMessageContext.Create(new NotificationDue("n-1"), scheduleId: "schedule-1"),
            CancellationToken.None);
        await handler.HandleAsync(TestMessageContext.Create(new NotificationDue("n-1"), scheduleId: "schedule-2"),
            CancellationToken.None);

        CollectionAssert.AreEqual(new[] { "n-1" }, sender.Sent);
    }

    [TestMethod]
    public async Task Timers_KeepOnePendingSchedulePerKey()
    {
        var publisher = new InMemoryPublisher();
        var timers = new Timers(publisher, new FakeNotificationStore());

        await timers.FollowUpAsync("journey-1");
        await timers.FollowUpAsync("journey-1");
        var first = await timers.ArrivingAsync("session-1");
        var second = await timers.ArrivingAsync("session-1");

        Assert.HasCount(2, publisher.PendingSchedules);
        Assert.IsTrue(first.Created);
        Assert.IsFalse(second.Created);
        Assert.AreEqual(first.ScheduleId, second.ScheduleId);
    }

    private sealed class FakeOrderService : IOrderService
    {
        public Task ReserveStockAsync(OrderPlaced order, string idempotencyKey, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public Task CaptureAsync(PaymentCaptured payment, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class FakeNotificationStore : INotificationStore
    {
        public string? Current { get; set; }

        public Task<string?> CurrentScheduleIdAsync(string notificationId, CancellationToken cancellationToken) =>
            Task.FromResult(Current);

        public Task SaveScheduleIdAsync(string notificationId, string scheduleId, CancellationToken cancellationToken)
        {
            Current = scheduleId;
            return Task.CompletedTask;
        }
    }

    private sealed class FakeSender : INotificationSender
    {
        public List<string> Sent { get; } = [];

        public Task SendAsync(string notificationId, CancellationToken cancellationToken)
        {
            Sent.Add(notificationId);
            return Task.CompletedTask;
        }
    }
}
