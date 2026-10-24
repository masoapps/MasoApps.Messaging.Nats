using System.Diagnostics;
using Messaging.NATS.IntegrationTests.Handlers;

namespace Messaging.NATS.IntegrationTests;

[TestClass]
public class TelemetryTests
{
    private const string Outcome = "messaging.nats.outcome";
    private const string ConsumerGroup = "messaging.consumer.group.name";

    [TestMethod]
    public async Task HandlerSpan_OfAnImmediatePublish_IsParentedOnThePublisherTrace()
    {
        using var activities = new ActivityCapture();
        var current = new TaskCompletionSource<ActivityContext?>();
        var probe = new Probe<MessageContext<OrderEvent>>
        {
            Behaviour = (_, _, _) =>
            {
                current.TrySetResult(Activity.Current?.Context);
                return Task.CompletedTask;
            }
        };
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>([probe], null,
            new NatsConsumerAttribute { Subject = ".created" });
        var subject = $"{scenario.Root}.created";

        PublishResult published;
        ActivityContext root;
        using (var rootActivity = ActivityCapture.TestSource.StartActivity("request"))
        {
            root = rootActivity!.Context;
            published = await scenario.Publisher.PublishAsync(subject, new OrderEvent("ORD-1", 1m));
        }

        var publish = await activities.WaitForAsync(a => a.OperationName == $"publish {subject}",
            TimeSpan.FromSeconds(5), "for the publish");
        var process = await activities.WaitForAsync(a => a.OperationName == $"process {subject}",
            TimeSpan.FromSeconds(10), "for the handler");

        Assert.AreEqual(ActivityKind.Producer, publish.Kind);
        Assert.AreEqual(root.SpanId, publish.ParentSpanId);
        Assert.AreEqual(published.MessageId, publish.Tag("messaging.message.id"));
        Assert.AreEqual("1", publish.Tag("messaging.nats.publish.attempts"));

        Assert.AreEqual(ActivityKind.Consumer, process.Kind);
        Assert.AreEqual(root.TraceId, process.TraceId);
        Assert.AreEqual(publish.SpanId, process.ParentSpanId);
        Assert.AreEqual(process.SpanId, (await current.Task)?.SpanId, "The handler runs inside its span");
        Assert.AreEqual("nats", process.Tag("messaging.system"));
        Assert.AreEqual("process", process.Tag("messaging.operation.name"));
        Assert.AreEqual(subject, process.Tag("messaging.destination.name"));
        Assert.AreEqual(scenario.Durable, process.Tag(ConsumerGroup));
        Assert.AreEqual(published.MessageId, process.Tag("messaging.message.id"));
        Assert.AreEqual(scenario.Stream.Name, process.Tag("messaging.nats.stream"));
        Assert.AreEqual(published.Sequence.ToString(), process.Tag("messaging.nats.stream_sequence"));
        Assert.AreEqual("1", process.Tag("messaging.nats.delivery_count"));
        Assert.AreEqual(typeof(OrderHandler).FullName, process.Tag("messaging.nats.handler"));
        Assert.AreEqual("ack", process.Tag(Outcome));
        Assert.AreEqual(ActivityStatusCode.Unset, process.Status);
    }

    [TestMethod]
    public async Task HandlerSpan_OfAFiredSchedule_IsParentedOnTheSchedulingTrace()
    {
        using var activities = new ActivityCapture();
        var probe = new Probe<MessageContext<OrderEvent>>();
        var root = TestNames.Unique("timers");
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>(
            StreamDefinition.Scheduled(TestNames.Unique("TIMERS"), $"{root}.>"), root, [probe], null,
            new NatsConsumerAttribute { Subject = ".fire" });
        var subject = $"{root}.fire";

        ScheduleResult scheduled;
        ActivityContext rootContext;
        using (var rootActivity = ActivityCapture.TestSource.StartActivity("request"))
        {
            rootContext = rootActivity!.Context;
            scheduled = await scenario.Publisher.ScheduleAsync(subject, new OrderEvent("ORD-1", 1m),
                DateTimeOffset.UtcNow.AddMilliseconds(500), new ScheduleOptions { Key = "k1" });
        }

        var schedule = await activities.WaitForAsync(a => a.OperationName == $"schedule {subject}",
            TimeSpan.FromSeconds(5), "for the schedule");
        var process = await activities.WaitForAsync(a => a.OperationName == $"process {subject}",
            TimeSpan.FromSeconds(10), "for the handler");

        Assert.AreEqual(ActivityKind.Producer, schedule.Kind);
        Assert.AreEqual(scheduled.ScheduleId, schedule.Tag("messaging.nats.schedule.id"));
        Assert.AreEqual("k1", schedule.Tag("messaging.nats.schedule.key"));
        Assert.AreEqual("Replace", schedule.Tag("messaging.nats.schedule.mode"));
        Assert.IsNotNull(schedule.Tag("messaging.nats.schedule.deliver_at"));
        Assert.AreEqual(rootContext.TraceId, process.TraceId);
        Assert.AreEqual(schedule.SpanId, process.ParentSpanId);
        Assert.AreEqual(scheduled.ScheduleId, process.Tag("messaging.nats.schedule.id"));
        Assert.AreEqual(scheduled.ScheduleId, process.Tag("messaging.message.id"));
    }

    [TestMethod]
    public async Task ForwardingContextHeaders_FromAHandler_ParentsTheNextHandlerOnTheForwardingPublish()
    {
        using var activities = new ActivityCapture();
        IPublisher? publisher = null;
        var probe = new Probe<MessageContext<OrderEvent>>
        {
            Behaviour = (context, _, _) => context.Subject.EndsWith(".created", StringComparison.Ordinal)
                ? publisher!.PublishAsync(context.Subject[..^".created".Length] + ".forwarded", context.Message,
                    new PublishOptions { Headers = context.Headers })
                : Task.CompletedTask
        };
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>([probe], null,
            new NatsConsumerAttribute { Subject = ".*" });
        publisher = scenario.Publisher;

        await scenario.Publisher.PublishAsync($"{scenario.Root}.created", new OrderEvent("ORD-1", 1m));

        var first = await activities.WaitForAsync(a => a.OperationName == $"process {scenario.Root}.*" &&
                                                       a.Tag("messaging.destination.name") == $"{scenario.Root}.created",
            TimeSpan.FromSeconds(10), "for the first handler");
        var forward = await activities.WaitForAsync(a => a.OperationName == $"publish {scenario.Root}.forwarded",
            TimeSpan.FromSeconds(10), "for the forwarding publish");
        var next = await activities.WaitForAsync(a => a.OperationName == $"process {scenario.Root}.*" &&
                                                      a.Tag("messaging.destination.name") == $"{scenario.Root}.forwarded",
            TimeSpan.FromSeconds(10), "for the next handler");

        Assert.AreEqual(first.SpanId, forward.ParentSpanId, "The forwarding publish runs in the handler's span");
        Assert.AreEqual(forward.SpanId, next.ParentSpanId,
            "The forwarded message carries the forwarding publish's trace context, not the first producer's");
        Assert.AreEqual(first.TraceId, next.TraceId);
        var forwarded = probe.Calls.Single(c => c.Arg.Subject.EndsWith(".forwarded", StringComparison.Ordinal)).Arg;
        Assert.AreEqual(forward.Id, forwarded.Headers["traceparent"].ToString());
    }

    [TestMethod]
    public async Task OutcomeAttribute_IsAckRetryOrDeadLetter()
    {
        using var activities = new ActivityCapture();
        var probe = new Probe<MessageContext<OrderEvent>>
        {
            Behaviour = (context, _, _) => context.Message.OrderId == "ORD-FAIL"
                ? throw new InvalidOperationException("broken")
                : Task.CompletedTask
        };
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>([probe], null,
            new NatsConsumerAttribute { Subject = ".created", MaxDeliveryAttempts = 2, RetryDelaysSeconds = [0] });
        var subject = $"{scenario.Root}.created";

        var ok = await scenario.Publisher.PublishAsync(subject, new OrderEvent("ORD-OK", 1m));
        var failing = await scenario.Publisher.PublishAsync(subject, new OrderEvent("ORD-FAIL", 1m));

        var acked = await activities.WaitForAsync(a => IsProcess(a, scenario, ok.MessageId, "1"),
            TimeSpan.FromSeconds(10), "for the successful delivery");
        var retried = await activities.WaitForAsync(a => IsProcess(a, scenario, failing.MessageId, "1"),
            TimeSpan.FromSeconds(10), "for the first failure");
        var deadLettered = await activities.WaitForAsync(a => IsProcess(a, scenario, failing.MessageId, "2"),
            TimeSpan.FromSeconds(10), "for the last failure");

        Assert.AreEqual("ack", acked.Tag(Outcome));
        Assert.IsNull(acked.Tag("error.type"));

        Assert.AreEqual("retry", retried.Tag(Outcome));
        Assert.AreEqual(typeof(InvalidOperationException).FullName, retried.Tag("error.type"));
        Assert.AreEqual(ActivityStatusCode.Error, retried.Status);
        Assert.IsTrue(retried.Events.Any(e => e.Name == "exception"), "The exception is recorded as an event");

        Assert.AreEqual("dead_letter", deadLettered.Tag(Outcome));
        Assert.AreEqual(typeof(InvalidOperationException).FullName, deadLettered.Tag("error.type"));
        Assert.AreEqual(ActivityStatusCode.Error, deadLettered.Status);
    }

    [TestMethod]
    public async Task Metrics_RecordTheProcessDuration_AndThePublishDurationWithItsOutcome()
    {
        using var metrics = new MetricCapture();
        var probe = new Probe<MessageContext<OrderEvent>>();
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>([probe], null,
            new NatsConsumerAttribute { Subject = ".created" });
        var subject = $"{scenario.Root}.created";
        var missing = $"{TestNames.Unique("nostream")}.created";

        var options = new PublishOptions { MessageId = TestNames.Unique("id") };
        await scenario.Publisher.PublishAsync(subject, new OrderEvent("ORD-1", 1m), options);
        await scenario.Publisher.PublishAsync(subject, new OrderEvent("ORD-1", 1m), options);
        await Assert.ThrowsExactlyAsync<NatsPublishException>(() => scenario.Publisher.PublishAsync(missing,
            new OrderEvent("ORD-2", 1m), new PublishOptions { Timeout = TimeSpan.FromSeconds(1) }));
        await probe.WaitForCallsAsync(1);

        var publishes = metrics.Of("messaging.client.operation.duration", "messaging.destination.name", subject);
        CollectionAssert.AreEquivalent(new[] { "ok", "duplicate" }, publishes.Select(m => m.Tag(Outcome)).ToArray());
        Assert.IsTrue(publishes.All(m => m.Tag("messaging.operation.name") == "publish" && m.Value > 0));
        var noStream = metrics.Of("messaging.client.operation.duration", "messaging.destination.name", missing).Single();
        Assert.AreEqual("no_stream", noStream.Tag(Outcome));
        Assert.AreEqual(typeof(NatsPublishException).FullName, noStream.Tag("error.type"));

        // Recorded last, after the duration and the in-flight decrement.
        var consumed = await metrics.WaitForAsync("messaging.client.consumed.messages",
            m => m.Tag(ConsumerGroup) == scenario.Durable, TimeSpan.FromSeconds(5));
        Assert.AreEqual(1d, consumed.Single().Value);
        var processed = metrics.Of("messaging.process.duration", ConsumerGroup, scenario.Durable).Single();
        Assert.AreEqual("ack", processed.Tag(Outcome));
        Assert.IsGreaterThan(0d, processed.Value);
        CollectionAssert.AreEqual(new[] { 1d, -1d },
            metrics.Of("messaging.nats.inflight", ConsumerGroup, scenario.Durable).Select(m => m.Value).ToArray(),
            "In-flight goes up and back down");
    }

    private static bool IsProcess(Activity activity, ConsumerScenario scenario, string? messageId, string delivery) =>
        activity.OperationName.StartsWith("process ", StringComparison.Ordinal) &&
        activity.Tag(ConsumerGroup) == scenario.Durable &&
        activity.Tag("messaging.message.id") == messageId &&
        activity.Tag("messaging.nats.delivery_count") == delivery;
}
