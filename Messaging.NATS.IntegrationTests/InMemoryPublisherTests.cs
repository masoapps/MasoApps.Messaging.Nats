using Messaging.NATS.Testing;
using NATS.Client.Core;

namespace Messaging.NATS.IntegrationTests;

[TestClass]
public class InMemoryPublisherTests
{
    [TestMethod]
    public async Task LegacyCalls_AreRecorded_AndAPositiveDelayIsAPendingSchedule()
    {
        var publisher = new InMemoryPublisher();
        object message = new OrderEvent("ORD", 1m);

        await publisher.PublishAsync("orders.created", message, (TimeSpan?)null);
        await publisher.PublishAsync("orders.created", message, TimeSpan.Zero);
        await publisher.PublishAsync("orders.remind", message, TimeSpan.FromMinutes(5));
        await publisher.PublishAsync("orders.remind", message, TimeSpan.FromMinutes(5));

        Assert.HasCount(4, publisher.Published);
        Assert.IsTrue(publisher.Published.All(p => p.MessageType == typeof(OrderEvent)), "Runtime type, as the real publisher");
        Assert.IsFalse(publisher.Published[0].IsScheduled);
        Assert.AreEqual(TimeSpan.Zero, publisher.Published[1].DeferredBy);

        Assert.HasCount(2, publisher.PendingSchedules, "Unkeyed: both stay pending");
        Assert.IsTrue(publisher.PendingSchedules.All(p => p.DeferredBy == TimeSpan.FromMinutes(5) && p.ScheduleId != null));
        Assert.AreNotEqual(publisher.PendingSchedules[0].ScheduleKey, publisher.PendingSchedules[1].ScheduleKey);
    }

    [TestMethod]
    public async Task TypedPublishes_AreRecordedWithOptions_AndMessagesOfFilters()
    {
        var publisher = new InMemoryPublisher();
        var options = new PublishOptions { CorrelationId = "c", Headers = new NatsHeaders { ["X-A"] = "1" } };

        var first = await publisher.PublishAsync("orders.created", new OrderEvent("A", 1m), options);
        await publisher.PublishAsync("orders.updated", new OrderEvent("B", 2m));
        await publisher.PublishAsync("other", new TestMessage("x"), CancellationToken.None);

        Assert.AreEqual(options, publisher.Published[0].Options);
        Assert.AreEqual(typeof(OrderEvent), publisher.Published[0].MessageType);
        Assert.IsNotNull(first.MessageId);
        CollectionAssert.AreEqual(new[] { "A", "B" }, publisher.MessagesOf<OrderEvent>().Select(o => o.OrderId).ToArray());
        CollectionAssert.AreEqual(new[] { "B" }, publisher.MessagesOf<OrderEvent>("orders.updated").Select(o => o.OrderId).ToArray());
        Assert.AreEqual(1, publisher.MessagesOf<TestMessage>().Count());
    }

    [TestMethod]
    public async Task SameMessageId_IsADuplicate_AndRetryAttemptsZeroHasNoId()
    {
        var publisher = new InMemoryPublisher();

        var first = await publisher.PublishAsync("s", new TestMessage("x"), new PublishOptions { MessageId = "id" });
        var second = await publisher.PublishAsync("s", new TestMessage("x"), new PublishOptions { MessageId = "id" });
        var none = await publisher.PublishAsync("s", new TestMessage("x"), new PublishOptions { RetryAttempts = 0 });
        var fireAndForget = await publisher.PublishAsync("s", new TestMessage("x"), new PublishOptions { FireAndForget = true });

        Assert.IsFalse(first.Duplicate);
        Assert.IsTrue(second.Duplicate);
        Assert.AreEqual(first.Sequence, second.Sequence);
        Assert.IsNull(none.MessageId);
        Assert.AreEqual(new PublishResult(string.Empty, 0, false, null), fireAndForget);
        Assert.HasCount(4, publisher.Published, "Every call is recorded");
    }

    [TestMethod]
    public async Task Replace_KeepsOnlyTheLastPendingSchedule()
    {
        var publisher = new InMemoryPublisher();
        var key = new ScheduleOptions { Key = "journey" };
        var at = DateTimeOffset.UtcNow.AddMinutes(1);

        var first = await publisher.ScheduleAsync("reschedule", new TestMessage("v1"), at, key);
        var second = await publisher.ScheduleAsync("reschedule", new TestMessage("v2"), at.AddMinutes(1), key);

        Assert.IsTrue(first.Created && second.Created);
        Assert.AreNotEqual(first.ScheduleId, second.ScheduleId);
        var pending = publisher.PendingSchedules.Single();
        Assert.AreEqual(second.ScheduleId, pending.ScheduleId);
        Assert.AreEqual(at.AddMinutes(1), pending.DeliverAt);
        Assert.AreEqual(ScheduleMode.Replace, pending.Mode);
        Assert.AreEqual(key, pending.ScheduleOptions);
        Assert.HasCount(2, publisher.Published);
    }

    [TestMethod]
    public async Task CreateIfAbsent_KeepsTheFirst_AndReturnsItsScheduleId()
    {
        var publisher = new InMemoryPublisher();
        var options = new ScheduleOptions { Key = "session", Mode = ScheduleMode.CreateIfAbsent };
        var at = DateTimeOffset.UtcNow.AddMinutes(1);

        var first = await publisher.ScheduleAsync("session.arriving", new TestMessage("first"), at, options);
        var second = await publisher.ScheduleAsync("session.arriving", new TestMessage("second"), at, options);

        Assert.IsTrue(first.Created);
        Assert.IsFalse(second.Created);
        Assert.AreEqual(first.ScheduleId, second.ScheduleId);
        Assert.AreEqual(first.Sequence, second.Sequence);
        Assert.AreEqual("first", ((TestMessage)publisher.PendingSchedules.Single().Message).Content);

        Assert.IsTrue(await publisher.CancelScheduleAsync("session.arriving", "session"));
        var third = await publisher.ScheduleAsync("session.arriving", new TestMessage("third"), at, options);
        Assert.IsTrue(third.Created, "The key is free again after a cancel");
    }

    [TestMethod]
    public async Task Cancel_ReturnsTrueThenFalse()
    {
        var publisher = new InMemoryPublisher();
        await publisher.ScheduleAsync("push.send", new TestMessage("x"), DateTimeOffset.UtcNow.AddMinutes(1),
            new ScheduleOptions { Key = "n1" });
        await publisher.ScheduleAsync("push.send", new TestMessage("y"), DateTimeOffset.UtcNow.AddMinutes(1),
            new ScheduleOptions { Key = "n2" });

        Assert.IsTrue(await publisher.CancelScheduleAsync("push.send", "n1"));
        Assert.IsFalse(await publisher.CancelScheduleAsync("push.send", "n1"));
        Assert.AreEqual("n2", publisher.PendingSchedules.Single().ScheduleKey);
    }

    [TestMethod]
    public async Task InvalidKeyAndSubject_Throw_LikeTheRealPublisher()
    {
        var publisher = new InMemoryPublisher();
        var at = DateTimeOffset.UtcNow.AddMinutes(1);

        await Assert.ThrowsExactlyAsync<ArgumentException>(() =>
            publisher.ScheduleAsync("s", new TestMessage("x"), at, new ScheduleOptions { Key = "a.b" }));
        await Assert.ThrowsExactlyAsync<ArgumentException>(() => publisher.ScheduleAsync("s.*", new TestMessage("x"), at));
        await Assert.ThrowsExactlyAsync<ArgumentException>(() => publisher.CancelScheduleAsync("s", "bad key"));
        await Assert.ThrowsExactlyAsync<ArgumentNullException>(() => publisher.PublishAsync("s", null!, (TimeSpan?)null));
    }

    [TestMethod]
    public async Task RequestAsync_UsesTheRegisteredResponder()
    {
        var publisher = new InMemoryPublisher();
        publisher.OnRequest<PingRequest, PingResponse>("ping", r => new PingResponse(r.Payload, "fake"));

        var response = await publisher.RequestAsync<PingRequest, PingResponse>("ping", new PingRequest("hi"));

        Assert.AreEqual(new PingResponse("hi", "fake"), response);
        await Assert.ThrowsExactlyAsync<NatsNoRespondersException>(() =>
            publisher.RequestAsync<PingRequest, PingResponse>("nobody", new PingRequest("hi")));
    }

    [TestMethod]
    public async Task Clear_ForgetsCallsAndPendingSchedules()
    {
        var publisher = new InMemoryPublisher();
        await publisher.PublishAsync("s", new TestMessage("x"), TimeSpan.FromMinutes(1));
        await publisher.PublishAsync("s", new TestMessage("x"));

        publisher.Clear();

        Assert.HasCount(0, publisher.Published);
        Assert.HasCount(0, publisher.PendingSchedules);
    }

    [TestMethod]
    public async Task Cancellation_IsHonoured()
    {
        var publisher = new InMemoryPublisher();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAsync<OperationCanceledException>(() => publisher.PublishAsync("s", new TestMessage("x"), cts.Token));
        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            publisher.ScheduleAsync("s", new TestMessage("x"), DateTimeOffset.UtcNow, null, cts.Token));
        Assert.HasCount(0, publisher.Published);
    }

    [TestMethod]
    public async Task ValidationAndCancellationErrors_SurfaceOnTheReturnedTask_LikeTheRealPublisher()
    {
        var publisher = new InMemoryPublisher();
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();
        var at = DateTimeOffset.UtcNow.AddMinutes(1);
        var calls = new (string Name, Func<Task> Call)[]
        {
            ("legacy empty subject", () => publisher.PublishAsync("", (object)new TestMessage("x"), (TimeSpan?)null)),
            ("legacy wildcard delayed", () => publisher.PublishAsync("s.*", (object)new TestMessage("x"), TimeSpan.FromMinutes(1))),
            ("typed empty subject", () => publisher.PublishAsync("", new TestMessage("x"))),
            ("typed null message", () => publisher.PublishAsync<TestMessage>("s", null!)),
            ("typed cancelled", () => publisher.PublishAsync("s", new TestMessage("x"), cancelled.Token)),
            ("schedule wildcard", () => publisher.ScheduleAsync("s.*", new TestMessage("x"), at)),
            ("schedule bad key", () => publisher.ScheduleAsync("s", new TestMessage("x"), at, new ScheduleOptions { Key = "a.b" })),
            ("schedule cancelled", () => publisher.ScheduleAsync("s", new TestMessage("x"), at, null, cancelled.Token)),
            ("cancel bad key", () => publisher.CancelScheduleAsync("s", "bad key")),
            ("cancel cancelled", () => publisher.CancelScheduleAsync("s", "k", cancelled.Token)),
            ("request no responders", () => publisher.RequestAsync<PingRequest, PingResponse>("nobody", new PingRequest("hi"))),
            ("request cancelled", () => publisher.RequestAsync<PingRequest, PingResponse>("nobody", new PingRequest("hi"),
                cancellationToken: cancelled.Token))
        };

        foreach (var (name, call) in calls)
        {
            Task task;
            try
            {
                task = call();
            }
            catch (Exception e)
            {
                Assert.Fail($"{name}: threw {e.GetType().Name} from the call instead of returning a faulted task");
                return;
            }

            Assert.IsTrue(task.IsFaulted || task.IsCanceled, $"{name}: the task is {task.Status}");
            await Assert.ThrowsAsync<Exception>(() => task, name);
        }

        Assert.IsTrue(publisher.ScheduleAsync("s", new TestMessage("x"), at, null, cancelled.Token).IsCanceled,
            "A cancellation is a cancelled task, as from an async method");
        Assert.HasCount(0, publisher.Published);
    }

    [TestMethod]
    public async Task FireAndForget_WithAMessageId_IsDeduplicated_LikeTheStream()
    {
        var publisher = new InMemoryPublisher();
        var options = new PublishOptions { FireAndForget = true, MessageId = "id" };

        var fireAndForget = await publisher.PublishAsync("s", new TestMessage("x"), options);
        await publisher.PublishAsync("s", new TestMessage("x"), options);
        var acked = await publisher.PublishAsync("s", new TestMessage("x"), new PublishOptions { MessageId = "id" });
        var other = await publisher.PublishAsync("s", new TestMessage("x"), new PublishOptions { MessageId = "other" });

        Assert.AreEqual(new PublishResult(string.Empty, 0, false, "id"), fireAndForget);
        Assert.IsTrue(acked.Duplicate, "The stream stored the fire-and-forget message with its id");
        Assert.AreEqual(1UL, acked.Sequence);
        Assert.IsFalse(other.Duplicate);
        Assert.AreEqual(2UL, other.Sequence, "The duplicate fire-and-forget publish was not stored");
    }

    [TestMethod]
    public async Task FailWhen_MakesMatchingCallsFail_WithoutRecordingThem()
    {
        var publisher = new InMemoryPublisher();
        var noStream = new NatsPublishException(PublishFailure.NoStream, "orders.created", null, null, "No stream");
        var noSchedules = new NatsMessagingConfigurationException("AllowMsgSchedules is off");
        publisher.FailOn("orders.created", noStream);
        publisher.FailWhen(p => p.IsScheduled ? noSchedules : null);

        var publish = await Assert.ThrowsExactlyAsync<NatsPublishException>(() =>
            publisher.PublishAsync("orders.created", new OrderEvent("A", 1m)));
        Assert.AreEqual(PublishFailure.NoStream, publish.Failure);
        await Assert.ThrowsExactlyAsync<NatsPublishException>(() =>
            publisher.PublishAsync("orders.created", (object)new OrderEvent("A", 1m), (TimeSpan?)null));
        await Assert.ThrowsExactlyAsync<NatsMessagingConfigurationException>(() =>
            publisher.ScheduleAsync("orders.remind", new OrderEvent("A", 1m), DateTimeOffset.UtcNow.AddMinutes(1)));
        await Assert.ThrowsExactlyAsync<NatsMessagingConfigurationException>(() =>
            publisher.PublishAsync("orders.remind", (object)new OrderEvent("A", 1m), TimeSpan.FromMinutes(1)));

        Assert.HasCount(0, publisher.Published);
        Assert.HasCount(0, publisher.PendingSchedules);
        await publisher.PublishAsync("orders.updated", new OrderEvent("B", 2m));
        Assert.HasCount(1, publisher.Published, "Other calls still work");

        publisher.ClearFailures();
        await publisher.PublishAsync("orders.created", new OrderEvent("A", 1m));
        Assert.HasCount(2, publisher.Published);
    }

    [TestMethod]
    public async Task ConfiguredRetryAttemptsZero_GeneratesNoMessageId_LikeTheRealPublisher()
    {
        var publisher = new InMemoryPublisher(new NatsMessagingOptions { PublishRetryAttempts = 0 });

        var first = await publisher.PublishAsync("orders.created", new OrderEvent("A", 1m));
        var second = await publisher.PublishAsync("orders.created", new OrderEvent("A", 1m));

        Assert.IsNull(first.MessageId);
        Assert.IsFalse(second.Duplicate);
        Assert.IsNotNull((await publisher.PublishAsync("orders.created", new OrderEvent("A", 1m),
            new PublishOptions { RetryAttempts = 1 })).MessageId, "A per-call RetryAttempts still wins");
    }

    [TestMethod]
    public async Task CancelsAndRequests_AreRecorded_AndCancelsFailByRule()
    {
        var publisher = new InMemoryPublisher();
        publisher.OnRequest<PingRequest, PingResponse>("ping", r => new PingResponse(r.Payload, "fake"));

        Assert.IsFalse(await publisher.CancelScheduleAsync("push.send", "n1"), "Nothing was pending");
        await publisher.RequestAsync<PingRequest, PingResponse>("ping", new PingRequest("hi"), TimeSpan.FromSeconds(2));
        await Assert.ThrowsExactlyAsync<NatsNoRespondersException>(() =>
            publisher.RequestAsync<PingRequest, PingResponse>("nobody", new PingRequest("hi")));

        Assert.AreEqual(new CancelledSchedule("push.send", "n1", false), publisher.Cancellations.Single());
        Assert.HasCount(2, publisher.Requests);
        Assert.AreEqual(new PingRequest("hi"), publisher.Requests[0].Request);
        Assert.AreEqual(TimeSpan.FromSeconds(2), publisher.Requests[0].Timeout);
        Assert.AreEqual("nobody", publisher.Requests[1].Subject);
        Assert.HasCount(0, publisher.Published, "Published holds publishes and schedules only");

        var unavailable = new NatsPublishException(PublishFailure.Timeout, "push.send", null, null, "No answer");
        publisher.FailCancelWhen(c => c.Key == "n2" ? unavailable : null);
        await publisher.ScheduleAsync("push.send", new TestMessage("x"), DateTimeOffset.UtcNow.AddMinutes(1),
            new ScheduleOptions { Key = "n2" });
        Assert.AreSame(unavailable, await Assert.ThrowsExactlyAsync<NatsPublishException>(() =>
            publisher.CancelScheduleAsync("push.send", "n2")));
        Assert.HasCount(1, publisher.PendingSchedules, "A failed cancel removes nothing");
        Assert.HasCount(1, publisher.Cancellations, "A failed cancel is not recorded");

        publisher.ClearFailures();
        publisher.FailOn("push.send", unavailable);
        await Assert.ThrowsExactlyAsync<NatsPublishException>(() => publisher.CancelScheduleAsync("push.send", "n2"));
        publisher.ClearFailures();
        Assert.IsTrue(await publisher.CancelScheduleAsync("push.send", "n2"));
        Assert.AreEqual(new CancelledSchedule("push.send", "n2", true), publisher.Cancellations[^1]);
    }

    [TestMethod]
    public async Task AScheduleWhoseTimeHasPassed_Fires_SoCreateIfAbsentCreatesAgainAndCancelFindsNothing()
    {
        var time = new ManualTime(DateTimeOffset.UtcNow);
        var publisher = new InMemoryPublisher { Time = time };
        var options = new ScheduleOptions { Key = "session-1", Mode = ScheduleMode.CreateIfAbsent };

        var first = await publisher.ScheduleAsync("session.arriving", new TestMessage("first"),
            time.Now.AddSeconds(90), options);
        var loser = await publisher.ScheduleAsync("session.arriving", new TestMessage("second"),
            time.Now.AddSeconds(90), options);
        Assert.IsFalse(loser.Created);

        time.Now = time.Now.AddSeconds(91);

        Assert.HasCount(0, publisher.PendingSchedules);
        Assert.AreEqual(first.ScheduleId, publisher.FiredSchedules.Single().ScheduleId);
        Assert.IsFalse(await publisher.CancelScheduleAsync("session.arriving", "session-1"), "It already fired");
        var next = await publisher.ScheduleAsync("session.arriving", new TestMessage("next session"),
            time.Now.AddSeconds(90), options);
        Assert.IsTrue(next.Created, "The fire freed the key");
        Assert.AreNotEqual(first.ScheduleId, next.ScheduleId);

        var past = await publisher.ScheduleAsync("session.late", new TestMessage("late"), time.Now.AddSeconds(-1));
        Assert.IsTrue(publisher.FiredSchedules.Any(f => f.ScheduleId == past.ScheduleId), "A past time fires at once");
    }

    [TestMethod]
    public async Task Fire_FiresAPendingScheduleBeforeItsTime()
    {
        var publisher = new InMemoryPublisher();
        var scheduled = await publisher.ScheduleAsync("push.send", new TestMessage("x"),
            DateTimeOffset.UtcNow.AddMinutes(5), new ScheduleOptions { Key = "n1" });

        Assert.IsTrue(publisher.Fire(scheduled.ScheduleId));
        Assert.IsFalse(publisher.Fire(scheduled.ScheduleId), "Already fired");

        Assert.HasCount(0, publisher.PendingSchedules);
        Assert.AreEqual(scheduled.ScheduleId, publisher.FiredSchedules.Single().ScheduleId);
        Assert.IsFalse(await publisher.CancelScheduleAsync("push.send", "n1"));
    }

    [TestMethod]
    public async Task ARetryWithTheSameScheduleId_IsADuplicate_PendingOrFired()
    {
        var publisher = new InMemoryPublisher();
        var options = new ScheduleOptions { ScheduleId = "reminder-1" };

        var first = await publisher.ScheduleAsync("app.remind", new TestMessage("x"), DateTimeOffset.UtcNow.AddMinutes(5), options);
        var retry = await publisher.ScheduleAsync("app.remind", new TestMessage("x"), DateTimeOffset.UtcNow.AddMinutes(5), options);
        publisher.Fire("reminder-1");
        var afterFire = await publisher.ScheduleAsync("app.remind", new TestMessage("x"), DateTimeOffset.UtcNow.AddMinutes(5), options);

        Assert.AreEqual("reminder-1", first.ScheduleId);
        Assert.AreEqual(first.Sequence, retry.Sequence);
        Assert.AreEqual(first.Sequence, afterFire.Sequence);
        Assert.HasCount(0, publisher.PendingSchedules, "No second schedule is stored");
        Assert.HasCount(1, publisher.FiredSchedules);
        Assert.HasCount(3, publisher.Published);
    }

    private sealed class ManualTime(DateTimeOffset now) : TimeProvider
    {
        public DateTimeOffset Now { get; set; } = now;

        public override DateTimeOffset GetUtcNow() => Now;
    }
}
