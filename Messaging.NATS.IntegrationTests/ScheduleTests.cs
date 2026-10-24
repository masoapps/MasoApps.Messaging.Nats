using System.Diagnostics;
using System.Globalization;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Client.Serializers.Json;

namespace Messaging.NATS.IntegrationTests;

[TestClass]
public class ScheduleTests : NatsTestBase
{
    // Compared with the server's store time (the fire time). NATS.Net 2.7.3 truncates that timestamp to whole
    // milliseconds, so a fire can look up to 1 ms early; the server itself fires only once now >= @at.
    private static readonly TimeSpan EarlyTolerance = TimeSpan.FromMilliseconds(1);

    public TestContext TestContext { get; set; } = null!;

    [TestMethod]
    public async Task ThreeUnkeyedSchedules_OnOneSubject_AllDeliver()
    {
        await using var f = await ScheduleFixture.CreateAsync();
        var subject = $"{f.Root}.remind";

        // The B1 regression: 5.x used one holder subject per target, so each schedule replaced the previous one.
        await f.Publisher.PublishAsync(subject, new TestMessage("one"), TimeSpan.FromSeconds(1));
        await f.Publisher.PublishAsync(subject, new TestMessage("two"), TimeSpan.FromSeconds(1));
        var third = await f.Publisher.ScheduleAsync(subject, new TestMessage("three"), DateTimeOffset.UtcNow.AddSeconds(1));

        var received = await f.Collector.WaitForAsync(3, TimeSpan.FromSeconds(10));

        CollectionAssert.AreEquivalent(new[] { "one", "two", "three" }, received.Select(r => r.Json<TestMessage>().Content).ToArray());
        Assert.AreEqual(3, received.Select(r => r.Header("X-Schedule-Id")).Distinct().Count());
        Assert.IsTrue(received.Any(r => r.Header("X-Schedule-Id") == third.ScheduleId));
        Assert.IsTrue(third.Created);
        Assert.AreEqual(32, third.Key.Length, "Unkeyed schedules get a unique key");
    }

    [TestMethod]
    public async Task WildcardConsumer_ReceivesFiredMessagesOnly_NeverHolders()
    {
        await using var f = await ScheduleFixture.CreateAsync();

        await f.Publisher.ScheduleAsync($"{f.Root}.a", new TestMessage("a"), DateTimeOffset.UtcNow.AddSeconds(1),
            new ScheduleOptions { Key = "k1" });
        await f.Publisher.ScheduleAsync($"{f.Root}.b.c", new TestMessage("b"), DateTimeOffset.UtcNow.AddSeconds(1));
        await f.Publisher.PublishAsync($"{f.Root}.now", new TestMessage("now"));

        var holdersPending = await HolderCountAsync(f);
        Assert.AreEqual(2L, holdersPending);

        await f.Collector.WaitForAsync(3, TimeSpan.FromSeconds(10));
        await Task.Delay(500);

        var received = f.Collector.Received;
        Assert.HasCount(3, received);
        Assert.IsTrue(received.All(r => !r.Subject.StartsWith("_sched.", StringComparison.Ordinal)));
        Assert.IsTrue(received.All(r => r.Header("Nats-Schedule") == null), "A holder was delivered");
        Assert.AreEqual(0L, await HolderCountAsync(f), "Fired holders are removed");
    }

    [TestMethod]
    public async Task Schedules_NeverFireEarly()
    {
        await using var f = await ScheduleFixture.CreateAsync();
        var now = DateTimeOffset.UtcNow;

        var results = new List<ScheduleResult>();
        for (var i = 0; i < 5; i++)
        {
            // Fractional seconds: 4.x/5.x truncated "@at" to whole seconds and fired up to 1 s early.
            var deliverAt = now + TimeSpan.FromMilliseconds(900 + i * 237);
            results.Add(await f.Publisher.ScheduleAsync($"{f.Root}.timer", new TestMessage($"t{i}"), deliverAt));
        }

        var received = await f.Collector.WaitForAsync(5, TimeSpan.FromSeconds(10));

        foreach (var result in results)
        {
            var message = received.Single(r => r.Header("X-Schedule-Id") == result.ScheduleId);
            Assert.IsTrue(message.Stored >= result.DeliverAt - EarlyTolerance,
                $"Fired {(result.DeliverAt - message.Stored).TotalMilliseconds:F1} ms early");
            TestContext.WriteLine($"fired {(message.Stored - result.DeliverAt).TotalMilliseconds:F1} ms after deliverAt");
        }
    }

    [TestMethod]
    public async Task Replace_OnlyTheLastFires_AtItsOwnTime()
    {
        await using var f = await ScheduleFixture.CreateAsync();
        var subject = $"{f.Root}.reschedule";
        var key = new ScheduleOptions { Key = "journey-1" };
        var now = DateTimeOffset.UtcNow;

        var first = await f.Publisher.ScheduleAsync(subject, new TestMessage("v1"), now.AddSeconds(1), key);
        var second = await f.Publisher.ScheduleAsync(subject, new TestMessage("v2"), now.AddSeconds(1.2), key);
        var third = await f.Publisher.ScheduleAsync(subject, new TestMessage("v3"), now.AddSeconds(2.2), key);

        Assert.IsTrue(first.Created && second.Created && third.Created);
        Assert.AreEqual(3, new[] { first.ScheduleId, second.ScheduleId, third.ScheduleId }.Distinct().Count());
        Assert.AreEqual(1L, await HolderCountAsync(f), "One pending holder per key");

        var received = await f.Collector.WaitForAsync(1, TimeSpan.FromSeconds(10));
        await Task.Delay(1500);
        received = f.Collector.Received;

        Assert.HasCount(1, received);
        Assert.AreEqual("v3", received[0].Json<TestMessage>().Content);
        Assert.AreEqual(third.ScheduleId, received[0].Header("X-Schedule-Id"));
        Assert.AreEqual("journey-1", received[0].Header("X-Schedule-Key"));
        Assert.IsTrue(received[0].Stored >= third.DeliverAt - EarlyTolerance);
    }

    [TestMethod]
    public async Task CreateIfAbsent_FirstWins_AndTheLoserGetsItsScheduleId()
    {
        await using var f = await ScheduleFixture.CreateAsync(declare: true);
        var subject = $"{f.Root}.arriving";
        var options = new ScheduleOptions { Key = "session-1", Mode = ScheduleMode.CreateIfAbsent };

        var first = await f.Publisher.ScheduleAsync(subject, new TestMessage("first"), DateTimeOffset.UtcNow.AddSeconds(1.5), options);
        var second = await f.Publisher.ScheduleAsync(subject, new TestMessage("second"), DateTimeOffset.UtcNow.AddSeconds(1), options);

        Assert.IsTrue(first.Created);
        Assert.IsFalse(second.Created);
        Assert.AreEqual(first.ScheduleId, second.ScheduleId);
        Assert.AreEqual(first.Sequence, second.Sequence);

        var received = await f.Collector.WaitForAsync(1, TimeSpan.FromSeconds(10));
        Assert.AreEqual("first", received[0].Json<TestMessage>().Content);
        Assert.AreEqual(first.ScheduleId, received[0].Header("X-Schedule-Id"));

        // The fire removed the holder, so the key is free again.
        await Task.Delay(300);
        var third = await f.Publisher.ScheduleAsync(subject, new TestMessage("third"), DateTimeOffset.UtcNow.AddMilliseconds(500), options);
        Assert.IsTrue(third.Created);

        received = await f.Collector.WaitForAsync(2, TimeSpan.FromSeconds(10));
        Assert.HasCount(2, received);
        Assert.AreEqual("third", received[1].Json<TestMessage>().Content);
    }

    [TestMethod]
    public async Task CreateIfAbsent_RetryAfterALostPubAck_FindsItsOwnStoredAttempt_AndCountsAsCreated()
    {
        await using var f = await ScheduleFixture.CreateAsync(options: o => o.PublisherTestHooks.DropPubAck =
            (subject, attempt) => subject.StartsWith("_sched.", StringComparison.Ordinal) && attempt == 1);
        var subject = $"{f.Root}.arriving";
        var options = new ScheduleOptions { Key = "session-1", Mode = ScheduleMode.CreateIfAbsent };

        // The first attempt is stored but its PubAck is dropped; the retry gets 10071 for its own holder.
        var created = await f.Publisher.ScheduleAsync(subject, new TestMessage("first"), DateTimeOffset.UtcNow.AddMinutes(1), options);

        Assert.IsTrue(created.Created, "The pending holder is this call's own");
        Assert.AreEqual(1L, await HolderCountAsync(f));
        var stored = await (await f.Js.GetStreamAsync(f.Stream)).GetAsync(new StreamMsgGetRequest
        {
            LastBySubj = ScheduleTargets.HolderFilter(f.Stream, subject, "session-1")
        });
        Assert.AreEqual(created.ScheduleId, NatsPublisher.ReadHeader(stored.Message.Hdrs, "X-Schedule-Id"));
        Assert.AreEqual(stored.Message.Seq, created.Sequence);
    }

    [TestMethod]
    public async Task CreateIfAbsent_WhenThePendingScheduleFiresBeforeTheLookup_CreatesItsOwn()
    {
        var lookups = 0;
        await using var f = await ScheduleFixture.CreateAsync(options: o => o.PublisherTestHooks.BeforePendingLookup =
            () => Interlocked.Increment(ref lookups) == 1 ? Task.Delay(TimeSpan.FromSeconds(2)) : Task.CompletedTask);
        var subject = $"{f.Root}.arriving";
        var options = new ScheduleOptions { Key = "session-1", Mode = ScheduleMode.CreateIfAbsent, Timeout = TimeSpan.FromSeconds(10) };

        var first = await f.Publisher.ScheduleAsync(subject, new TestMessage("first"), DateTimeOffset.UtcNow.AddMilliseconds(500), options);
        var second = await f.Publisher.ScheduleAsync(subject, new TestMessage("second"), DateTimeOffset.UtcNow.AddSeconds(1), options);

        Assert.IsTrue(first.Created);
        Assert.IsTrue(second.Created, "The first fired before the lookup, so the key was free");
        Assert.AreNotEqual(first.ScheduleId, second.ScheduleId);
        Assert.IsFalse(string.IsNullOrEmpty(second.ScheduleId));

        var received = await f.Collector.WaitForAsync(2, TimeSpan.FromSeconds(10));
        ScheduleRaces.AssertFromItsHolder(received, ("first", first.ScheduleId), ("second", second.ScheduleId));
    }

    [TestMethod]
    public async Task CreateIfAbsent_AStoredHolderWithAnotherId_MakesTheCallTheLoser_WithThatId()
    {
        await using var connection = CreateConnection();
        var js = CreateJetStreamContext(connection);
        var stream = TestNames.Unique("SCHED");
        var root = TestNames.Unique("sched");
        await js.CreateStreamAsync(StreamDefinition.Scheduled(stream, $"{root}.>").ToStreamConfig());

        // A stored holder that carries the schedule id a retry would use. The server checks the expected
        // subject sequence before deduplication, so the retry gets 10071, not a duplicate ack.
        var subject = $"{root}.arriving";
        var holder = ScheduleTargets.HolderSubject(stream, subject, "session-1", "stored-id");
        var holders = ScheduleTargets.HolderFilter(stream, subject, "session-1");
        var headers = new NatsHeaders
        {
            ["Nats-Schedule"] = $"@at {HeaderSanitiser.FormatTime(DateTimeOffset.UtcNow.AddMinutes(5))}",
            ["Nats-Schedule-Target"] = subject,
            ["Nats-Msg-Id"] = "stored-id",
            ["X-Schedule-Id"] = "stored-id"
        };
        var stored = await js.PublishAsync(holder, Encoding.UTF8.GetBytes("{}"), NatsRawSerializer<byte[]>.Default, headers: headers);

        var retry = new NatsHeaders
        {
            ["Nats-Schedule"] = headers["Nats-Schedule"],
            ["Nats-Schedule-Target"] = subject,
            ["Nats-Msg-Id"] = "stored-id",
            ["Nats-Expected-Last-Subject-Sequence"] = "0",
            ["Nats-Expected-Last-Subject-Sequence-Subject"] = holders
        };
        var ack = await connection.RequestAsync<byte[], PubAckResponse>(holder, Encoding.UTF8.GetBytes("{}"), retry,
            NatsRawSerializer<byte[]>.Default, NatsJSJsonSerializer<PubAckResponse>.Default);
        Assert.AreEqual(10071, ack.Data!.Error?.ErrCode);

        // What the publisher does with that 10071: the pending holder carries its own id.
        var publisher = CreatePublisher(connection, js);
        var loser = await publisher.ScheduleAsync(subject, new TestMessage("x"), DateTimeOffset.UtcNow.AddMinutes(1),
            new ScheduleOptions { Key = "session-1", Mode = ScheduleMode.CreateIfAbsent });
        Assert.IsFalse(loser.Created);
        Assert.AreEqual("stored-id", loser.ScheduleId);
        Assert.AreEqual(stored.Seq, loser.Sequence);
    }

    [TestMethod]
    public async Task Cancel_ReturnsTrueThenFalse_AndTheScheduleNeverFires()
    {
        await using var f = await ScheduleFixture.CreateAsync();
        var subject = $"{f.Root}.push";

        await f.Publisher.ScheduleAsync(subject, new TestMessage("cancelled"), DateTimeOffset.UtcNow.AddSeconds(1.5),
            new ScheduleOptions { Key = "notification-1" });

        Assert.IsTrue(await f.Publisher.CancelScheduleAsync(subject, "notification-1"));
        Assert.IsFalse(await f.Publisher.CancelScheduleAsync(subject, "notification-1"));
        Assert.IsFalse(await f.Publisher.CancelScheduleAsync(subject, "never-scheduled"));

        await Task.Delay(2500);
        Assert.HasCount(0, f.Collector.Received);
    }

    [TestMethod]
    public async Task Cancel_AfterTheFire_ReturnsFalse()
    {
        await using var f = await ScheduleFixture.CreateAsync();
        var subject = $"{f.Root}.push";

        await f.Publisher.ScheduleAsync(subject, new TestMessage("fired"), DateTimeOffset.UtcNow.AddMilliseconds(500),
            new ScheduleOptions { Key = "notification-1" });
        await f.Collector.WaitForAsync(1, TimeSpan.FromSeconds(10));
        await Task.Delay(300);

        Assert.IsFalse(await f.Publisher.CancelScheduleAsync(subject, "notification-1"));
        Assert.AreEqual(0L, await HolderCountAsync(f));
    }

    [TestMethod]
    [DataRow(-60)]
    [DataRow(-1)]
    public async Task PastDeliverAt_IsStillAKeyedSchedule_AndFiresAtOnce(int seconds)
    {
        await using var f = await ScheduleFixture.CreateAsync(configure: d =>
        {
            d.Retention = StreamConfigRetention.Limits;
            d.MaxAge = 30;
        });
        var subject = $"{f.Root}.late";
        var called = DateTimeOffset.UtcNow;

        var result = await f.Publisher.ScheduleAsync(subject, new TestMessage("late"), called.AddSeconds(seconds),
            new ScheduleOptions { Key = "k" });

        Assert.IsTrue(result.Created);
        var fired = (await f.Collector.WaitForAsync(1, TimeSpan.FromSeconds(5)))[0];
        Assert.AreEqual(result.ScheduleId, fired.Header("X-Schedule-Id"));
        Assert.IsLessThan(TimeSpan.FromSeconds(2), fired.At - called, "Fires at once");
        Assert.IsFalse(await f.Publisher.CancelScheduleAsync(subject, "k"), "Nothing is pending after the fire");
    }

    [TestMethod]
    public async Task ACachedStreamCheck_IsReadAgain_SoALoweredMaxAgeIsSeen()
    {
        await using var f = await ScheduleFixture.CreateAsync(
            options: o => o.PublisherTestHooks.ScheduleTargetRefreshInterval = TimeSpan.FromMilliseconds(200));
        var subject = $"{f.Root}.remind";
        Assert.IsTrue((await f.Publisher.ScheduleAsync(subject, new TestMessage("warm"), DateTimeOffset.UtcNow.AddSeconds(1))).Created);

        var config = (await f.Js.GetStreamAsync(f.Stream)).Info.Config;
        await f.Js.UpdateStreamAsync(config with { MaxAge = TimeSpan.FromSeconds(3), DuplicateWindow = TimeSpan.FromSeconds(1) });
        await Task.Delay(400);

        var error = await Assert.ThrowsExactlyAsync<ArgumentOutOfRangeException>(() =>
            f.Publisher.ScheduleAsync(subject, new TestMessage("too late"), DateTimeOffset.UtcNow.AddSeconds(8)));
        StringAssert.Contains(error.Message, "MaxAge 00:00:03");
    }

    [TestMethod]
    [DataRow("Interest", "Interest retention")]
    [DataRow("MaxMsgs", "MaxMsgs is 100")]
    [DataRow("MaxBytes", "MaxBytes is 1000000")]
    [DataRow("MaxMsgsPerSubject", "MaxMsgsPerSubject is 5")]
    [DataRow("DiscardNew", "Discard New")]
    [DataRow("NoHolderSubject", "schedule holder subject")]
    public async Task ServerStreamThatBreaksAScheduleRule_IsRejectedByScheduleAndByVerifyOnly(string rule, string expected)
    {
        await using var connection = CreateConnection();
        var js = CreateJetStreamContext(connection);
        var stream = TestNames.Unique("BADSCHED");
        var root = TestNames.Unique("badsched");
        var config = new StreamConfig(stream, [$"{root}.>", $"_sched.{stream}.>"]) { AllowMsgSchedules = true };
        config = rule switch
        {
            "Interest" => config with { Retention = StreamConfigRetention.Interest },
            "MaxMsgs" => config with { MaxMsgs = 100 },
            "MaxBytes" => config with { MaxBytes = 1_000_000 },
            "MaxMsgsPerSubject" => config with { MaxMsgsPerSubject = 5 },
            "DiscardNew" => config with { Discard = StreamConfigDiscard.New, MaxMsgs = -1 },
            "NoHolderSubject" => config with { Subjects = [$"{root}.>"] },
            _ => throw new ArgumentOutOfRangeException(nameof(rule))
        };
        try
        {
            await js.CreateStreamAsync(config);
        }
        catch (NatsJSApiException e) when (rule == "DiscardNew" && e.Message.Contains("discard new"))
        {
            // Newer 2.12 servers enforce this rule themselves, so the stream can never exist.
            return;
        }

        var publisher = CreatePublisher(connection, js);
        var error = await Assert.ThrowsExactlyAsync<NatsMessagingConfigurationException>(() =>
            publisher.ScheduleAsync($"{root}.timer", new TestMessage("x"), DateTimeOffset.UtcNow.AddSeconds(5)));
        StringAssert.Contains(error.Message, expected);

        await using var host = await TestHost.StartAsync(o =>
        {
            o.StreamDefinitions =
            [
                new StreamDefinition { Name = stream, Subjects = [$"{root}.>"], AllowMsgSchedules = true }
            ];
            o.StreamProvisioning = StreamProvisioning.VerifyOnly;
        });
        var faulted = await host.WaitForStateAsync($"stream:{stream}", ComponentState.Faulted);
        StringAssert.Contains(faulted.Error, expected);
    }

    [TestMethod]
    public async Task FiredMessage_CarriesScheduleIdScheduledForAndCorrelationId_NotAForwardedTraceparent()
    {
        await using var f = await ScheduleFixture.CreateAsync();
        const string traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        var deliverAt = DateTimeOffset.UtcNow.AddSeconds(1.1);

        var result = await f.Publisher.ScheduleAsync($"{f.Root}.timer", new TestMessage("x"), deliverAt, new ScheduleOptions
        {
            Key = "k-1",
            CorrelationId = "corr-7",
            Headers = new NatsHeaders { ["traceparent"] = traceparent, ["X-Tenant"] = "acme" }
        });

        var fired = (await f.Collector.WaitForAsync(1, TimeSpan.FromSeconds(10)))[0];

        Assert.AreEqual(result.ScheduleId, fired.Header("X-Schedule-Id"));
        Assert.AreEqual("k-1", fired.Header("X-Schedule-Key"));
        Assert.AreEqual(deliverAt, DateTimeOffset.Parse(fired.Header("X-Scheduled-For")!, CultureInfo.InvariantCulture));
        Assert.AreNotEqual(traceparent, fired.Header("traceparent"), "Only the schedule span's own trace context is sent");
        Assert.AreEqual("corr-7", fired.Header("X-Correlation-Id"));
        Assert.AreEqual("acme", fired.Header("X-Tenant"));
        Assert.IsNotNull(fired.Header("X-Sent-Time"));
        Assert.IsNull(fired.Header("Nats-Msg-Id"), "The scheduler strips the holder's message id");
        Assert.AreEqual(ScheduleTargets.HolderSubject(f.Stream, $"{f.Root}.timer", "k-1", result.ScheduleId),
            fired.Header("Nats-Scheduler"));
    }

    [TestMethod]
    public async Task ForwardingFiredHeaders_AfterAReplace_DoesNotPurgeTheNewHolder()
    {
        await using var f = await ScheduleFixture.CreateAsync();
        var subject = $"{f.Root}.timer";
        var key = new ScheduleOptions { Key = "k" };

        await f.Publisher.ScheduleAsync(subject, new TestMessage("v1"), DateTimeOffset.UtcNow.AddMilliseconds(300), key);
        var fired = (await f.Collector.WaitForAsync(1, TimeSpan.FromSeconds(10)))[0];
        Assert.AreEqual("purge", fired.Header("Nats-Schedule-Next"));

        var replacement = await f.Publisher.ScheduleAsync(subject, new TestMessage("v2"), DateTimeOffset.UtcNow.AddSeconds(1.5), key);

        // A handler forwards the fired message's headers. Raw, Nats-Scheduler + Nats-Schedule-Next: purge would
        // purge the new holder.
        await f.Publisher.PublishAsync($"{f.Root}.forwarded", new TestMessage("forwarded"),
            new PublishOptions { Headers = fired.Headers });

        var received = await f.Collector.WaitForAsync(3, TimeSpan.FromSeconds(10));
        var forwarded = received.Single(r => r.Subject == $"{f.Root}.forwarded");
        Assert.IsNull(forwarded.Header("Nats-Scheduler"));
        Assert.IsNull(forwarded.Header("X-Schedule-Id"), "A forwarded message does not look scheduled");

        var second = received.Last(r => r.Subject == subject);
        Assert.AreEqual("v2", second.Json<TestMessage>().Content);
        Assert.AreEqual(replacement.ScheduleId, second.Header("X-Schedule-Id"));
    }

    [TestMethod]
    public async Task Replace_RacingTheFire_EndsInADocumentedOutcome()
    {
        await using var f = await ScheduleFixture.CreateAsync();

        var outcomes = await ScheduleRaces.ReplaceAsync(f.Publisher, f.Collector, f.Root);

        TestContext.WriteLine(string.Join(Environment.NewLine, outcomes));
    }

    [TestMethod]
    public async Task Replace_AcrossTheFireInstant_NeverLosesTheNewSchedule()
    {
        await using var f = await ScheduleFixture.CreateAsync();
        const int count = 1000;
        var key = new ScheduleOptions { Key = "k" };
        var fireAt = DateTimeOffset.UtcNow.AddSeconds(4);
        await Task.WhenAll(Enumerable.Range(0, count).Select(i =>
            f.Publisher.ScheduleAsync($"{f.Root}.t{i}", new TestMessage("old"), fireAt, key)));

        // One Replace per key, spread from 250 ms before to 250 ms after the fire instant. With one holder subject
        // per key, a Replace stored while the old holder fired was purged by the fire (nats-server 2.12.1).
        Assert.AreEqual((long)count, await HolderCountAsync(f), "Every old schedule is pending before the Replaces");
        var start = fireAt.AddMilliseconds(-250);
        await ScheduleRaces.DelayUntil(start);
        var replacements = new List<Task<(ScheduleResult Result, DateTimeOffset ReturnedAt)>>();
        for (var i = 0; i < count; i++)
        {
            var sendAt = start + TimeSpan.FromTicks(TimeSpan.FromMilliseconds(500).Ticks * i / count);
            while (DateTimeOffset.UtcNow < sendAt)
            {
                Thread.SpinWait(50);
            }

            var subject = $"{f.Root}.t{i}";
            replacements.Add(Task.Run(async () =>
            {
                var result = await f.Publisher.ScheduleAsync(subject, new TestMessage("new"), fireAt.AddMinutes(5), key);
                return (result, DateTimeOffset.UtcNow);
            }));
        }

        var returned = await Task.WhenAll(replacements);
        var results = returned.Select(r => r.Result).ToArray();
        await Task.Delay(2000);

        // A Replace that returned clearly before the fire time removed the old schedule before it could fire.
        var early = Enumerable.Range(0, count).Where(i => returned[i].ReturnedAt <= fireAt - ScheduleRaces.ClearlyBefore)
            .ToList();
        var firedAfterReplace = early
            .Where(i => f.Collector.Received.Any(r => r.Subject == $"{f.Root}.t{i}" && r.Json<TestMessage>().Content == "old"))
            .Select(i => $"t{i}")
            .ToList();
        Assert.IsNotEmpty(early, "No Replace returned before the fire time");
        Assert.IsEmpty(firedAfterReplace,
            $"Old schedules fired although their Replace returned before the fire time: {string.Join(", ", firedAfterReplace)}");

        var info = await f.Js.GetStreamAsync(f.Stream, new StreamInfoRequest { SubjectsFilter = "_sched.>" });
        var pending = info.Info.State.Subjects?.Keys.ToHashSet() ?? [];
        var lost = Enumerable.Range(0, count)
            .Where(i => !pending.Contains(ScheduleTargets.HolderSubject(f.Stream, $"{f.Root}.t{i}", "k", results[i].ScheduleId)))
            .Select(i => $"t{i} (seq {results[i].Sequence})")
            .ToList();

        Assert.IsTrue(results.All(r => r.Created));
        Assert.IsEmpty(lost, $"Replacements lost: {string.Join(", ", lost)}");
        Assert.HasCount(count, pending, "One pending holder per key: only the replacements are left");
        var oldFires = f.Collector.Received.Count(r => r.Json<TestMessage>().Content == "old");
        TestContext.WriteLine($"{oldFires} of {count} old schedules fired before their Replace removed them; " +
                              $"{early.Count} Replaces returned before the fire time");
    }

    [TestMethod]
    public async Task Cancel_RacingTheFire_EndsInADocumentedOutcome()
    {
        await using var f = await ScheduleFixture.CreateAsync();

        var outcomes = await ScheduleRaces.CancelAsync(f.Publisher, f.Collector, f.Root);

        TestContext.WriteLine(string.Join(Environment.NewLine, outcomes));
    }

    [TestMethod]
    public async Task StreamWithoutSchedules_ThrowsConfigurationException()
    {
        await using var connection = CreateConnection();
        var js = CreateJetStreamContext(connection);
        var stream = TestNames.Unique("NOSCHED");
        var root = TestNames.Unique("nosched");
        await js.CreateStreamAsync(new StreamConfig(stream, [$"{root}.>"]));

        var publisher = CreatePublisher(connection, js);
        var error = await Assert.ThrowsExactlyAsync<NatsMessagingConfigurationException>(() =>
            publisher.ScheduleAsync($"{root}.timer", new TestMessage("x"), DateTimeOffset.UtcNow.AddSeconds(5)));
        StringAssert.Contains(error.Message, "AllowMsgSchedules is off");

        await Assert.ThrowsExactlyAsync<NatsMessagingConfigurationException>(() =>
            publisher.PublishAsync($"{root}.timer", new TestMessage("x"), TimeSpan.FromSeconds(5)));
    }

    [TestMethod]
    public async Task SubjectWithoutAStream_ThrowsConfigurationException()
    {
        await using var connection = CreateConnection();
        var publisher = CreatePublisher(connection, CreateJetStreamContext(connection));

        await Assert.ThrowsExactlyAsync<NatsMessagingConfigurationException>(() =>
            publisher.ScheduleAsync($"{TestNames.Unique("nowhere")}.timer", new TestMessage("x"), DateTimeOffset.UtcNow.AddSeconds(5)));
    }

    [TestMethod]
    public async Task DelayOfMaxAgeOrMore_Throws()
    {
        await using var f = await ScheduleFixture.CreateAsync(configure: d =>
        {
            d.Retention = StreamConfigRetention.Limits;
            d.MaxAge = 60;
        });

        await Assert.ThrowsExactlyAsync<ArgumentOutOfRangeException>(() =>
            f.Publisher.ScheduleAsync($"{f.Root}.timer", new TestMessage("x"), DateTimeOffset.UtcNow.AddSeconds(61)));

        var accepted = await f.Publisher.ScheduleAsync($"{f.Root}.timer", new TestMessage("x"), DateTimeOffset.UtcNow.AddSeconds(30));
        Assert.IsTrue(accepted.Created);
    }

    [TestMethod]
    public async Task SubjectsOfADeclaredStream_AddNoCacheEntry_AndServerResolvedSubjectsAreBounded()
    {
        await using var connection = CreateConnection();
        var js = CreateJetStreamContext(connection);
        var root = TestNames.Unique("cache");
        var definition = StreamDefinition.Scheduled(TestNames.Unique("CACHE"), $"{root}.>");
        await js.CreateStreamAsync(definition.ToStreamConfig());

        // Subjects that carry ids, as per-journey timers would.
        var declared = new ScheduleTargets(js, [definition], NullLogger.Instance, TimeSpan.FromSeconds(15));
        for (var i = 0; i < 300; i++)
        {
            Assert.AreEqual(definition.Name, (await declared.ResolveAsync($"{root}.reschedule.j{i}", CancellationToken.None)).Stream);
        }

        Assert.AreEqual(0, declared.CachedSubjects);

        var resolved = new ScheduleTargets(js, [], NullLogger.Instance, TimeSpan.FromSeconds(15), maxCachedSubjects: 50);
        for (var i = 0; i < 120; i++)
        {
            Assert.AreEqual(definition.Name, (await resolved.ResolveAsync($"{root}.reschedule.j{i}", CancellationToken.None)).Stream);
        }

        Assert.IsLessThanOrEqualTo(50, resolved.CachedSubjects);
        resolved.Invalidate(definition.Name);
        Assert.AreEqual(0, resolved.CachedSubjects, "Invalidating a stream drops the subjects resolved to it");
    }

    [TestMethod]
    public async Task StreamReplacedUnderTheSameSubjects_TheNextScheduleFindsTheNewStream()
    {
        await using var connection = CreateConnection();
        var js = CreateJetStreamContext(connection);
        var root = TestNames.Unique("replaced");
        var first = StreamDefinition.Scheduled(TestNames.Unique("SCHEDA"), $"{root}.>");
        var second = StreamDefinition.Scheduled(TestNames.Unique("SCHEDB"), $"{root}.>");
        await js.CreateStreamAsync(first.ToStreamConfig());
        var publisher = CreatePublisher(connection, js, o => o.PublishTimeout = TimeSpan.FromSeconds(1));
        var subject = $"{root}.timer";
        await publisher.ScheduleAsync(subject, new TestMessage("x"), DateTimeOffset.UtcNow.AddMinutes(1));

        await js.DeleteStreamAsync(first.Name);
        await js.CreateStreamAsync(second.ToStreamConfig());
        try
        {
            // The cached stream is gone: at most one call finds that out, and the next resolves the subject again.
            try
            {
                await publisher.ScheduleAsync(subject, new TestMessage("x"), DateTimeOffset.UtcNow.AddMinutes(1));
            }
            catch (Exception e) when (e is NatsPublishException or NatsMessagingConfigurationException)
            {
                TestContext.WriteLine($"first call after the replace: {e.GetType().Name}: {e.Message}");
            }

            var result = await publisher.ScheduleAsync(subject, new TestMessage("y"), DateTimeOffset.UtcNow.AddMinutes(1));

            Assert.IsTrue(result.Created);
            var stored = await (await js.GetStreamAsync(second.Name)).GetAsync(new StreamMsgGetRequest { Seq = result.Sequence });
            Assert.AreEqual(result.ScheduleId, NatsPublisher.ReadHeader(stored.Message.Hdrs, "X-Schedule-Id"));
        }
        finally
        {
            await js.DeleteStreamAsync(second.Name);
        }
    }

    [TestMethod]
    [DataRow("")]
    [DataRow("a.b")]
    [DataRow("with space")]
    [DataRow("star*")]
    [DataRow("line\n")]
    public async Task InvalidKey_Throws(string key)
    {
        await using var f = await ScheduleFixture.CreateAsync();

        await Assert.ThrowsExactlyAsync<ArgumentException>(() => f.Publisher.ScheduleAsync($"{f.Root}.timer",
            new TestMessage("x"), DateTimeOffset.UtcNow.AddSeconds(5), new ScheduleOptions { Key = key }));
        await Assert.ThrowsExactlyAsync<ArgumentException>(() => f.Publisher.CancelScheduleAsync($"{f.Root}.timer", key));
    }

    [TestMethod]
    public async Task InvalidKeyLengthOrSubject_Throws()
    {
        await using var f = await ScheduleFixture.CreateAsync();
        var deliverAt = DateTimeOffset.UtcNow.AddSeconds(5);

        await Assert.ThrowsExactlyAsync<ArgumentException>(() => f.Publisher.ScheduleAsync($"{f.Root}.timer",
            new TestMessage("x"), deliverAt, new ScheduleOptions { Key = new string('k', 129) }));
        await Assert.ThrowsExactlyAsync<ArgumentException>(() =>
            f.Publisher.ScheduleAsync($"{f.Root}.*", new TestMessage("x"), deliverAt));
        await Assert.ThrowsExactlyAsync<ArgumentException>(() =>
            f.Publisher.ScheduleAsync($"_sched.{f.Stream}.x", new TestMessage("x"), deliverAt));

        var longest = await f.Publisher.ScheduleAsync($"{f.Root}.timer", new TestMessage("x"), deliverAt,
            new ScheduleOptions { Key = new string('k', 128) });
        Assert.IsTrue(longest.Created);
    }

    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task DeclaredOrServerResolvedStream_Works(bool declare)
    {
        await using var f = await ScheduleFixture.CreateAsync(declare);

        var result = await f.Publisher.ScheduleAsync($"{f.Root}.timer", new TestMessage("x"), DateTimeOffset.UtcNow.AddMilliseconds(500));
        var fired = (await f.Collector.WaitForAsync(1, TimeSpan.FromSeconds(10)))[0];

        Assert.AreEqual(result.ScheduleId, fired.Header("X-Schedule-Id"));
    }

    [TestMethod]
    [DataRow("th-TH")]
    [DataRow("ar-SA")]
    public async Task Schedule_WorksUnderNonGregorianCultures(string culture)
    {
        await using var f = await ScheduleFixture.CreateAsync();
        var previous = (CultureInfo.CurrentCulture, CultureInfo.CurrentUICulture);
        CultureInfo.CurrentCulture = CultureInfo.CurrentUICulture = CultureInfo.GetCultureInfo(culture);
        try
        {
            var deliverAt = DateTimeOffset.UtcNow.AddMilliseconds(1234);
            var result = await f.Publisher.ScheduleAsync($"{f.Root}.timer", new TestMessage("x"), deliverAt,
                new ScheduleOptions { Key = "k" });
            var fired = (await f.Collector.WaitForAsync(1, TimeSpan.FromSeconds(10)))[0];

            Assert.AreEqual(result.ScheduleId, fired.Header("X-Schedule-Id"));
            Assert.AreEqual(deliverAt, DateTimeOffset.Parse(fired.Header("X-Scheduled-For")!, CultureInfo.InvariantCulture));
            Assert.IsTrue(fired.Stored >= deliverAt - EarlyTolerance);
            StringAssert.StartsWith(fired.Header("X-Sent-Time"), DateTime.UtcNow.Year.ToString(CultureInfo.InvariantCulture));
        }
        finally
        {
            (CultureInfo.CurrentCulture, CultureInfo.CurrentUICulture) = previous;
        }
    }

    [TestMethod]
    public async Task KeyedCallsOnASubject_NeverTouchTheSchedulesOfTheSubjectNamedByItsKey()
    {
        await using var f = await ScheduleFixture.CreateAsync();
        var parent = $"{f.Root}.a";
        var child = $"{f.Root}.a.b";
        var later = DateTimeOffset.UtcNow.AddSeconds(30);

        // The holders of subject "a.b" start with the holder prefix of subject "a" and key "b".
        var keyedChild = await f.Publisher.ScheduleAsync(child, new TestMessage("child keyed"), later,
            new ScheduleOptions { Key = "c" });
        var unkeyedChild = await f.Publisher.ScheduleAsync(child, new TestMessage("child unkeyed"), later);
        Assert.AreEqual(2L, await HolderCountAsync(f, child));

        var created = await f.Publisher.ScheduleAsync(parent, new TestMessage("parent"), later,
            new ScheduleOptions { Key = "b", Mode = ScheduleMode.CreateIfAbsent });
        Assert.IsTrue(created.Created, "No schedule of subject 'a' with key 'b' was pending");
        Assert.AreNotEqual(keyedChild.ScheduleId, created.ScheduleId);
        Assert.AreNotEqual(unkeyedChild.ScheduleId, created.ScheduleId);

        await f.Publisher.ScheduleAsync(parent, new TestMessage("parent v2"), later, new ScheduleOptions { Key = "b" });
        Assert.AreEqual(2L, await HolderCountAsync(f, child), "A Replace on ('a', 'b') kept the holders of 'a.b'");
        Assert.AreEqual(3L, await HolderCountAsync(f));

        Assert.IsTrue(await f.Publisher.CancelScheduleAsync(parent, "b"));
        Assert.IsFalse(await f.Publisher.CancelScheduleAsync(parent, "b"));
        Assert.AreEqual(2L, await HolderCountAsync(f, child), "A Cancel of ('a', 'b') kept the holders of 'a.b'");
        Assert.AreEqual(2L, await HolderCountAsync(f));
    }

    [TestMethod]
    public async Task TwoConcurrentReplaces_OfOneKey_LeaveOneHolder_TheHigherSequence()
    {
        await using var f = await ScheduleFixture.CreateAsync();
        var key = new ScheduleOptions { Key = "j" };
        var later = DateTimeOffset.UtcNow.AddMinutes(5);

        for (var round = 0; round < 30; round++)
        {
            var subject = $"{f.Root}.journey{round}";
            await f.Publisher.ScheduleAsync(subject, new TestMessage("seed"), later, key);

            var results = await Task.WhenAll(
                f.Publisher.ScheduleAsync(subject, new TestMessage("pod-1"), later, key),
                f.Publisher.ScheduleAsync(subject, new TestMessage("pod-2"), later, key));

            var pending = await PendingHoldersAsync(f, subject);
            var newest = results.MaxBy(r => r.Sequence)!;
            CollectionAssert.AreEqual(new[] { ScheduleTargets.HolderSubject(f.Stream, subject, "j", newest.ScheduleId) },
                pending, $"Round {round}: one pending holder, the one with the higher sequence");
        }
    }

    [TestMethod]
    public async Task Replace_WhosePurgeFails_IsOutcomeUnknown_AndACallAgainLeavesOnlyTheNewest()
    {
        var failPurge = 1;
        await using var f = await ScheduleFixture.CreateAsync(options: o => o.PublisherTestHooks.BeforeReplacePurge =
            () => Interlocked.Exchange(ref failPurge, 0) == 1
                ? Task.FromException(new NatsException("injected purge failure"))
                : Task.CompletedTask);
        var subject = $"{f.Root}.journey";
        var key = new ScheduleOptions { Key = "j" };
        var later = DateTimeOffset.UtcNow.AddMinutes(5);
        await f.Publisher.ScheduleAsync(subject, new TestMessage("v1"), later, key);

        var error = await Assert.ThrowsExactlyAsync<NatsPublishException>(() =>
            f.Publisher.ScheduleAsync(subject, new TestMessage("v2"), later, key));
        Assert.IsTrue(error.OutcomeUnknown);
        StringAssert.Contains(error.Message, "both may fire");
        Assert.AreEqual(2L, await HolderCountAsync(f, subject), "The new holder is stored, the old one is not removed");

        var third = await f.Publisher.ScheduleAsync(subject, new TestMessage("v3"), later, key);
        CollectionAssert.AreEqual(new[] { ScheduleTargets.HolderSubject(f.Stream, subject, "j", third.ScheduleId) },
            await PendingHoldersAsync(f, subject), "The call again replaced both");
    }

    [TestMethod]
    public async Task UnkeyedScheduleWithAScheduleId_RetriedAfterAnUnknownOutcome_IsStoredAndFiresOnce()
    {
        var dropAcks = 1;
        await using var f = await ScheduleFixture.CreateAsync(options: o => o.PublisherTestHooks.DropPubAck =
            (subject, _) => subject.StartsWith("_sched.", StringComparison.Ordinal) && Volatile.Read(ref dropAcks) == 1);
        var subject = $"{f.Root}.remind";
        var options = new ScheduleOptions { ScheduleId = "reminder-42", Timeout = TimeSpan.FromSeconds(1) };
        var deliverAt = DateTimeOffset.UtcNow.AddSeconds(3);

        var error = await Assert.ThrowsExactlyAsync<NatsPublishException>(() =>
            f.Publisher.ScheduleAsync(subject, new TestMessage("remind"), deliverAt, options));
        Assert.IsTrue(error.OutcomeUnknown);
        Assert.AreEqual("reminder-42", error.MessageId);
        Assert.AreEqual(1L, await HolderCountAsync(f), "The first attempt was stored");

        Volatile.Write(ref dropAcks, 0);
        var retry = await f.Publisher.ScheduleAsync(subject, new TestMessage("remind"), deliverAt, options);

        Assert.IsTrue(retry.Created);
        Assert.AreEqual("reminder-42", retry.ScheduleId);
        Assert.AreEqual(1L, await HolderCountAsync(f), "The retry was a duplicate");
        var fired = await f.Collector.WaitForAsync(1, TimeSpan.FromSeconds(10));
        await Task.Delay(1000);
        Assert.HasCount(1, f.Collector.Received);
        Assert.AreEqual("reminder-42", fired[0].Header("X-Schedule-Id"));

        await Assert.ThrowsExactlyAsync<ArgumentException>(() => f.Publisher.ScheduleAsync(subject,
            new TestMessage("x"), deliverAt, new ScheduleOptions { ScheduleId = "not.one.token" }));
    }

    [TestMethod]
    public async Task ScheduleAndCancel_WhenJetStreamDoesNotAnswer_RetryInTheBudget_AndThrowNatsPublishException()
    {
        // A server without JetStream: every stream lookup gets no responders, as during a meta leader election.
        await using var server = await NatsServerProcess.StartAsync(TestEnvironment.UnusedPort(), jetStream: false);
        await using var connection = new NatsConnection(new NatsOpts
        {
            Url = server.Url,
            SerializerRegistry = NatsJsonSerializerRegistry.Default
        });
        var js = CreateJetStreamContext(connection);
        var publisher = CreatePublisher(connection, js, o =>
        {
            o.PublishTimeout = TimeSpan.FromSeconds(1);
            o.StreamDefinitions = [StreamDefinition.Scheduled(TestNames.Unique("NOJS"), "nojs.>")];
        });

        var stopwatch = Stopwatch.StartNew();
        var schedule = await Assert.ThrowsExactlyAsync<NatsPublishException>(() => publisher.ScheduleAsync(
            "nojs.timer", new TestMessage("x"), DateTimeOffset.UtcNow.AddMinutes(1),
            new ScheduleOptions { Key = "k", Mode = ScheduleMode.CreateIfAbsent }));
        var scheduleTook = stopwatch.Elapsed;
        stopwatch.Restart();
        var cancel = await Assert.ThrowsExactlyAsync<NatsPublishException>(() =>
            publisher.CancelScheduleAsync("nojs.timer", "k"));
        var cancelTook = stopwatch.Elapsed;

        TestContext.WriteLine($"schedule: {schedule.Failure} after {scheduleTook}: {schedule.Message}");
        TestContext.WriteLine($"cancel: {cancel.Failure} after {cancelTook}: {cancel.Message}");
        Assert.AreEqual(PublishFailure.NoStream, schedule.Failure);
        Assert.AreEqual(PublishFailure.NoStream, cancel.Failure);
        Assert.IsInstanceOfType<NatsNoRespondersException>(schedule.InnerException);
        Assert.IsTrue(scheduleTook >= TimeSpan.FromMilliseconds(700) && cancelTook >= TimeSpan.FromMilliseconds(700),
            "No responders are retried until the budget ends");
        Assert.IsTrue(scheduleTook < TimeSpan.FromSeconds(2) && cancelTook < TimeSpan.FromSeconds(2));
    }

    private static async Task<long> HolderCountAsync(ScheduleFixture f, string? subject = null)
    {
        var filter = subject == null ? "_sched.>" : $"_sched.{f.Stream}.{subject}.*.*";
        var info = await f.Js.GetStreamAsync(f.Stream, new StreamInfoRequest { SubjectsFilter = filter });
        return info.Info.State.Subjects?.Values.Sum(v => (long)v) ?? 0;
    }

    private static async Task<string[]> PendingHoldersAsync(ScheduleFixture f, string subject)
    {
        var info = await f.Js.GetStreamAsync(f.Stream,
            new StreamInfoRequest { SubjectsFilter = $"_sched.{f.Stream}.{subject}.*.*" });
        return info.Info.State.Subjects?.Keys.ToArray() ?? [];
    }

    /// <summary>
    /// A schedule-enabled stream with unique names, a publisher and a collector on "{root}.>".
    /// </summary>
    private sealed class ScheduleFixture : IAsyncDisposable
    {
        private ScheduleFixture(NatsConnection connection, NatsJSContext js, string stream, string root,
            NatsPublisher publisher, StreamCollector collector)
        {
            Connection = connection;
            Js = js;
            Stream = stream;
            Root = root;
            Publisher = publisher;
            Collector = collector;
        }

        public NatsConnection Connection { get; }

        public NatsJSContext Js { get; }

        public string Stream { get; }

        public string Root { get; }

        public NatsPublisher Publisher { get; }

        public StreamCollector Collector { get; }

        /// <param name="declare">Declare the stream in StreamDefinitions; otherwise the publisher resolves it
        /// through the server.</param>
        /// <param name="configure">Changes to the <see cref="StreamDefinition.Scheduled"/> definition.</param>
        /// <param name="options">Changes to the publisher's options.</param>
        public static async Task<ScheduleFixture> CreateAsync(bool declare = false, Action<StreamDefinition>? configure = null,
            Action<NatsMessagingOptions>? options = null)
        {
            var connection = CreateConnection();
            var js = CreateJetStreamContext(connection);
            var stream = TestNames.Unique("SCHED");
            var root = TestNames.Unique("sched");
            var definition = StreamDefinition.Scheduled(stream, $"{root}.>");
            configure?.Invoke(definition);
            await js.CreateStreamAsync(definition.ToStreamConfig());

            var publisher = CreatePublisher(connection, js, o =>
            {
                if (declare)
                {
                    o.StreamDefinitions = [definition];
                }

                options?.Invoke(o);
            });
            var collector = await StreamCollector.StartAsync(js, stream, $"{root}.>");
            return new ScheduleFixture(connection, js, stream, root, publisher, collector);
        }

        public async ValueTask DisposeAsync()
        {
            await Collector.DisposeAsync();
            await Connection.DisposeAsync();
        }
    }
}
