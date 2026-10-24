namespace Messaging.NATS.IntegrationTests;

/// <summary>
/// The Replace and Cancel races against a fire (design 2.8 guarantees), shared by the R1 and R3 tests. Each run
/// asserts that it ends in a documented outcome, and returns one line per offset for the test log. A call that
/// returned clearly before the fire time has no race: its outcome must be the plain one (replaced or cancelled
/// before the fire), so a Replace or Cancel that removes nothing fails the test.
/// </summary>
public static class ScheduleRaces
{
    public static readonly int[] Offsets = [-1000, -300, -100, 0, 100, 300];

    // Clock and timestamp skew between this process and the server on the same machine.
    private static readonly TimeSpan InFlightTolerance = TimeSpan.FromMilliseconds(20);

    // A call that returned this long before the fire time removed the old holder before it could fire.
    public static readonly TimeSpan ClearlyBefore = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// Schedules v1 for each offset, publishes a Replace (v2, one second later) at deliverAt + offset, and checks
    /// what was delivered.
    /// </summary>
    public static async Task<IReadOnlyList<string>> ReplaceAsync(IPublisher publisher, StreamCollector collector,
        string root)
    {
        var runs = await Task.WhenAll(Offsets.Select(async (offset, index) =>
        {
            var subject = $"{root}.replace{index}";
            var options = new ScheduleOptions { Key = "k" };
            var deliverAt = DateTimeOffset.UtcNow.AddSeconds(1.5);
            var first = await publisher.ScheduleAsync(subject, new TestMessage("v1"), deliverAt, options);
            await DelayUntil(deliverAt.AddMilliseconds(offset));
            var second = await publisher.ScheduleAsync(subject, new TestMessage("v2"), deliverAt.AddSeconds(1), options);
            var replacedAt = DateTimeOffset.UtcNow;
            return (Offset: offset, Subject: subject, First: first, Second: second,
                Early: replacedAt <= deliverAt - ClearlyBefore);
        }));

        await DelayUntil(runs.Max(r => r.Second.DeliverAt).AddSeconds(1.5));

        var outcomes = new List<string>();
        foreach (var run in runs)
        {
            var deliveries = Fires(collector, run.Subject);
            AssertFromItsHolder(deliveries, ("v1", run.First.ScheduleId), ("v2", run.Second.ScheduleId));

            var payloads = string.Join("+", deliveries.Select(d => d.Json<TestMessage>().Content));
            // Each schedule has its own holder subject, so the fire's purge never removes the replacement: "v1"
            // alone (the replacement lost) is not a documented outcome. Before the fire time only "v2" is.
            var outcome = (payloads, run.Early) switch
            {
                ("v2", _) => "replaced before the fire",
                ("v1+v2", false) => "fired, then replaced (two deliveries)",
                _ => null
            };
            Assert.IsNotNull(outcome,
                $"Offset {run.Offset} ms (returned before the fire time: {run.Early}): undocumented outcome '{payloads}'");
            outcomes.Add($"replace {run.Offset,5} ms: {outcome}");
        }

        Assert.IsTrue(runs.Any(r => r.Early), "No Replace returned before the fire time, so none was checked strictly");
        return outcomes;
    }

    /// <summary>
    /// Schedules v1 for each offset, cancels it at deliverAt + offset, and checks what was delivered.
    /// </summary>
    public static async Task<IReadOnlyList<string>> CancelAsync(IPublisher publisher, StreamCollector collector,
        string root)
    {
        var runs = await Task.WhenAll(Offsets.Select(async (offset, index) =>
        {
            var subject = $"{root}.cancel{index}";
            var deliverAt = DateTimeOffset.UtcNow.AddSeconds(1.5);
            var scheduled = await publisher.ScheduleAsync(subject, new TestMessage("v1"), deliverAt,
                new ScheduleOptions { Key = "k" });
            await DelayUntil(deliverAt.AddMilliseconds(offset));
            var cancelledAt = DateTimeOffset.UtcNow;
            var cancelled = await publisher.CancelScheduleAsync(subject, "k");
            return (Offset: offset, Subject: subject, Scheduled: scheduled, Cancelled: cancelled, CancelledAt: cancelledAt,
                Early: DateTimeOffset.UtcNow <= deliverAt - ClearlyBefore);
        }));

        await DelayUntil(runs.Max(r => r.Scheduled.DeliverAt).AddSeconds(1.5));

        var outcomes = new List<string>();
        foreach (var run in runs)
        {
            var deliveries = Fires(collector, run.Subject);
            AssertFromItsHolder(deliveries, ("v1", run.Scheduled.ScheduleId));
            Assert.IsLessThanOrEqualTo(1, deliveries.Count, $"Offset {run.Offset} ms: {deliveries.Count} deliveries");

            var outcome = (run.Cancelled, deliveries.Count, run.Early) switch
            {
                (true, 0, _) => "cancelled before the fire",
                (false, 1, false) => "already fired (Cancel returned false)",
                (true, 1, false) => "cancelled while the fire was in flight (Cancel returned true, still delivered)",
                _ => null
            };
            Assert.IsNotNull(outcome,
                $"Offset {run.Offset} ms (returned before the fire time: {run.Early}): Cancel returned " +
                $"{run.Cancelled} and {deliveries.Count} deliveries");

            // Storing a fire purges its holder, so Cancel can only purge one whose fire was stored after it started.
            if (run is { Cancelled: true } && deliveries.Count == 1)
            {
                Assert.IsTrue(deliveries[0].Stored >= run.CancelledAt - InFlightTolerance,
                    $"Offset {run.Offset} ms: Cancel returned true, but the fire was stored " +
                    $"{(run.CancelledAt - deliveries[0].Stored).TotalMilliseconds:F0} ms before the call");
            }

            outcomes.Add($"cancel  {run.Offset,5} ms: {outcome}");
        }

        Assert.IsTrue(runs.Any(r => r.Early), "No Cancel returned before the fire time, so none was checked strictly");
        return outcomes;
    }

    /// <summary>
    /// Every delivery's X-Schedule-Id is the id of the holder its payload came from.
    /// </summary>
    public static void AssertFromItsHolder(IEnumerable<ReceivedMessage> deliveries,
        params (string Payload, string ScheduleId)[] holders)
    {
        foreach (var delivery in deliveries)
        {
            var payload = delivery.Json<TestMessage>().Content;
            var holder = holders.SingleOrDefault(h => h.Payload == payload);
            Assert.IsNotNull(holder.ScheduleId, $"Unexpected payload {payload}");
            Assert.AreEqual(holder.ScheduleId, delivery.Header("X-Schedule-Id"), $"{payload} carries another holder's id");
        }
    }

    public static async Task DelayUntil(DateTimeOffset time)
    {
        var delay = time - DateTimeOffset.UtcNow;
        if (delay > TimeSpan.Zero)
        {
            await Task.Delay(delay);
        }
    }

    /// <summary>
    /// The stored messages on <paramref name="subject"/> in stream order. A redelivery to the collector (same
    /// sequence) is counted once.
    /// </summary>
    private static List<ReceivedMessage> Fires(StreamCollector collector, string subject) =>
        collector.Received.Where(r => r.Subject == subject).DistinctBy(r => r.Sequence).OrderBy(r => r.Sequence).ToList();
}
