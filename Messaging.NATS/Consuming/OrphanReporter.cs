using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Messaging.NATS;

/// <summary>
/// Reports consumers this service owns that no handler uses any more (design 2.9). It only logs and sets the
/// <c>messaging.nats.consumer.orphans</c> gauge; it never deletes a consumer.
/// <para>
/// Only <see cref="NatsMessagingOptions.ServiceName"/> proves ownership: a consumer on a stream this service consumes
/// from, whose name is not a current durable and whose owner metadata is this ServiceName, is an orphan. It is logged
/// as a Warning with the delete command and counted in the gauge. A queue group can be shared by several services,
/// so a consumer whose owner metadata is one of this service's queue groups, or that has no owner metadata and a name
/// starting with "{queueGroup}_" (a 4.x durable), is only "possibly unused": it is logged at Information, without a
/// delete command, and not counted. Consumers owned by other services are ignored.
/// </para>
/// </summary>
internal sealed class OrphanReporter(NatsJSContext js, ILogger logger)
{
    internal enum Ownership { NotOwned, Owned, Possibly }

    internal sealed record Orphan(string Stream, string Name, ulong NumPending, long NumAckPending, bool PinsMessages);

    /// <summary>
    /// Finds and logs the orphans on <paramref name="stream"/> among the durables of this service, and returns the
    /// ones it owns for certain.
    /// </summary>
    public async Task<IReadOnlyList<Orphan>> ReportAsync(string stream, IReadOnlyCollection<ConsumerDurable> durables,
        string? serviceName, CancellationToken cancellationToken)
    {
        var current = durables.Where(d => d.Stream == stream).Select(d => d.Durable).ToHashSet(StringComparer.Ordinal);
        var queueGroups = durables.Select(d => d.QueueGroup).ToHashSet(StringComparer.Ordinal);

        var info = await js.GetStreamAsync(stream, cancellationToken: cancellationToken).ConfigureAwait(false);
        var interest = info.Info.Config.Retention == StreamConfigRetention.Interest;

        var orphans = new List<Orphan>();
        await foreach (var consumer in js.ListConsumersAsync(stream, cancellationToken).ConfigureAwait(false))
        {
            var consumerInfo = consumer.Info;
            var name = consumerInfo.Name;
            if (current.Contains(name))
            {
                continue;
            }

            var ownership = OwnershipOf(consumerInfo.Config, name, serviceName, queueGroups);
            if (ownership == Ownership.NotOwned)
            {
                continue;
            }

            var orphan = new Orphan(stream, name, consumerInfo.NumPending, consumerInfo.NumAckPending, interest);
            if (ownership == Ownership.Owned)
            {
                orphans.Add(orphan);
                LogOwned(orphan);
            }
            else
            {
                LogPossible(orphan);
            }
        }

        return orphans;
    }

    internal static Ownership OwnershipOf(ConsumerConfig config, string name, string? serviceName,
        IReadOnlySet<string> queueGroups)
    {
        if (config.Metadata != null && config.Metadata.TryGetValue(ConsumerDurable.OwnerMetadata, out var owner))
        {
            if (!string.IsNullOrEmpty(serviceName) && owner == serviceName)
            {
                return Ownership.Owned;
            }

            return queueGroups.Contains(owner) ? Ownership.Possibly : Ownership.NotOwned;
        }

        return queueGroups.Any(g => name.StartsWith($"{g}_", StringComparison.Ordinal))
            ? Ownership.Possibly
            : Ownership.NotOwned;
    }

    private void LogOwned(Orphan orphan)
    {
        if (orphan.PinsMessages)
        {
            logger.LogWarning(
                "Consumer {Consumer} on stream {Stream} belongs to this service but no handler uses it ({NumPending} pending, {NumAckPending} unacknowledged). " +
                "On this Interest stream it keeps messages from being removed. Delete it with: nats consumer rm {Stream} {Consumer}",
                orphan.Name, orphan.Stream, orphan.NumPending, orphan.NumAckPending, orphan.Stream, orphan.Name);
        }
        else
        {
            logger.LogWarning(
                "Consumer {Consumer} on stream {Stream} belongs to this service but no handler uses it ({NumPending} pending, {NumAckPending} unacknowledged). " +
                "It is kept; delete it with: nats consumer rm {Stream} {Consumer}",
                orphan.Name, orphan.Stream, orphan.NumPending, orphan.NumAckPending, orphan.Stream, orphan.Name);
        }
    }

    private void LogPossible(Orphan orphan) =>
        logger.LogInformation(
            "Consumer {Consumer} on stream {Stream} may be unused ({NumPending} pending, {NumAckPending} unacknowledged): no handler " +
            "of this service uses it, and its owner is one of this service's queue groups. Another service with the same " +
            "queue group may own it, so check before deleting it. Set a distinct ServiceName on every service to get exact orphan reports.",
            orphan.Name, orphan.Stream, orphan.NumPending, orphan.NumAckPending);
}
