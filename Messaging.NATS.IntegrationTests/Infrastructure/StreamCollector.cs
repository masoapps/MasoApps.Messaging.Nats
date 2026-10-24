using System.Text.Json;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Messaging.NATS.IntegrationTests;

/// <summary>
/// A durable JetStream consumer that acks and records everything it receives, with the local receive time.
/// </summary>
public sealed class StreamCollector : IAsyncDisposable
{
    private readonly List<ReceivedMessage> _received = [];
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _loop;

    private StreamCollector(INatsJSConsumer consumer)
    {
        _loop = Task.Run(async () =>
        {
            try
            {
                await foreach (var msg in consumer.ConsumeAsync(NatsRawSerializer<byte[]>.Default,
                                   cancellationToken: _cts.Token))
                {
                    var received = new ReceivedMessage(DateTimeOffset.UtcNow, msg.Subject, msg.Data ?? [],
                        msg.Headers ?? new NatsHeaders())
                    {
                        Sequence = msg.Metadata?.Sequence.Stream ?? 0,
                        Stored = msg.Metadata?.Timestamp ?? default
                    };
                    lock (_received)
                    {
                        _received.Add(received);
                    }

                    await msg.AckAsync(cancellationToken: _cts.Token);
                }
            }
            catch (OperationCanceledException)
            {
            }
        });
    }

    public IReadOnlyList<ReceivedMessage> Received
    {
        get
        {
            lock (_received)
            {
                return _received.ToList();
            }
        }
    }

    public static async Task<StreamCollector> StartAsync(NatsJSContext js, string stream, string filter)
    {
        var durable = TestNames.Unique("collector");
        var consumer = await js.CreateOrUpdateConsumerAsync(stream, new ConsumerConfig(durable)
        {
            FilterSubject = filter,
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
            DeliverPolicy = ConsumerConfigDeliverPolicy.All
        });
        return new StreamCollector(consumer);
    }

    /// <summary>
    /// Waits until at least <paramref name="count"/> messages have arrived, and returns all of them.
    /// </summary>
    public async Task<IReadOnlyList<ReceivedMessage>> WaitForAsync(int count, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            var received = Received;
            if (received.Count >= count)
            {
                return received;
            }

            await Task.Delay(25);
        }

        var final = Received;
        Assert.IsGreaterThanOrEqualTo(count, final.Count,
            $"Expected {count} messages within {timeout}, got {final.Count}: {string.Join(", ", final.Select(r => r.Subject))}");
        return final;
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        await _loop;
        _cts.Dispose();
    }
}

public sealed record ReceivedMessage(DateTimeOffset At, string Subject, byte[] Data, NatsHeaders Headers)
{
    /// <summary>
    /// The stream sequence. A redelivery has the same sequence; a second fire of one schedule has another.
    /// </summary>
    public ulong Sequence { get; init; }

    /// <summary>
    /// The server's store time. For a fired schedule this is the fire time.
    /// </summary>
    public DateTimeOffset Stored { get; init; }

    public string? Header(string name) => Headers.TryGetValue(name, out var value) ? value.ToString() : null;

    public T Json<T>() => JsonSerializer.Deserialize<T>(Data)!;
}
