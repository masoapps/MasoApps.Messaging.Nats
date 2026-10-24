using System.Diagnostics;
using Messaging.NATS.IntegrationTests.Handlers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Messaging.NATS.IntegrationTests;

[TestClass]
public class DrainTests
{
    [TestMethod]
    public async Task StopDuringAHandler_WaitsForIt_AndTheMessageIsAcked()
    {
        var completed = new TaskCompletionSource();
        var probe = new Probe<MessageContext<OrderEvent>>
        {
            Behaviour = async (_, _, ct) =>
            {
                await Task.Delay(2000, ct);
                completed.SetResult();
            }
        };
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>([probe], null,
            new NatsConsumerAttribute { Subject = ".created", AckWaitSeconds = 30 });

        var published = await scenario.Publisher.PublishAsync($"{scenario.Root}.created", new OrderEvent("ORD-1", 1m));
        await probe.WaitForCallsAsync(1);
        var stopping = Stopwatch.StartNew();
        await scenario.Host.Host.StopAsync();
        stopping.Stop();

        Assert.IsTrue(completed.Task.IsCompletedSuccessfully, "The handler ran to the end");
        Assert.IsLessThan(TimeSpan.FromSeconds(5), stopping.Elapsed, "Stop did not wait for the shutdown timeout");
        var info = await ConsumerInfoAsync(scenario);
        Assert.AreEqual(published.Sequence, info.AckFloor.StreamSeq, "Acked before the connection closed");
        Assert.AreEqual(0L, info.NumAckPending);
        Assert.AreEqual(0L, info.NumRedelivered);
        Assert.AreEqual(ComponentState.Stopped, scenario.Host.Status.Get(scenario.Component)!.State);
    }

    [TestMethod]
    public async Task AfterStopStarts_NoNewMessageIsDelivered()
    {
        var probe = new Probe<MessageContext<OrderEvent>>
        {
            Behaviour = (_, call, _) => call == 1 ? Task.Delay(2000) : Task.CompletedTask
        };
        var (stream, root) = ConsumerBindings.UniqueStream("DRAIN");
        var queueGroup = TestNames.Unique("qg");
        await using var scenario = await ConsumerScenario.StartAsync<OrderHandler>(stream, root, [probe], null,
            new NatsConsumerAttribute { Subject = ".created", QueueGroup = queueGroup, AckWaitSeconds = 2, MaxConcurrency = 2 });

        await scenario.Publisher.PublishAsync($"{root}.created", new OrderEvent("ORD-1", 1m));
        await probe.WaitForCallsAsync(1);

        // The second slot has a pull open while the first handler runs; stopping must close it.
        var stop = scenario.Host.Host.StopAsync();
        await Task.Delay(300);
        await scenario.Publisher.PublishAsync($"{root}.created", new OrderEvent("ORD-2", 1m));
        await stop;
        await Task.Delay(500);

        Assert.AreEqual(1, probe.Count, "Only the message that was already running was handled");
        var info = await ConsumerInfoAsync(scenario);
        Assert.AreEqual(0L, info.NumAckPending, "ORD-2 was never delivered to the stopping process");
        Assert.AreEqual(0L, info.NumRedelivered);
        Assert.AreEqual(1UL, info.NumPending, "ORD-2 waits in the stream");

        var next = new Probe<MessageContext<OrderEvent>>();
        await using var successor = await ConsumerScenario.StartAsync<OrderHandler>(stream, root, [next], null,
            new NatsConsumerAttribute { Subject = ".created", QueueGroup = queueGroup, AckWaitSeconds = 2, MaxConcurrency = 2 });
        var handled = await next.WaitForCallsAsync(1, TimeSpan.FromSeconds(10));
        Assert.AreEqual("ORD-2", handled.Single().Arg.Message.OrderId, "The message is left for the next process");
        Assert.AreEqual(1, handled.Single().Arg.DeliveryCount, "A first delivery, not one the stopping process gave back");
    }

    [TestMethod]
    public async Task HandlerPastShutdownTimeout_IsCancelled_AndTheMessageIsHandedToAnotherHost()
    {
        var cancelled = new TaskCompletionSource();
        var hanging = new Probe<MessageContext<OrderEvent>>
        {
            Behaviour = async (_, _, ct) =>
            {
                try
                {
                    await Task.Delay(Timeout.Infinite, ct);
                }
                catch (OperationCanceledException)
                {
                    cancelled.SetResult();
                    throw;
                }
            }
        };
        var (stream, root) = ConsumerBindings.UniqueStream("HANDOFF");
        var queueGroup = TestNames.Unique("qg");
        await using var first = await ConsumerScenario.StartAsync<OrderHandler>(stream, root, [hanging],
            o => o.ShutdownTimeout = TimeSpan.FromSeconds(1),
            new NatsConsumerAttribute { Subject = ".created", QueueGroup = queueGroup, AckWaitSeconds = 30 });

        await first.Publisher.PublishAsync($"{root}.created", new OrderEvent("ORD-1", 1m));
        await hanging.WaitForCallsAsync(1);

        var healthy = new Probe<MessageContext<OrderEvent>>();
        await using var second = await ConsumerScenario.StartAsync<OrderHandler>(stream, root, [healthy], null,
            new NatsConsumerAttribute { Subject = ".created", QueueGroup = queueGroup, AckWaitSeconds = 30 });

        var stopping = Stopwatch.StartNew();
        await first.Host.Host.StopAsync();
        var handedOff = (await healthy.WaitForCallsAsync(1, TimeSpan.FromSeconds(10))).Single().Arg;
        stopping.Stop();

        Assert.IsTrue(cancelled.Task.IsCompleted, "The handler saw its token cancelled");
        Assert.AreEqual(2, handedOff.DeliveryCount);
        Assert.AreEqual("ORD-1", handedOff.Message.OrderId);
        Assert.IsLessThan(TimeSpan.FromSeconds(10), stopping.Elapsed, "NAKed at once, not redelivered after AckWait (30 s)");
    }

    [TestMethod]
    public async Task HandlerThatReturnsAfterItsTokenIsCancelled_IsHandedToAnotherHost_NotAcked()
    {
        var cancelled = new TaskCompletionSource();
        var swallowing = new Probe<MessageContext<OrderEvent>>
        {
            Behaviour = async (_, _, ct) =>
            {
                // A catch-all handler (or a loop on the token) that stops half way and returns normally.
                try
                {
                    await Task.Delay(Timeout.Infinite, ct);
                }
                catch (OperationCanceledException)
                {
                    cancelled.SetResult();
                }
            }
        };
        var (stream, root) = ConsumerBindings.UniqueStream("SWALLOW");
        var queueGroup = TestNames.Unique("qg");
        await using var first = await ConsumerScenario.StartAsync<OrderHandler>(stream, root, [swallowing],
            o => o.ShutdownTimeout = TimeSpan.FromSeconds(1),
            new NatsConsumerAttribute { Subject = ".created", QueueGroup = queueGroup, AckWaitSeconds = 30 });

        await first.Publisher.PublishAsync($"{root}.created", new OrderEvent("ORD-1", 1m));
        await swallowing.WaitForCallsAsync(1);

        var healthy = new Probe<MessageContext<OrderEvent>>();
        await using var second = await ConsumerScenario.StartAsync<OrderHandler>(stream, root, [healthy], null,
            new NatsConsumerAttribute { Subject = ".created", QueueGroup = queueGroup, AckWaitSeconds = 30 });

        await first.Host.Host.StopAsync();
        var handedOff = (await healthy.WaitForCallsAsync(1, TimeSpan.FromSeconds(10))).Single().Arg;

        Assert.IsTrue(cancelled.Task.IsCompleted, "The handler saw its token cancelled");
        Assert.AreEqual("ORD-1", handedOff.Message.OrderId);
        Assert.AreEqual(2, handedOff.DeliveryCount, "NAKed at once, not redelivered after AckWait (30 s)");
    }

    [TestMethod]
    public async Task HostBudgetShorterThanShutdownTimeout_HandsTheMessageOffBeforeTheConnectionIsDisposed()
    {
        var cancelled = new TaskCompletionSource();
        var hanging = new Probe<MessageContext<OrderEvent>>
        {
            Behaviour = async (_, _, ct) =>
            {
                try
                {
                    await Task.Delay(Timeout.Infinite, ct);
                }
                catch (OperationCanceledException)
                {
                    cancelled.TrySetResult();
                    throw;
                }
            }
        };
        var (stream, root) = ConsumerBindings.UniqueStream("CUT");
        var queueGroup = TestNames.Unique("qg");
        var attribute = new NatsConsumerAttribute
        {
            Stream = stream.Name, Subject = $"{root}.created", QueueGroup = queueGroup, AckWaitSeconds = 30
        };

        // The host gives every hosted service 1 s; the library would wait 10 s for its handlers.
        var first = await TestHost.StartAsync(o =>
            {
                o.StreamDefinitions = [stream];
                o.Bind<OrderHandler>(attribute);
                o.ShutdownTimeout = TimeSpan.FromSeconds(10);
            },
            services =>
            {
                services.AddSingleton(hanging);
                services.Configure<HostOptions>(h => h.ShutdownTimeout = TimeSpan.FromSeconds(1));
            });
        await first.WaitUntilStartedAsync();
        await first.Publisher.PublishAsync($"{root}.created", new OrderEvent("ORD-1", 1m));
        await hanging.WaitForCallsAsync(1);

        var healthy = new Probe<MessageContext<OrderEvent>>();
        await using var second = await ConsumerScenario.StartAsync<OrderHandler>(stream, root, [healthy], null,
            new NatsConsumerAttribute { Subject = ".created", QueueGroup = queueGroup, AckWaitSeconds = 30 });

        var stopping = Stopwatch.StartNew();
        await first.Host.StopAsync();
        await ((IAsyncDisposable)first.Host).DisposeAsync();
        var handedOff = (await healthy.WaitForCallsAsync(1, TimeSpan.FromSeconds(15))).Single().Arg;
        stopping.Stop();

        Assert.IsTrue(cancelled.Task.IsCompleted, "The handler saw its token cancelled when the host budget ended");
        Assert.AreEqual(2, handedOff.DeliveryCount);
        Assert.IsLessThan(TimeSpan.FromSeconds(8), stopping.Elapsed, "NAKed before the dispose, not redelivered after AckWait (30 s)");
    }

    private static async Task<ConsumerInfo> ConsumerInfoAsync(ConsumerScenario scenario)
    {
        await using var connection = TestHost.CreateConnection();
        return (await new NatsJSContext(connection).GetConsumerAsync(scenario.Stream.Name, scenario.Durable)).Info;
    }
}
