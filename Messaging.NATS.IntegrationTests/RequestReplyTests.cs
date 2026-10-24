using System.Diagnostics;
using Messaging.NATS.IntegrationTests.Handlers;
using Microsoft.Extensions.DependencyInjection;
using NATS.Client.Core;

namespace Messaging.NATS.IntegrationTests;

[TestClass]
public class RequestReplyTests : NatsTestBase
{
    [TestMethod]
    public async Task ReplyHandler_RespondsToRequest()
    {
        await using var host = await TestHost.StartAsync(o => o.HandlerTypes = [typeof(TestRequestReplyHandler)]);
        await host.WaitUntilStartedAsync();

        var response = await host.Publisher.RequestAsync<PingRequest, PingResponse>(TestRequestReplyHandler.Subject,
            new PingRequest("hello"));

        Assert.IsNotNull(response);
        Assert.AreEqual("hello", response!.Payload);
        Assert.AreEqual("TestHandler", response.HandledBy);
    }

    [TestMethod]
    public async Task ReplyHandler_FailingRequest_DoesNotStopTheSubscription()
    {
        await using var host = await TestHost.StartAsync(o => o.HandlerTypes = [typeof(TestRequestReplyHandler)]);
        await host.WaitUntilStartedAsync();

        await Assert.ThrowsAsync<NatsNoReplyException>(() => host.Publisher.RequestAsync<PingRequest, PingResponse>(
            TestRequestReplyHandler.Subject, new PingRequest(TestRequestReplyHandler.Fail), TimeSpan.FromMilliseconds(500)));

        var response = await host.Publisher.RequestAsync<PingRequest, PingResponse>(TestRequestReplyHandler.Subject,
            new PingRequest("again"));
        Assert.AreEqual("again", response!.Payload);
        Assert.AreEqual(ComponentState.Running, host.Status.Get($"reply:{TestRequestReplyHandler.Subject}")!.State);
    }

    [TestMethod]
    public async Task RequestHandledWhileTheHostStops_StillGetsItsReply()
    {
        var gate = new ReplyGate();
        await using var host = await TestHost.StartAsync(o => o.HandlerTypes = [typeof(GatedReplyHandler)],
            services => services.AddSingleton(gate));
        await host.WaitUntilStartedAsync();

        await using var client = CreateConnection();
        var request = client.RequestAsync<PingRequest, PingResponse>(GatedReplyHandler.Subject, new PingRequest("hi"),
            replyOpts: new NatsSubOpts { Timeout = TimeSpan.FromSeconds(4) }).AsTask();
        await gate.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Other publishes on the host's connection make the reply take the slow path, which checks its token.
        var connection = host.Services.GetRequiredService<INatsConnection>();
        using var flood = new CancellationTokenSource();
        var noise = new byte[64 * 1024];
        var flooders = Enumerable.Range(0, 16).Select(_ => Task.Run(async () =>
        {
            while (!flood.IsCancellationRequested)
            {
                try
                {
                    await connection.PublishAsync($"{GatedReplyHandler.Subject}.noise", noise);
                }
                catch (Exception)
                {
                    // The flood only adds contention; a failed noise publish does not matter.
                }
            }
        })).ToArray();

        await Task.Delay(200);
        var stop = host.Host.StopAsync();
        await Task.Delay(200);
        gate.Release.TrySetResult();

        try
        {
            var reply = await request;
            Assert.AreEqual("hi", reply.Data!.Payload);
        }
        finally
        {
            await flood.CancelAsync();
            await Task.WhenAll(flooders);
            await stop;
        }
    }

    [TestMethod]
    public async Task Stop_LeavesQueuedRequestsUnhandled_AndReturnsWithoutWaitingForThem()
    {
        var backlog = new ReplyBacklog();
        await using var host = await TestHost.StartAsync(o => o.HandlerTypes = [typeof(BacklogReplyHandler)],
            services => services.AddSingleton(backlog));
        await host.WaitUntilStartedAsync();

        await using var client = CreateConnection();
        var inbox = client.NewInbox();
        await client.PublishAsync(BacklogReplyHandler.Subject, new PingRequest("first"), replyTo: inbox);
        await backlog.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        for (var i = 0; i < 200; i++)
        {
            await client.PublishAsync(BacklogReplyHandler.Subject, new PingRequest($"queued-{i}"), replyTo: inbox);
        }

        await client.PingAsync();
        await Task.Delay(200);

        var stopping = Stopwatch.StartNew();
        var stop = host.Host.StopAsync();
        await Task.Delay(200);
        backlog.Release.TrySetResult();
        await stop;
        stopping.Stop();
        await Task.Delay(300);

        Assert.AreEqual(1, backlog.Calls, "Only the request that was running when stop started was handled");
        Assert.IsLessThan(TimeSpan.FromSeconds(3), stopping.Elapsed, "Stop did not run the queued requests");
    }

    [TestMethod]
    public async Task RequestAsync_NoResponder_ThrowsNatsNoRespondersException()
    {
        await using var connection = CreateConnection();
        var js = CreateJetStreamContext(connection);

        var publisher = CreatePublisher(connection, js);

        await Assert.ThrowsExactlyAsync<NatsNoRespondersException>(async () =>
        {
            await publisher.RequestAsync<PingRequest, PingResponse>(
                $"{TestNames.Unique("nobody")}.listening",
                new PingRequest("hello"),
                timeout: TimeSpan.FromSeconds(2));
        });
    }
}
