using System.Diagnostics;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Client.Serializers.Json;

namespace Messaging.NATS.IntegrationTests;

/// <summary>
/// A 3-node nats-server cluster with JetStream, for the R3 tests. The binary is NATS_SERVER_BIN, or nats-server on
/// PATH. Client ports are base..base+2 (NATS_CLUSTER_BASE_PORT). Route ports are NATS_CLUSTER_ROUTE_PORT..+2, by
/// default base+10..base+12. Unix only: a graceful stop sends SIGTERM.
/// </summary>
public sealed class NatsClusterFixture : IAsyncDisposable
{
    public const string RoutePortVariable = "NATS_CLUSTER_ROUTE_PORT";
    public const string ClusterName = "c";

    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(30);

    private readonly string _root;
    private readonly ClusterNode[] _nodes;

    private NatsClusterFixture(string root, ClusterNode[] nodes)
    {
        _root = root;
        _nodes = nodes;
        Connection = CreateConnection();
        JetStream = new NatsJSContext(Connection);
    }

    public IReadOnlyList<ClusterNode> Nodes => _nodes;

    /// <summary>
    /// Every node's client URL, comma separated, so a client fails over when a node stops.
    /// </summary>
    public string Url => string.Join(",", _nodes.Select(n => n.Url));

    /// <summary>
    /// An admin connection with the JSON serialiser registry.
    /// </summary>
    public NatsConnection Connection { get; }

    public NatsJSContext JetStream { get; }

    /// <summary>
    /// Starts the cluster on the ports from the environment, or on free ports when NATS_CLUSTER_BASE_PORT is unset
    /// (see <see cref="TestEnvironment"/>).
    /// </summary>
    public static Task<NatsClusterFixture> StartFromEnvironmentAsync()
    {
        var basePortSet = Environment.GetEnvironmentVariable(TestEnvironment.ClusterBasePortVariable) != null;
        var basePort = TestEnvironment.RequireClusterBasePort();
        var routePort = int.TryParse(Environment.GetEnvironmentVariable(RoutePortVariable), out var port)
            ? port
            : basePortSet ? basePort + 10 : TestEnvironment.FreePorts(3);
        return StartAsync(basePort, routePort);
    }

    public static async Task<NatsClusterFixture> StartAsync(int basePort, int routeBasePort)
    {
        var root = Path.Combine(Path.GetTempPath(), TestNames.Unique("nats-cluster"));
        var nodes = Enumerable.Range(0, 3)
            .Select(i => new ClusterNode($"n{i}", basePort + i, routeBasePort + i, Path.Combine(root, $"n{i}")))
            .ToArray();
        foreach (var node in nodes)
        {
            Directory.CreateDirectory(node.StoreDir);
        }

        var cluster = new NatsClusterFixture(root, nodes);
        try
        {
            foreach (var node in nodes)
            {
                cluster.Launch(node);
            }

            foreach (var node in nodes)
            {
                await WaitForListenerAsync(node);
            }

            await cluster.WaitForMetaLeaderAsync();
            return cluster;
        }
        catch
        {
            await cluster.DisposeAsync();
            throw;
        }
    }

    public ClusterNode Node(string name) =>
        _nodes.SingleOrDefault(n => n.Name == name) ?? throw new ArgumentException($"No node {name}", nameof(name));

    /// <summary>
    /// A new connection to every node, with the JSON serialiser registry.
    /// </summary>
    public NatsConnection CreateConnection() => new(new NatsOpts
    {
        Url = Url,
        SerializerRegistry = NatsJsonSerializerRegistry.Default
    });

    /// <summary>
    /// Creates a stream, retrying while the cluster cannot place its replicas yet, and waits until every replica
    /// is current.
    /// </summary>
    public async Task CreateStreamAsync(StreamConfig config)
    {
        await RetryAsync(async () => await JetStream.CreateStreamAsync(config), $"create stream {config.Name}");
        await WaitForStreamAsync(config.Name!);
    }

    public async Task<string?> LeaderOfAsync(string stream)
    {
        var info = await JetStream.GetStreamAsync(stream);
        return info.Info.Cluster?.Leader;
    }

    public async Task<string?> ConsumerLeaderOfAsync(string stream, string consumer)
    {
        var info = await JetStream.GetConsumerAsync(stream, consumer);
        return info.Info.Cluster?.Leader;
    }

    /// <summary>
    /// Asks the stream leader to step down, and returns the newly elected leader.
    /// </summary>
    public async Task<string> StepDownStreamLeaderAsync(string stream)
    {
        await WaitForStreamAsync(stream);
        var previous = await LeaderOfAsync(stream);
        await RetryAsync(async () =>
        {
            var response = await JetStream.JSRequestResponseAsync<object, StreamLeaderStepdownResponse>(
                $"$JS.API.STREAM.LEADER.STEPDOWN.{stream}", null);
            return response.Success ? response : throw new InvalidOperationException("Stepdown was not accepted");
        }, $"step down the leader of {stream}");
        return await WaitForNewLeaderAsync(() => LeaderOfAsync(stream), previous, stream);
    }

    /// <summary>
    /// Asks the consumer leader to step down, and returns the newly elected leader.
    /// </summary>
    public async Task<string> StepDownConsumerLeaderAsync(string stream, string consumer)
    {
        var previous = await ConsumerLeaderOfAsync(stream, consumer);
        await RetryAsync(async () =>
        {
            var response = await JetStream.JSRequestResponseAsync<object, ConsumerLeaderStepdownResponse>(
                $"$JS.API.CONSUMER.LEADER.STEPDOWN.{stream}.{consumer}", null);
            return response.Success ? response : throw new InvalidOperationException("Stepdown was not accepted");
        }, $"step down the leader of {stream}/{consumer}");
        return await WaitForNewLeaderAsync(() => ConsumerLeaderOfAsync(stream, consumer), previous,
            $"{stream}/{consumer}");
    }

    /// <summary>
    /// SIGKILL: the node stops at once, with no leader transfer.
    /// </summary>
    public async Task KillServerAsync(string name)
    {
        var node = Node(name);
        if (node.Process is { HasExited: false } process)
        {
            process.Kill();
            await process.WaitForExitAsync();
        }

        node.Process?.Dispose();
        node.Process = null;
    }

    /// <summary>
    /// SIGTERM: a graceful shutdown, as in a rolling restart.
    /// </summary>
    public async Task StopServerAsync(string name)
    {
        var node = Node(name);
        if (node.Process is { HasExited: false } process)
        {
            if (Kill(process.Id, SigTerm) != 0)
            {
                throw new InvalidOperationException($"Could not signal {name} (errno {Marshal.GetLastPInvokeError()})");
            }

            using var timeout = new CancellationTokenSource(WaitTimeout);
            await process.WaitForExitAsync(timeout.Token);
        }

        node.Process?.Dispose();
        node.Process = null;
    }

    /// <summary>
    /// Starts a stopped node again with its own store, and waits until it accepts clients.
    /// </summary>
    public async Task RestartServerAsync(string name)
    {
        var node = Node(name);
        if (node.IsRunning)
        {
            throw new InvalidOperationException($"{name} is running");
        }

        Launch(node);
        await WaitForListenerAsync(node);
    }

    /// <summary>
    /// Waits until the stream has a leader and every replica is current.
    /// </summary>
    public async Task WaitForStreamAsync(string stream)
    {
        var deadline = DateTime.UtcNow + WaitTimeout;
        var last = "no answer";
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                var info = await JetStream.GetStreamAsync(stream);
                var cluster = info.Info.Cluster;
                var replicas = cluster?.Replicas ?? [];
                if (cluster?.Leader != null && replicas.All(r => r is { Current: true, Offline: false }) &&
                    replicas.Count + 1 == info.Info.Config.NumReplicas)
                {
                    return;
                }

                last = $"leader {cluster?.Leader ?? "none"}, replicas " +
                       string.Join(", ", replicas.Select(r => $"{r.Name} current={r.Current} offline={r.Offline}"));
            }
            catch (NatsException e)
            {
                last = e.Message;
            }

            await Task.Delay(100);
        }

        throw new TimeoutException($"Stream {stream} did not settle: {last}");
    }

    public async ValueTask DisposeAsync()
    {
        await Connection.DisposeAsync();
        foreach (var node in _nodes)
        {
            await KillServerAsync(node.Name);
        }

        try
        {
            Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    private void Launch(ClusterNode node)
    {
        var routes = string.Join(",", _nodes.Where(n => n != node).Select(n => $"nats://127.0.0.1:{n.RoutePort}"));
        var binary = Environment.GetEnvironmentVariable("NATS_SERVER_BIN") ?? "nats-server";
        node.Process = Process.Start(new ProcessStartInfo(binary,
        [
            "-js", "-a", "127.0.0.1", "-p", node.ClientPort.ToString(), "-n", node.Name, "-sd", node.StoreDir,
            "--cluster_name", ClusterName, "--cluster", $"nats://127.0.0.1:{node.RoutePort}", "--routes", routes,
            "-l", Path.Combine(_root, $"{node.Name}.log")
        ])
        {
            UseShellExecute = false
        }) ?? throw new InvalidOperationException($"Could not start {binary}");
    }

    private static async Task WaitForListenerAsync(ClusterNode node)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (DateTime.UtcNow < deadline)
        {
            if (node.Process is not { HasExited: false } process)
            {
                throw new InvalidOperationException(
                    $"nats-server {node.Name} exited (is port {node.ClientPort} or {node.RoutePort} in use?)");
            }

            try
            {
                using var client = new TcpClient();
                await client.ConnectAsync("127.0.0.1", node.ClientPort);
                return;
            }
            catch (SocketException)
            {
                await Task.Delay(100);
            }
        }

        throw new TimeoutException($"nats-server {node.Name} did not listen on port {node.ClientPort}");
    }

    private Task WaitForMetaLeaderAsync() =>
        RetryAsync(async () => await JetStream.GetAccountInfoAsync(), "reach the JetStream meta leader");

    private static async Task<string> WaitForNewLeaderAsync(Func<Task<string?>> leader, string? previous, string what)
    {
        var deadline = DateTime.UtcNow + WaitTimeout;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                if (await leader() is { } current && current != previous)
                {
                    return current;
                }
            }
            catch (NatsException)
            {
            }

            await Task.Delay(50);
        }

        throw new TimeoutException($"No new leader for {what} after {previous} stepped down");
    }

    private static async Task<T> RetryAsync<T>(Func<Task<T>> action, string what)
    {
        var deadline = DateTime.UtcNow + WaitTimeout;
        while (true)
        {
            try
            {
                return await action();
            }
            catch (Exception e) when (e is NatsException or InvalidOperationException && DateTime.UtcNow < deadline)
            {
                await Task.Delay(200);
            }
        }
    }

    private const int SigTerm = 15;

    [DllImport("libc", EntryPoint = "kill", SetLastError = true)]
    private static extern int Kill(int pid, int signal);
}

public sealed class ClusterNode(string name, int clientPort, int routePort, string storeDir)
{
    public string Name { get; } = name;

    public int ClientPort { get; } = clientPort;

    public int RoutePort { get; } = routePort;

    public string StoreDir { get; } = storeDir;

    public string Url => $"nats://127.0.0.1:{ClientPort}";

    public bool IsRunning => Process is { HasExited: false };

    internal Process? Process { get; set; }
}
