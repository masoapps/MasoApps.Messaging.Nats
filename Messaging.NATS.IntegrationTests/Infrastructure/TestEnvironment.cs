using System.Net;
using System.Net.Sockets;

namespace Messaging.NATS.IntegrationTests;

/// <summary>
/// Ports for tests that start their own servers. A variable that is set wins. When it is unset and nats-server can
/// be found (NATS_SERVER_BIN, or on PATH), free ports are picked, so the default run covers these tests too.
/// Otherwise the test ends Inconclusive, or fails when NATS_REQUIRE_SERVER_TESTS=1 (for CI, so the R3 schedule tests
/// cannot silently not run).
/// </summary>
public static class TestEnvironment
{
    public const string SparePortVariable = "NATS_TEST_SPARE_PORT";
    public const string ClusterBasePortVariable = "NATS_CLUSTER_BASE_PORT";
    public const string RequireVariable = "NATS_REQUIRE_SERVER_TESTS";

    /// <summary>
    /// A port with no server on it, which a test may start nats-server on.
    /// </summary>
    public static int RequireSparePort() => RequirePorts(SparePortVariable, 1);

    /// <summary>
    /// The first client port of the test cluster.
    /// </summary>
    public static int RequireClusterBasePort() => RequirePorts(ClusterBasePortVariable, 3);

    /// <summary>
    /// The first of <paramref name="count"/> consecutive free local ports.
    /// </summary>
    public static int FreePorts(int count)
    {
        for (var tries = 0; tries < 50; tries++)
        {
            var first = UnusedPort();
            if (Enumerable.Range(first + 1, count - 1).All(IsFree))
            {
                return first;
            }
        }

        throw new InvalidOperationException($"Found no {count} consecutive free ports");
    }

    /// <summary>
    /// A local port nothing listens on (picked by the OS, then released).
    /// </summary>
    public static int UnusedPort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    private static int RequirePorts(string variable, int count)
    {
        var value = Environment.GetEnvironmentVariable(variable);
        if (int.TryParse(value, out var port))
        {
            return port;
        }

        if (ServerBinaryAvailable())
        {
            return FreePorts(count);
        }

        var message = $"Set {variable}, or put nats-server on PATH (or in NATS_SERVER_BIN), to run this test";
        if (Environment.GetEnvironmentVariable(RequireVariable) == "1")
        {
            Assert.Fail(message);
        }

        Assert.Inconclusive(message);
        return 0;
    }

    private static bool ServerBinaryAvailable()
    {
        var binary = Environment.GetEnvironmentVariable("NATS_SERVER_BIN");
        if (!string.IsNullOrEmpty(binary))
        {
            return File.Exists(binary);
        }

        var path = Environment.GetEnvironmentVariable("PATH") ?? string.Empty;
        return path.Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries)
            .Any(dir => File.Exists(Path.Combine(dir, "nats-server")) || File.Exists(Path.Combine(dir, "nats-server.exe")));
    }

    private static bool IsFree(int port)
    {
        try
        {
            using var listener = new TcpListener(IPAddress.Loopback, port);
            listener.Start();
            return true;
        }
        catch (SocketException)
        {
            return false;
        }
    }
}
