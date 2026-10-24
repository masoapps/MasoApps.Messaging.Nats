using System.Diagnostics;
using System.Net.Sockets;

namespace Messaging.NATS.IntegrationTests;

/// <summary>
/// A nats-server process with JetStream and its own store directory. The binary is NATS_SERVER_BIN, or
/// nats-server on PATH.
/// </summary>
public sealed class NatsServerProcess : IAsyncDisposable
{
    private readonly Process _process;
    private readonly string _storeDir;
    private bool _disposed;

    private NatsServerProcess(Process process, string storeDir, int port)
    {
        _process = process;
        _storeDir = storeDir;
        Port = port;
    }

    public int Port { get; }

    public string Url => $"nats://127.0.0.1:{Port}";

    /// <param name="port">The client port.</param>
    /// <param name="timeout">How long to wait for the server to listen.</param>
    /// <param name="jetStream">False starts the server without JetStream, so every JetStream API call gets no
    /// responders.</param>
    /// <param name="arguments">Extra nats-server arguments, for example "--user", "u", "--pass", "p".</param>
    public static async Task<NatsServerProcess> StartAsync(int port, TimeSpan? timeout = null, bool jetStream = true,
        params string[] arguments)
    {
        var storeDir = Path.Combine(Path.GetTempPath(), TestNames.Unique("nats-store"));
        Directory.CreateDirectory(storeDir);

        var binary = Environment.GetEnvironmentVariable("NATS_SERVER_BIN") ?? "nats-server";
        string[] js = jetStream ? ["-js", "-sd", storeDir] : [];
        var process = Process.Start(new ProcessStartInfo(binary, [.. js, "-a", "127.0.0.1", "-p", port.ToString(), .. arguments])
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false
        }) ?? throw new InvalidOperationException($"Could not start {binary}");
        process.OutputDataReceived += (_, _) => { };
        process.ErrorDataReceived += (_, _) => { };
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();

        var server = new NatsServerProcess(process, storeDir, port);
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(10));
        while (DateTime.UtcNow < deadline)
        {
            if (process.HasExited)
            {
                throw new InvalidOperationException($"nats-server exited with code {process.ExitCode}");
            }

            try
            {
                using var client = new TcpClient();
                await client.ConnectAsync("127.0.0.1", port);
                return server;
            }
            catch (SocketException)
            {
                await Task.Delay(100);
            }
        }

        await server.DisposeAsync();
        throw new TimeoutException($"nats-server did not listen on port {port}");
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        if (!_process.HasExited)
        {
            // nats-server has no child processes. Kill(entireProcessTree: true) sends SIGSTOP before SIGKILL on
            // Unix, and a server left stopped resumes on SIGCONT when the test host exits.
            _process.Kill();
            await _process.WaitForExitAsync();
        }

        _process.Dispose();

        try
        {
            Directory.Delete(_storeDir, recursive: true);
        }
        catch (IOException)
        {
        }
    }
}
