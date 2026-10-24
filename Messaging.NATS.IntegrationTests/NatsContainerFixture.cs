using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace Messaging.NATS.IntegrationTests;

[TestClass]
public static class NatsContainerFixture
{
    private static IContainer? _natsContainer;
    public static string NatsUrl { get; private set; } = null!;

    [AssemblyInitialize]
    public static async Task AssemblySetup(TestContext context)
    {
        // Allow CI/dev environments without Docker to point tests at an existing NATS server
        // (must have JetStream enabled). When unset, we fall back to spinning a container.
        var external = Environment.GetEnvironmentVariable("NATS_TEST_URL");
        if (!string.IsNullOrEmpty(external))
        {
            NatsUrl = external;
            return;
        }

        // NATS_SERVER_VERSION pins the image so the container matches the binary the cluster tests use.
        var version = Environment.GetEnvironmentVariable("NATS_SERVER_VERSION") is { Length: > 0 } v ? v : "2.12";
        _natsContainer = new ContainerBuilder($"nats:{version}-alpine")
            .WithCommand("-js")
            .WithPortBinding(4222, true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Server is ready"))
            .Build();

        await _natsContainer.StartAsync();
        NatsUrl = $"nats://localhost:{_natsContainer.GetMappedPublicPort(4222)}";
    }

    [AssemblyCleanup]
    public static async Task AssemblyTeardown()
    {
        if (_natsContainer != null)
        {
            await _natsContainer.DisposeAsync();
        }
    }
}
