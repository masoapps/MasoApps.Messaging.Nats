using NATS.Client.JetStream.Models;

namespace Messaging.NATS.IntegrationTests;

[TestClass]
public class LegacyOptionsCompileTests
{
    // Compile-only: the 4.x option and StreamDefinition initialisers, MaxAge as int seconds.
    [TestMethod]
    public void FourXInitialisers_StillCompile()
    {
        var options = new NatsMessagingOptions
        {
            NatsUrl = "nats://localhost:4222",
            CredsFilePath = null,
            Assemblies = [typeof(LegacyOptionsCompileTests).Assembly],
            DefaultQueueGroup = "default-worker",
            StreamDefinitions = new List<StreamDefinition>
            {
                new StreamDefinition
                {
                    Name = "wh",
                    Subjects = new[] { "wh.>" },
                    Retention = StreamConfigRetention.Workqueue,
                    MaxAge = 3600,
                    MaxBytes = 1024
                }
            }
        };

        Assert.AreEqual(3600, options.StreamDefinitions[0].MaxAge);
        Assert.AreEqual(TimeSpan.FromHours(1), options.StreamDefinitions[0].ToStreamConfig().MaxAge);
    }
}
