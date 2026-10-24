using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Messaging.NATS.IntegrationTests;

[TestClass]
public class NatsErrorClassifierTests
{
    [TestMethod]
    [DataRow(10012, 500)]
    [DataRow(10052, 500)]
    [DataRow(10058, 400)]
    [DataRow(10065, 400)]
    [DataRow(10069, 500)]
    [DataRow(10093, 400)]
    [DataRow(10094, 400)]
    [DataRow(10099, 400)]
    [DataRow(10100, 400)]
    [DataRow(10101, 400)]
    [DataRow(10116, 400)]
    [DataRow(10136, 400)]
    [DataRow(10138, 400)]
    [DataRow(10139, 400)]
    [DataRow(10188, 400)]
    [DataRow(10190, 400)]
    public void PermanentCodes_ArePermanent_WhateverTheHttpCode(int errCode, int httpCode)
    {
        Assert.AreEqual(ErrorClass.Permanent, NatsErrorClassifier.Classify(ApiError(errCode, httpCode)));
    }

    [TestMethod]
    [DataRow(10004)]
    [DataRow(10008)]
    [DataRow(10010)]
    [DataRow(10023)]
    [DataRow(10035)]
    [DataRow(10036)]
    [DataRow(10039)]
    [DataRow(10063)]
    [DataRow(10075)]
    [DataRow(10076)]
    [DataRow(10077)]
    [DataRow(10163)]
    public void Http503Codes_AreTransient(int errCode)
    {
        Assert.AreEqual(ErrorClass.Transient, NatsErrorClassifier.Classify(ApiError(errCode, 503)));
    }

    [TestMethod]
    [DataRow(10059)]
    [DataRow(10014)]
    public void NotFoundCodes_AreTransient(int errCode)
    {
        Assert.AreEqual(ErrorClass.Transient, NatsErrorClassifier.Classify(ApiError(errCode, 404)));
    }

    [TestMethod]
    public void AConnectionInTheFailedState_IsPermanent_OtherNatsErrorsAreTransient()
    {
        // NATS.Net never leaves the Failed state, so retrying it can never succeed.
        Assert.AreEqual(ErrorClass.Permanent,
            NatsErrorClassifier.Classify(new NatsConnectionFailedException("Connection is in failed state")));
        Assert.AreEqual(ErrorClass.Transient, NatsErrorClassifier.Classify(new NatsException("connection lost")));
    }

    [TestMethod]
    public void UnknownCode_IsUnknown_EvenWithHttp400()
    {
        Assert.AreEqual(ErrorClass.Unknown, NatsErrorClassifier.Classify(ApiError(99999, 400)));
    }

    [TestMethod]
    public void UnknownCode_IsUnknown_EvenWithHttp500()
    {
        Assert.AreEqual(ErrorClass.Unknown, NatsErrorClassifier.Classify(ApiError(99998, 500)));
    }

    [TestMethod]
    public void ConfigurationException_IsPermanent()
    {
        Assert.AreEqual(ErrorClass.Permanent,
            NatsErrorClassifier.Classify(new NatsMessagingConfigurationException("bad stream")));
    }

    [TestMethod]
    public void ConnectionAndTimeoutErrors_AreTransient()
    {
        Exception[] errors =
        [
            new TimeoutException(),
            new NatsNoRespondersException(),
            new NatsNoReplyException(),
            new NatsException("can not connect uris"),
            new NatsJSApiNoResponseException(),
            new OperationCanceledException(),
            new AggregateException(new TimeoutException())
        ];

        foreach (var error in errors)
        {
            Assert.AreEqual(ErrorClass.Transient, NatsErrorClassifier.Classify(error), error.GetType().Name);
        }
    }

    [TestMethod]
    public void OtherExceptions_AreUnknown()
    {
        Assert.AreEqual(ErrorClass.Unknown, NatsErrorClassifier.Classify(new InvalidOperationException()));
    }

    [TestMethod]
    public void EveryTableCode_IsInExactlyOneSet()
    {
        Assert.IsFalse(NatsErrorClassifier.PermanentCodes.Overlaps(NatsErrorClassifier.TransientCodes));
    }

    [TestMethod]
    public void Backoff_DoublesFromOneSecond_CappedAtThirtySeconds_WithJitter()
    {
        for (var i = 0; i < 50; i++)
        {
            var first = RetryLoop.Backoff(1);
            var third = RetryLoop.Backoff(3);
            var late = RetryLoop.Backoff(20);

            Assert.IsTrue(first >= TimeSpan.FromSeconds(0.8) && first <= TimeSpan.FromSeconds(1.2), first.ToString());
            Assert.IsTrue(third >= TimeSpan.FromSeconds(3.2) && third <= TimeSpan.FromSeconds(4.8), third.ToString());
            Assert.IsTrue(late >= TimeSpan.FromSeconds(24) && late <= TimeSpan.FromSeconds(36), late.ToString());
        }
    }

    private static NatsJSApiException ApiError(int errCode, int httpCode) =>
        new(new ApiError { Code = httpCode, ErrCode = errCode, Description = $"error {errCode}" });
}
