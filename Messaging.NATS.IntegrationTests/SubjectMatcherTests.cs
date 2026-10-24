namespace Messaging.NATS.IntegrationTests;

[TestClass]
public class SubjectMatcherTests
{
    [TestMethod]
    [DataRow("dlq.>", "dlq.>", true)]
    [DataRow("dlq.orders.created", "dlq.>", true)]
    [DataRow("dlq.>", ">", true)]
    [DataRow("dlq.a", "dlq.*", true)]
    [DataRow("dlq.*", "dlq.>", true)]
    [DataRow("dlq.>", "dlq.*", false)]
    [DataRow("dlq.a.b", "dlq.*", false)]
    [DataRow("dlq", "dlq.>", false)]
    [DataRow("dead.>", "dlq.>", false)]
    [DataRow("dlq.*", "dlq.a", false)]
    public void IsSubsetOf(string filter, string pattern, bool expected)
    {
        Assert.AreEqual(expected, SubjectMatcher.IsSubsetOf(filter, pattern));
    }

    [TestMethod]
    [DataRow("a.>", "a.b", true)]
    [DataRow("a.*", "a.b", true)]
    [DataRow("a.*", "*.b", true)]
    [DataRow(">", "_sched.S.>", true)]
    [DataRow("a.b", "a.c", false)]
    [DataRow("a.*", "a.b.c", false)]
    [DataRow("timers.>", "_sched.TIMERS.>", false)]
    public void Overlaps(string a, string b, bool expected)
    {
        Assert.AreEqual(expected, SubjectMatcher.Overlaps(a, b));
        Assert.AreEqual(expected, SubjectMatcher.Overlaps(b, a));
    }

    [TestMethod]
    [DataRow("a.b", true)]
    [DataRow("a.*.c", true)]
    [DataRow("a.>", true)]
    [DataRow("", false)]
    [DataRow("a..b", false)]
    [DataRow("a.>.b", false)]
    [DataRow("a.b*", false)]
    [DataRow("a b", false)]
    public void IsValid(string subject, bool expected)
    {
        Assert.AreEqual(expected, SubjectMatcher.IsValid(subject));
    }
}
