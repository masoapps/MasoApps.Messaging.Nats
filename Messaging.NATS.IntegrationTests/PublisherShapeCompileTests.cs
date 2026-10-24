using System.Linq.Expressions;
using System.Reflection;

namespace Messaging.NATS.IntegrationTests;

/// <summary>
/// Compile-only checks of the IPublisher call shapes downstream code uses, written as the expression trees Moq's
/// Setup and Verify take. An overload with optional parameters would not compile here (CS0854).
/// </summary>
[TestClass]
public class PublisherShapeCompileTests
{
    [TestMethod]
    public void MoqShape_BindsTheLegacyOverload_ThatProductionCodeBindsToo()
    {
        Expression<Func<IPublisher, Task>> verify = p =>
            p.PublishAsync(It.IsAny<string>(), It.IsAny<object>(), It.IsAny<TimeSpan>());
        Expression<Func<IPublisher, Task>> production = p =>
            p.PublishAsync("app.remind", new TestMessage("x"), TimeSpan.FromMinutes(5));
        Expression<Func<IPublisher, Task>> zero = p => p.PublishAsync("app.remind", new TestMessage("x"), TimeSpan.Zero);
        Expression<Func<IPublisher, Task>> named = p => p.PublishAsync("app.remind", new TestMessage("x"), deferredBy: TimeSpan.Zero);

        var legacy = Method(verify);
        Assert.IsFalse(legacy.IsGenericMethod);
        Assert.AreEqual(typeof(Task), legacy.ReturnType, "Moq's .Returns(Task.CompletedTask) needs Task");
        Assert.AreEqual(legacy, Method(production));
        Assert.AreEqual(legacy, Method(zero));
        Assert.AreEqual(legacy, Method(named));
    }

    [TestMethod]
    public void NullThirdArgument_BindsTheGenericOptionsOverload()
    {
        Expression<Func<IPublisher, Task>> call = p => p.PublishAsync("area.report", new TestMessage("x"), null);

        var method = Method(call);
        Assert.IsTrue(method.IsGenericMethod);
        Assert.AreEqual(typeof(PublishOptions), method.GetParameters()[2].ParameterType);
    }

    [TestMethod]
    public void NewOverloads_CompileAsMoqExpressions()
    {
        var expressions = new LambdaExpression[]
        {
            (Expression<Func<IPublisher, Task<PublishResult>>>)(p =>
                p.PublishAsync(It.IsAny<string>(), It.IsAny<TestMessage>())),
            (Expression<Func<IPublisher, Task<PublishResult>>>)(p =>
                p.PublishAsync(It.IsAny<string>(), It.IsAny<TestMessage>(), It.IsAny<CancellationToken>())),
            (Expression<Func<IPublisher, Task<PublishResult>>>)(p =>
                p.PublishAsync(It.IsAny<string>(), It.IsAny<TestMessage>(), It.IsAny<PublishOptions?>())),
            (Expression<Func<IPublisher, Task<PublishResult>>>)(p =>
                p.PublishAsync(It.IsAny<string>(), It.IsAny<TestMessage>(), It.IsAny<PublishOptions?>(), It.IsAny<CancellationToken>())),
            (Expression<Func<IPublisher, Task<ScheduleResult>>>)(p =>
                p.ScheduleAsync(It.IsAny<string>(), It.IsAny<TestMessage>(), It.IsAny<DateTimeOffset>())),
            (Expression<Func<IPublisher, Task<ScheduleResult>>>)(p =>
                p.ScheduleAsync(It.IsAny<string>(), It.IsAny<TestMessage>(), It.IsAny<DateTimeOffset>(), It.IsAny<ScheduleOptions?>())),
            (Expression<Func<IPublisher, Task<ScheduleResult>>>)(p =>
                p.ScheduleAsync(It.IsAny<string>(), It.IsAny<TestMessage>(), It.IsAny<DateTimeOffset>(), It.IsAny<ScheduleOptions?>(),
                    It.IsAny<CancellationToken>())),
            (Expression<Func<IPublisher, Task<bool>>>)(p => p.CancelScheduleAsync(It.IsAny<string>(), It.IsAny<string>())),
            (Expression<Func<IPublisher, Task<bool>>>)(p =>
                p.CancelScheduleAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>())),
        };

        var methods = expressions.Select(e => Method(e).ToString()).ToList();
        Assert.AreEqual(expressions.Length, methods.Distinct().Count(), "Each expression binds its own overload");
    }

    private static MethodInfo Method(LambdaExpression expression)
    {
        var method = ((MethodCallExpression)expression.Body).Method;
        return method.IsGenericMethod ? method.GetGenericMethodDefinition() : method;
    }

    /// <summary>
    /// Stands in for Moq's It, which the test project does not reference.
    /// </summary>
    private static class It
    {
        public static T IsAny<T>() => default!;
    }
}
