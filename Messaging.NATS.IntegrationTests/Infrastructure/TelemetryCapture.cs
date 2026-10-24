using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;

namespace Messaging.NATS.IntegrationTests;

/// <summary>
/// Records every log entry of one host. Add it with <c>services.AddSingleton&lt;ILoggerProvider&gt;(capture)</c>.
/// </summary>
public sealed class LogCapture : ILoggerProvider
{
    private readonly ConcurrentQueue<LogEntry> _entries = new();

    public IReadOnlyList<LogEntry> Entries => _entries.ToList();

    public ILogger CreateLogger(string categoryName) => new Logger(categoryName, _entries);

    public async Task<LogEntry> WaitForAsync(Func<LogEntry, bool> match, TimeSpan timeout, string description)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            var found = _entries.FirstOrDefault(match);
            if (found != null)
            {
                return found;
            }

            await Task.Delay(50);
        }

        Assert.Fail($"No log entry {description} within {timeout}");
        return null!;
    }

    public void Dispose()
    {
    }

    private sealed class Logger(string category, ConcurrentQueue<LogEntry> entries) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) =>
            entries.Enqueue(new LogEntry(category, logLevel, formatter(state, exception), exception));
    }
}

public sealed record LogEntry(string Category, LogLevel Level, string Message, Exception? Exception);

/// <summary>
/// Records every measurement of the library's meter while it lives. Measurements from parallel tests arrive too,
/// so filter by a tag that is unique to the test.
/// </summary>
public sealed class MetricCapture : IDisposable
{
    private readonly MeterListener _listener = new();
    private readonly ConcurrentQueue<Measurement> _measurements = new();

    public MetricCapture()
    {
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == NatsMessagingTelemetry.SourceName)
            {
                listener.EnableMeasurementEvents(instrument);
            }
        };
        _listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) => Add(instrument, value, tags));
        _listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => Add(instrument, value, tags));
        _listener.SetMeasurementEventCallback<int>((instrument, value, tags, _) => Add(instrument, value, tags));
        _listener.Start();
    }

    public IReadOnlyList<Measurement> Measurements => _measurements.ToList();

    /// <summary>
    /// Reads the observable instruments (the orphans gauge) now.
    /// </summary>
    public void Observe() => _listener.RecordObservableInstruments();

    public IReadOnlyList<Measurement> Of(string instrument, string tag, string value) =>
        Measurements.Where(m => m.Instrument == instrument && m.Tag(tag) == value).ToList();

    public async Task<IReadOnlyList<Measurement>> WaitForAsync(string instrument, Func<Measurement, bool> match,
        TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            var found = Measurements.Where(m => m.Instrument == instrument && match(m)).ToList();
            if (found.Count > 0)
            {
                return found;
            }

            await Task.Delay(50);
        }

        Assert.Fail($"No {instrument} measurement matched within {timeout}");
        return [];
    }

    public void Dispose() => _listener.Dispose();

    private void Add(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags) =>
        _measurements.Enqueue(new Measurement(instrument.Name, value,
            tags.ToArray().ToDictionary(t => t.Key, t => t.Value?.ToString())));

    public sealed record Measurement(string Instrument, double Value, IReadOnlyDictionary<string, string?> Tags)
    {
        public string? Tag(string name) => Tags.GetValueOrDefault(name);
    }
}

/// <summary>
/// Records every stopped activity of the library's source and of <see cref="TestSource"/>.
/// </summary>
public sealed class ActivityCapture : IDisposable
{
    private const string TestSourceName = "Messaging.NATS.IntegrationTests";

    public static readonly ActivitySource TestSource = new(TestSourceName);

    private readonly ActivityListener _listener;
    private readonly ConcurrentQueue<Activity> _stopped = new();

    public ActivityCapture()
    {
        _listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name is NatsMessagingTelemetry.SourceName or TestSourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => _stopped.Enqueue(activity)
        };
        ActivitySource.AddActivityListener(_listener);
    }

    public IReadOnlyList<Activity> Stopped => _stopped.ToList();

    public async Task<Activity> WaitForAsync(Func<Activity, bool> match, TimeSpan timeout, string description)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            var found = _stopped.FirstOrDefault(match);
            if (found != null)
            {
                return found;
            }

            await Task.Delay(50);
        }

        Assert.Fail($"No activity {description} within {timeout}");
        return null!;
    }

    public void Dispose() => _listener.Dispose();
}

public static class ActivityExtensions
{
    public static string? Tag(this Activity activity, string name) => activity.GetTagItem(name)?.ToString();
}
