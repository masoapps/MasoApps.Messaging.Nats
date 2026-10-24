namespace Messaging.NATS;

/// <summary>
/// NATS subject wildcard logic ("*" matches one token, ">" matches one or more trailing tokens).
/// </summary>
internal static class SubjectMatcher
{
    /// <summary>
    /// True when every subject that <paramref name="filter"/> matches is also matched by <paramref name="pattern"/>.
    /// A literal filter is a subset when the pattern matches it.
    /// </summary>
    public static bool IsSubsetOf(string filter, string pattern)
    {
        var f = filter.Split('.');
        var p = pattern.Split('.');

        for (var i = 0; i < p.Length; i++)
        {
            if (p[i] == ">")
            {
                return i < f.Length;
            }

            if (i >= f.Length)
            {
                return false;
            }

            if (f[i] == ">")
            {
                return false;
            }

            if (p[i] == "*")
            {
                continue;
            }

            if (f[i] == "*" || f[i] != p[i])
            {
                return false;
            }
        }

        return f.Length == p.Length;
    }

    /// <summary>
    /// True when at least one subject matches both <paramref name="a"/> and <paramref name="b"/>.
    /// </summary>
    public static bool Overlaps(string a, string b)
    {
        var x = a.Split('.');
        var y = b.Split('.');

        for (var i = 0; i < Math.Min(x.Length, y.Length); i++)
        {
            if (x[i] == ">" || y[i] == ">")
            {
                return true;
            }

            if (x[i] != "*" && y[i] != "*" && x[i] != y[i])
            {
                return false;
            }
        }

        return x.Length == y.Length;
    }

    /// <summary>
    /// True when some pattern in <paramref name="patterns"/> covers <paramref name="filter"/> on its own.
    /// </summary>
    public static bool IsCoveredBy(string filter, IEnumerable<string> patterns) =>
        patterns.Any(p => IsSubsetOf(filter, p));

    /// <summary>
    /// True when <paramref name="subject"/> is a valid subject or filter: non-empty tokens, no whitespace,
    /// and ">" only as the last token.
    /// </summary>
    public static bool IsValid(string subject)
    {
        if (string.IsNullOrEmpty(subject) || subject.Any(char.IsWhiteSpace))
        {
            return false;
        }

        var tokens = subject.Split('.');
        for (var i = 0; i < tokens.Length; i++)
        {
            var t = tokens[i];
            if (t.Length == 0)
            {
                return false;
            }

            if (t.Length > 1 && (t.Contains('*') || t.Contains('>')))
            {
                return false;
            }

            if (t == ">" && i != tokens.Length - 1)
            {
                return false;
            }
        }

        return true;
    }
}
