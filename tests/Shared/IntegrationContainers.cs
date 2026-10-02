using DotNet.Testcontainers.Containers;

internal static class IntegrationContainers
{
    private static readonly TimeSpan StartupTimeout = TimeSpan.FromMinutes(2);
    private static readonly TimeSpan DiagnosticsTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Labels for every integration container. scripts/test-integration.sh supplies the run id
    /// so it can remove this run's containers when the test process dies before teardown.
    /// </summary>
    public static IReadOnlyDictionary<string, string> RunLabels { get; } =
        Environment.GetEnvironmentVariable("CONVEYO_TEST_RUN_ID") is { Length: > 0 } runId
            ? new Dictionary<string, string> { ["conveyo.test-run"] = runId }
            : new Dictionary<string, string>();

    /// <summary>
    /// Starts the container and waits for readiness. A failed or timed-out start disposes the
    /// container and reports its state and logs.
    /// </summary>
    public static async Task StartAsync(IContainer container)
    {
        using var timeout = new CancellationTokenSource(StartupTimeout);
        try
        {
            await container.StartAsync(timeout.Token);
        }
        catch (Exception exception)
        {
            var diagnostics = await DescribeAsync(container);
            await container.DisposeAsync();
            throw new InvalidOperationException(
                $"Container '{container.Image.FullName}' failed to start (startup timeout: {StartupTimeout.TotalSeconds:0} seconds). {diagnostics}",
                exception);
        }
    }

    private static async Task<string> DescribeAsync(IContainer container)
    {
        var runtime = Environment.GetEnvironmentVariable("DOCKER_HOST") ?? "default endpoint";
        try
        {
            using var timeout = new CancellationTokenSource(DiagnosticsTimeout);
            var (stdout, stderr) = await container.GetLogsAsync(ct: timeout.Token);
            return $"Container runtime: {runtime}. State: {container.State}.{Environment.NewLine}"
                + $"stdout:{Environment.NewLine}{stdout}{Environment.NewLine}stderr:{Environment.NewLine}{stderr}";
        }
        catch (Exception exception)
        {
            return $"Container runtime: {runtime}. No container logs are available: {exception.Message}";
        }
    }
}
