using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging.Abstractions;

namespace Conveyo.RabbitMQ.Test.Integration;

[TestFixture]
[Category("Integration")]
public class RabbitMqInitialConnectionIntegrationTests
{
    [Test]
    public async Task StartAsync_Connects_WhenTheBrokerAppearsLate()
    {
        var broker = BrokerFixture.GetOptions("late-broker");

        // Point the bus at a port nothing is listening on yet, then start forwarding to the real broker after a delay.
        using var forwarder = new DelayedForwarder(broker.Host, broker.Port);
        var options = new RabbitMqHostOptions
        {
            ClientName = broker.ClientName,
            Host = "127.0.0.1",
            Port = forwarder.Port,
            VHost = broker.VHost,
            Username = broker.Username,
            Password = broker.Password,
            InitialConnectionRetryDelay = TimeSpan.FromMilliseconds(200),
            InitialConnectionMaxRetryDelay = TimeSpan.FromMilliseconds(400),
            InitialConnectionTimeout = TimeSpan.FromSeconds(30)
        };

        var manager = new RabbitMqConnectionManager(NullLogger.Instance);
        var connecting = manager.StartAsync(options, CancellationToken.None);

        await Task.Delay(TimeSpan.FromSeconds(1));
        forwarder.Start();

        await connecting;

        Assert.Multiple(() =>
        {
            Assert.That(manager.Connection, Is.Not.Null);
            Assert.That(manager.Connection!.IsOpen, Is.True);
        });

        await manager.StopAsync(CancellationToken.None);
    }

    /// <summary>
    /// Owns a port from construction but only accepts and forwards to the broker once
    /// <see cref="Start"/> is called, so connections before that are refused.
    /// </summary>
    private sealed class DelayedForwarder : IDisposable
    {
        private readonly string _targetHost;
        private readonly int _targetPort;
        private readonly CancellationTokenSource _cancellation = new();
        private TcpListener? _listener;

        public DelayedForwarder(string targetHost, int targetPort)
        {
            _targetHost = targetHost;
            _targetPort = targetPort;

            // Reserve a free port, then release it so connections are refused until Start().
            var probe = new TcpListener(IPAddress.Loopback, 0);
            probe.Start();
            Port = ((IPEndPoint)probe.LocalEndpoint).Port;
            probe.Stop();
        }

        public int Port { get; }

        public void Start()
        {
            _listener = new TcpListener(IPAddress.Loopback, Port);
            _listener.Start();
            _ = AcceptAsync(_listener, _cancellation.Token);
        }

        private async Task AcceptAsync(TcpListener listener, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                TcpClient inbound;
                try
                {
                    inbound = await listener.AcceptTcpClientAsync(cancellationToken);
                }
                catch (Exception)
                {
                    return;
                }

                _ = ForwardAsync(inbound, cancellationToken);
            }
        }

        private async Task ForwardAsync(TcpClient inbound, CancellationToken cancellationToken)
        {
            using (inbound)
            using (var outbound = new TcpClient())
            {
                await outbound.ConnectAsync(_targetHost, _targetPort, cancellationToken);

                var inboundStream = inbound.GetStream();
                var outboundStream = outbound.GetStream();

                await Task.WhenAny(
                    inboundStream.CopyToAsync(outboundStream, cancellationToken),
                    outboundStream.CopyToAsync(inboundStream, cancellationToken));
            }
        }

        public void Dispose()
        {
            _cancellation.Cancel();
            _listener?.Stop();
            _cancellation.Dispose();
        }
    }
}
