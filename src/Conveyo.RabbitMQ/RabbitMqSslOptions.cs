using System.Security;

namespace Conveyo.RabbitMQ;

public sealed record RabbitMqSslOptions
{
    public System.Security.Authentication.SslProtocols Protocol { get; init; }

    public string? ServerName { get; init; }

    public string? CertificatePath { get; init; }

    public string? CertificatePassphrase { get; init; }

    public System.Security.Cryptography.X509Certificates.X509Certificate? Certificate { get; init; }

    public bool UseCertificateAsAuthenticationIdentity { get; init; }

    public System.Net.Security.SslPolicyErrors AcceptablePolicyErrors { get; init; }

    public System.Net.Security.LocalCertificateSelectionCallback? CertificateSelectionCallback { get; init; }

    public System.Net.Security.RemoteCertificateValidationCallback? CertificateValidationCallback { get; init; }
}
