using System.Diagnostics.CodeAnalysis;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;

namespace Conveyo.RabbitMQ;

public sealed class RabbitMqSslConfigurator
{
    public SslProtocols Protocol { get; set; } = SslProtocols.None;

    public string? ServerName { get; set; }

    public string? CertificatePath { get; set; }

    public string? CertificatePassphrase { get; set; }

    public X509Certificate? Certificate { get; set; }

    public bool UseCertificateAsAuthenticationIdentity { get; set; }

    public LocalCertificateSelectionCallback? CertificateSelectionCallback { get; set; }

    public RemoteCertificateValidationCallback? CertificateValidationCallback { get; set; }

    public SslPolicyErrors AcceptablePolicyErrors { get; private set; }

    public void AllowPolicyErrors(SslPolicyErrors policyErrors) => AcceptablePolicyErrors |= policyErrors;

    public void EnforcePolicyErrors(SslPolicyErrors policyErrors) => AcceptablePolicyErrors &= ~policyErrors;

    /// <summary>
    /// Accepts any server certificate. Use only for local development or trusted internal networks.
    /// </summary>
    [SuppressMessage(
        "Security",
        "S4830:Server certificates should be verified during SSL/TLS connections",
        Justification = "This method is an explicitly named opt-in escape hatch for local development or trusted internal networks.")]
    public void TrustServerCertificate() => CertificateValidationCallback = (_, _, _, _) => true;

    internal RabbitMqSslOptions ToOptions(string fallbackServerName) => new()
    {
        Protocol = Protocol,
        ServerName = ServerName ?? fallbackServerName,
        CertificatePath = CertificatePath,
        CertificatePassphrase = CertificatePassphrase,
        Certificate = Certificate,
        UseCertificateAsAuthenticationIdentity = UseCertificateAsAuthenticationIdentity,
        AcceptablePolicyErrors = AcceptablePolicyErrors,
        CertificateSelectionCallback = CertificateSelectionCallback,
        CertificateValidationCallback = CertificateValidationCallback
    };
}
