namespace Conveyo.Test;

[TestFixture]
public class MessageDataTests
{
    [Test]
    public void Constructor_WithAddress_HasValueFalse()
    {
        var address = new Uri("pgbin://md/files/0194ad8f-61a2-7f28-9001-111111111111");
        var messageData = new MessageData<string>(address);

        Assert.That(messageData.Address, Is.EqualTo(address));
        Assert.That(messageData.HasValue, Is.False);
        Assert.That(messageData.Value, Is.Null);
    }

    [Test]
    public void Constructor_NullAddress_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => _ = new MessageData<string>(null!));
    }

    [Test]
    public void InternalHydrationConstructor_PopulatesAddressAndValue()
    {
        var address = new Uri("pgbin://md/files/0194ad8f-61a2-7f28-9001-222222222222");
        var instance = new MessageData<string>(address, "hello");

        Assert.That(instance.Address, Is.EqualTo(address));
        Assert.That(instance.HasValue, Is.True);
        Assert.That(instance.Value, Is.EqualTo("hello"));
    }
}
