namespace Conveyo.Test;

[TestFixture]
public class MessageDataTests
{
    [Test]
    public void Constructor_WithAddress_HasValueFalse()
    {
        // Arrange
        var address = new Uri("pgbin://md/files/0194ad8f-61a2-7f28-9001-111111111111");

        // Act
        var messageData = new MessageData<string>(address);

        // Assert
        messageData.Address.ShouldBe(address);
        messageData.HasValue.ShouldBeFalse();
        messageData.Value.ShouldBeNull();
    }

    [Test]
    public void Constructor_NullAddress_Throws()
    {
        // Arrange
        Uri address = null!;

        // Act
        var operation = () => new MessageData<string>(address);

        // Assert
        Should.Throw<ArgumentNullException>(operation);
    }

    [Test]
    public void InternalHydrationConstructor_PopulatesAddressAndValue()
    {
        // Arrange
        var address = new Uri("pgbin://md/files/0194ad8f-61a2-7f28-9001-222222222222");

        // Act
        var instance = new MessageData<string>(address, "hello");

        // Assert
        instance.Address.ShouldBe(address);
        instance.HasValue.ShouldBeTrue();
        instance.Value.ShouldBe("hello");
    }
}
