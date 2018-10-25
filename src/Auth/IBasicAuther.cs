namespace RQLite.Sharp.Auth
{
    /// <summary>
    /// BasicAuther is the interface an object must support to return basic auth information.
    /// </summary>
    public interface IBasicAuther
    {
        string Username { get; }
        string Password { get; }
    }
}