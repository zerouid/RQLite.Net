namespace RQLite.Sharp.Store
{
    /// <summary>
    /// ClusterState defines the possible Raft states the current node can be in
    /// </summary>
    public enum ClusterState
    {
        Leader,
        Follower,
        Candidate,
        Shutdown,
        Unknown,

    }
}