using Newtonsoft.Json;

namespace RQLite.Sharp.Db
{
    /// <summary>
    /// Result represents the outcome of an operation that changes rows.
    /// </summary>
    public class Result
    {
        [JsonProperty("last_insert_id")]
        public long LastInsertID { get; set; }

        [JsonProperty("rows_affected")]
        public long RowsAffected { get; set; }

        [JsonProperty("error")]
        public string Error { get; set; }

        [JsonProperty("time")]
        public double Time { get; set; }

    }
}