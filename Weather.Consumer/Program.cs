using Confluent.Kafka;
using Newtonsoft.Json;

var config = new ConsumerConfig
{
    GroupId = "weather-consumer-group",
    BootstrapServers = "localhost:9092",
    AutoOffsetReset = AutoOffsetReset.Earliest
};

var consumer = new ConsumerBuilder<Null, string>(config).Build();

consumer.Subscribe("weather-topic");

CancellationTokenSource tokenSource = new();

try
{
    while (true)
    {
        var response = consumer.Consume(tokenSource.Token);
        if(response.Message != null)
        {
            var weather = JsonConvert.DeserializeObject<Weather>(response.Message!.Value);
            Console.WriteLine($"Stae: {weather!.State}, - Temp: {weather.Temarature}");
        }
    }
}
catch (System.Exception)
{
    
    throw;
}

public record Weather(string State, int Temarature);