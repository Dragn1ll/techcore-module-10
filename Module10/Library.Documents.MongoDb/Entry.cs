using Library.Documents.MongoDb.Repositories;
using Library.Domain.Abstractions.Storage;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver;

namespace Library.Documents.MongoDb;

public static class Entry
{
    public static IServiceCollection AddMongoDb(this IServiceCollection serviceCollection, IConfiguration configuration)
    {
        BsonSerializer.RegisterSerializer(new GuidSerializer(GuidRepresentation.Standard));

        var mongoClient = new MongoClient("mongodb://mongo:27017");
        serviceCollection.AddSingleton<IMongoClient>(mongoClient);

        serviceCollection.AddSingleton<IReviewRepository, ReviewRepository>();

        return serviceCollection;
    }
}
