using Grpc.Net.Client;
using GrpcAuthorServer;

using var channel = GrpcChannel.ForAddress("https://localhost:7039"); // ПРИМЕР АДРЕСА!

var client = new AuthorService.AuthorServiceClient(channel);

Console.WriteLine("Введите ID автора для запроса:");
var authorId = Console.ReadLine();

var reply = await client.GetAuthorAsync(new GetAuthorRequest { AuthorId = authorId });

Console.WriteLine("Ответ от сервера:");
Console.WriteLine($"ID: {reply.AuthorId}");
Console.WriteLine($"Имя: {reply.Name}");
Console.WriteLine($"Биография: {reply.Biography}");

Console.WriteLine("Нажмите любую клавишу для выхода...");
Console.ReadKey();