using Library.Contracts.RabbitMq;
using Library.Data.PostgreSql;
using Library.Data.PostgreSql.Entities;
using MassTransit;
using Microsoft.EntityFrameworkCore;

namespace OrderWorkerService.Consumers;

public class SubmitOrderConsumer : IConsumer<SubmitOrderCommand>
{
    private readonly ILogger<SubmitOrderConsumer> _logger;
    private readonly BookContext _context;

    public SubmitOrderConsumer(ILogger<SubmitOrderConsumer> logger, BookContext context)
    {
        _logger = logger;
        _context = context;
    }

    public async Task Consume(ConsumeContext<SubmitOrderCommand> context)
    {
        var command = context.Message;
        
        _logger.LogInformation(
            "Получена команда на обработку заказа {OrderId}",
            command.OrderId
        );
        
        throw new InvalidOperationException("Не удалось подключиться к базе данных!");

        if (await _context.Orders.AnyAsync(o => o.Id == command.OrderId))
        {
            _logger.LogWarning("Заказ {OrderId} уже существует. Сообщение проигнорировано.", command.OrderId);
            return;
        }

        var order = new OrderEntity
        {
            Id = command.OrderId,
            OrderDate = DateTime.UtcNow,
            Items = command.Items.Select(i => new OrderItemEntity
            {
                BookId = i.BookId,
                Quantity = i.Quantity
            }).ToList()
        };

        _context.Orders.Add(order);
        await _context.SaveChangesAsync(context.CancellationToken); 
        
        _logger.LogInformation("Заказ {OrderId} успешно сохранен в базу данных!", command.OrderId);
    }
}