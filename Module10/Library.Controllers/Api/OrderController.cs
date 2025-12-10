using Library.Contracts.Orders.Request;
using Library.Contracts.RabbitMq;
using MassTransit;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace Library.Controllers.Api;

[ApiController]
[Route("api/[controller]")]
public class OrderController : ControllerBase
{
    private readonly IBus _bus;
    private readonly ILogger<OrderController> _logger;

    public OrderController(IBus bus, ILogger<OrderController> logger)
    {
        _bus = bus;
        _logger = logger;
    }

    [HttpPost]
    public async Task<IActionResult> SubmitOrder([FromBody] OrderRequest request)
    {
        if (!ModelState.IsValid)
        {
            return BadRequest(ModelState);
        }

        var orderId = Guid.NewGuid();
        
        var command = new SubmitOrderCommand(
            orderId,
            request.Items.Select(i => new LineItem(i.BookId, i.Quantity)).ToList()
        );

        await _bus.Publish(command);
        
        _logger.LogInformation("Опубликована команда SubmitOrderCommand для заказа {OrderId}", orderId);

        return Accepted(new { OrderId = orderId });
    }
}