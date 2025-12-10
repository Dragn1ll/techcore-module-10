using Library.Contracts.Auth.Request;
using Library.Domain.Abstractions.Storage;
using Library.Domain.Identity;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System.Security.Claims;

namespace Library.Controllers.Api;

[ApiController]
[Route("api")]
public sealed class AuthController : Controller
{
    private readonly UserManager<ApplicationUser> _userManager;
    private readonly SignInManager<ApplicationUser> _signInManager;
    private readonly IJwtWorker _jwtWorker;
    private readonly RoleManager<IdentityRole> _roleManager;
    private readonly ILogger<AuthController> _logger;

    private const string AdminRole = "Admin"; 
    private const string UserRole = "User";

    public AuthController(
        UserManager<ApplicationUser> userManager, SignInManager<ApplicationUser> signInManager,
        IJwtWorker jwtWorker, RoleManager<IdentityRole> roleManager,
        ILogger<AuthController> logger)
    {
        _userManager = userManager;
        _signInManager = signInManager;
        _jwtWorker = jwtWorker;
        _roleManager = roleManager;
        _logger = logger;
    }

    [HttpPost("register")]
    public async Task<IActionResult> Register([FromBody] RegisterRequest request)
    {
        _logger.LogInformation(
            "Запрос регистрации пользователя: имя = {UserName}, email = {Email}",
            request.UserName, request.Email);

        var user = new ApplicationUser
        {
            UserName = request.UserName,
            Email = request.Email,
            DateOfBirth = request.BirthDay
        };

        var result = await _userManager.CreateAsync(user, request.Password);

        if (!result.Succeeded)
        {
            _logger.LogWarning(
                "Регистрация пользователя {UserName} не удалась. Ошибки: {Errors}",
                request.UserName,
                string.Join("; ", result.Errors.Select(e => $"{e.Code}: {e.Description}")));

            return BadRequest(result.Errors);
        }

        _logger.LogInformation(
            "Пользователь {UserName} успешно создан. Назначение роли...",
            request.UserName);

        await CreateRoles();

        if (string.Equals(request.UserName, AdminRole, StringComparison.CurrentCultureIgnoreCase))
        {
            await _userManager.AddToRoleAsync(user, "Admin");
            _logger.LogInformation(
                "Пользователю {UserName} назначена роль Admin",
                request.UserName);
        }
        else
        {
            await _userManager.AddToRoleAsync(user, "User");
            _logger.LogInformation(
                "Пользователю {UserName} назначена роль User",
                request.UserName);
        }

        return Ok(new { Message = "Пользователь успешно создан" });
    }

    [HttpPost("login")]
    public async Task<IActionResult> Login(LoginRequest dto)
    {
        _logger.LogInformation(
            "Попытка входа: email = {Email}",
            dto.Email);

        var user = await _userManager.FindByEmailAsync(dto.Email);

        if (user == null)
        {
            _logger.LogWarning(
                "Попытка входа с несуществующим email: {Email}",
                dto.Email);

            return NotFound(new { Message = "Failure" });
        }

        var result = await _signInManager.CheckPasswordSignInAsync(
            user,
            dto.Password,
            lockoutOnFailure: false);

        if (!result.Succeeded)
        {
            _logger.LogWarning(
                "Неудачная попытка входа для пользователя {UserId} ({Email})",
                user.Id, dto.Email);

            return Unauthorized(new { Message = "Failure" });
        }

        var token = _jwtWorker.GenerateToken(user);

        _logger.LogInformation(
            "Пользователь {UserId} ({Email}) успешно вошёл в систему",
            user.Id, dto.Email);

        return Ok(new { access_token = token });
    }

    [Authorize]
    [HttpGet("profile")]
    public IActionResult GetProfile()
    {
        var userName = User.Identity?.Name;
        var userId = User.FindFirst(ClaimTypes.NameIdentifier)?.Value;

        _logger.LogInformation(
            "Запрос профиля для пользователя: Id = {UserId}, Name = {UserName}",
            userId, userName);

        if (string.IsNullOrEmpty(userId))
        {
            _logger.LogWarning(
                "Не удалось получить идентификатор пользователя из Claims при запросе профиля");

            return Unauthorized();
        }

        return Ok(new
        {
            UserName = userName,
            UserId = userId
        });
    }

    private async Task CreateRoles()
    {
        _logger.LogDebug("Проверка наличия роли {Role}", AdminRole);

        if (!await _roleManager.RoleExistsAsync(AdminRole))
        {
            _logger.LogInformation(
                "Роль {Role} не найдена. Создаем...",
                AdminRole);

            var result = await _roleManager.CreateAsync(new IdentityRole(AdminRole));

            if (!result.Succeeded)
            {
                _logger.LogWarning(
                    "Не удалось создать роль {Role}. Ошибки: {Errors}",
                    AdminRole,
                    string.Join("; ", result.Errors.Select(e => $"{e.Code}: {e.Description}")));
            }
            else
            {
                _logger.LogInformation(
                    "Роль {Role} успешно создана",
                    AdminRole);
            }
        }
        else
        {
            _logger.LogDebug("Роль {Role} уже существует", AdminRole);
        }

        _logger.LogDebug("Проверка наличия роли {Role}", UserRole);

        if (!await _roleManager.RoleExistsAsync(UserRole))
        {
            _logger.LogInformation(
                "Роль {Role} не найдена. Создаем...",
                UserRole);

            var result = await _roleManager.CreateAsync(new IdentityRole(UserRole));

            if (!result.Succeeded)
            {
                _logger.LogWarning(
                    "Не удалось создать роль {Role}. Ошибки: {Errors}",
                    UserRole,
                    string.Join("; ", result.Errors.Select(e => $"{e.Code}: {e.Description}")));
            }
            else
            {
                _logger.LogInformation(
                    "Роль {Role} успешно создана",
                    UserRole);
            }
        }
        else
        {
            _logger.LogDebug("Роль {Role} уже существует", UserRole);
        }
    }
}