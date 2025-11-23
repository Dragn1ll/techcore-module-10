FROM mcr.microsoft.com/dotnet/runtime:9.0 AS base
USER $APP_UID
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["OrderWorkerService/OrderWorkerService.csproj", "OrderWorkerService/"]
COPY ["Library.Contracts/Library.Contracts.csproj", "Library.Contracts/"]
COPY ["Library.SharedKernel/Library.SharedKernel.csproj", "Library.SharedKernel/"]
COPY ["Library.Data.PostgreSql/Library.Data.PostgreSql.csproj", "Library.Data.PostgreSql/"]
RUN dotnet restore "OrderWorkerService/OrderWorkerService.csproj"
COPY . .
WORKDIR "/src/OrderWorkerService"
RUN dotnet build "OrderWorkerService.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "OrderWorkerService.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "OrderWorkerService.dll"]
