# syntax=docker/dockerfile:1.7

FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/sdk:10.0-alpine AS build
ARG TARGETARCH

WORKDIR /source

COPY *.csproj .

RUN dotnet restore -a $TARGETARCH

COPY . .

RUN dotnet publish --no-restore \
    -a $TARGETARCH \
    -c Release \
    -o /app \
    /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/runtime:10.0-alpine AS final

WORKDIR /app

COPY --from=build /app .

ENTRYPOINT ["dotnet", "InfinityAI.Maintenance.Worker.dll"]
