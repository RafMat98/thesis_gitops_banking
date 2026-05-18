# Stage 1: Build με Maven
FROM maven:3.9.6-eclipse-temurin-17 AS build
COPY . /app
WORKDIR /app
RUN mvn clean package -DskipTests

# Stage 2: Runtime image
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app

# Create non-root user for security (Kubernetes Security Best Practices)
RUN adduser --system --group spring
USER spring

# Copy the jar file from the Stage 1 to the runtime image-
COPY --from=build /app/target/*.jar app.jar

EXPOSE 8080

# Use ENTRYPOINT to run the application, allowing for better signal handling and compatibility with Kubernetes
ENTRYPOINT ["java", "-jar", "app.jar"]