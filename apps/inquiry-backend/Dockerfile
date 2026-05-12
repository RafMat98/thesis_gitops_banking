# Stage 1: Build με Maven
FROM maven:3.9.6-eclipse-temurin-17 AS build
COPY . /app
WORKDIR /app
RUN mvn clean package -DskipTests

# Stage 2: Runtime image
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app

# Δημιουργία μη-προνομιούχου χρήστη για ασφάλεια (Kubernetes Security Best Practices)
RUN adduser --system --group spring
USER spring

# Αντιγραφή του τελικού jar από το Stage 1
COPY --from=build /app/target/*.jar app.jar
# COPY --from=build /app/certs /app/certs #Only if you have certs to copy in local testing, otherwise the volume mount in k8s will handle it

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]