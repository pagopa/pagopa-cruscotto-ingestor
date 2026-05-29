# Step 1: Use Maven to build the project
FROM maven:3.9.5-eclipse-temurin-17-alpine@sha256:24a2de7ce5f847dadb9873c1df09f6b4bf64c5c5cd10b516a3a9c45bcda3cadf AS build
WORKDIR /app
COPY . .
RUN mvn clean package -DskipTests

# Step 2: Use Eclipse Temurin JRE to run the application
FROM eclipse-temurin:17-jre-alpine@sha256:dfc2fb89bab269a3f005aef6c58c83b4ddbc4e0819e1f87c20fca52ca04e26e8
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar

# Expose the port your Spring Boot application runs on
EXPOSE 8080

# Run the Spring Boot application
ENTRYPOINT ["java", "-jar", "app.jar"]

