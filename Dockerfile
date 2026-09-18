# Step 1: Use Maven to build the project
FROM maven:3.9.5-eclipse-temurin-17-alpine@sha256:a30f71650b13e5dd376afa423fdf0996e0a8d548cfd1e05e6114ea114c8a3cf5 AS build
WORKDIR /app
COPY . .
RUN mvn clean package -DskipTests -Pdocker

# Step 2: Use Eclipse Temurin JRE to run the application
FROM eclipse-temurin:17-jre-alpine@sha256:45d19def67191d1df7d233282051ac2f6908c6b2d7eaef75441d7e7c721c6d01
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar

# Expose the port your Spring Boot application runs on
EXPOSE 8080

# Run the Spring Boot application
ENTRYPOINT ["java", "-jar", "app.jar"]


