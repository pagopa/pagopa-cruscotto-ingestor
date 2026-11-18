# Immagini pinnate per digest, come i due backend del cruscotto: il tag dice a una persona
# cos'e', il digest dice a Docker quale esattamente. Il prezzo e' che non si aggiornano da sole,
# quindi vanno rinnovate quando esce una patch del runtime - e' cosi' che la CVE-2026-114938
# (Base64 di OpenJDK) ha trovato i backend fermi su un'immagine di aprile 2025.

# Step 1: Use Maven to build the project
# Corretto 21.0.13.1.2. Si compila comunque al livello di linguaggio 17 (maven.compiler.* nel pom):
# il JDK della build e' piu' recente del target, che e' una combinazione supportata e la stessa che
# i backend usano gia'.
FROM maven:3-amazoncorretto-24-al2023@sha256:9bd1d20d8b70542833657acced078c7ac146a280c048abd5f57dc9a44e1e8b20 AS build
WORKDIR /app
COPY . .
RUN mvn clean package -DskipTests -Pdocker

# Step 2: Amazon Corretto 21.0.13.1.2 - la build che corregge la CVE-2026-114938.
# Stesso runtime dei due backend: un solo vendor da seguire quando esce un aggiornamento, invece
# di due. E' una JDK e non una JRE perche' Corretto non pubblica una variante JRE per alpine:
# l'immagine e' piu' grande di quella Temurin che c'era prima.
FROM amazoncorretto:26-alpine@sha256:26a8444c023a7c2ab2608f179581287e3ad333cef246b027c628da099d1b096e
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar

# Expose the port your Spring Boot application runs on
EXPOSE 8080

# Run the Spring Boot application
ENTRYPOINT ["java", "-jar", "app.jar"]


