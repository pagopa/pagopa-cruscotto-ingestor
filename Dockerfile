# Immagini pinnate per digest, come i due backend del cruscotto: il tag dice a una persona
# cos'e', il digest dice a Docker quale esattamente. Il prezzo e' che non si aggiornano da sole,
# quindi vanno rinnovate quando esce una patch del runtime - e' cosi' che la CVE-2026-114938
# (Base64 di OpenJDK) ha trovato i backend fermi su un'immagine di aprile 2025.

# Step 1: Use Maven to build the project
# Corretto 21.0.13.1.2. Si compila comunque al livello di linguaggio 17 (maven.compiler.* nel pom):
# il JDK della build e' piu' recente del target, che e' una combinazione supportata e la stessa che
# i backend usano gia'.
FROM maven:3.10.0-amazoncorretto-21-al2023@sha256:c287cc32273f94ac4ab07ba68c92809374db696633307ae06887d902f4bfb93e AS build
WORKDIR /app
COPY . .
RUN mvn clean package -DskipTests -Pdocker

# Step 2: Amazon Corretto 21.0.13.1.2 - la build che corregge la CVE-2026-114938.
# Stesso runtime dei due backend: un solo vendor da seguire quando esce un aggiornamento, invece
# di due. E' una JDK e non una JRE perche' Corretto non pubblica una variante JRE per alpine:
# l'immagine e' piu' grande di quella Temurin che c'era prima.
FROM amazoncorretto:21-alpine@sha256:600e99e39da14b6a909027a924136c423e9334cb7347a8a77d5908bdaa3011b7
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar

# Expose the port your Spring Boot application runs on
EXPOSE 8080

# Run the Spring Boot application
ENTRYPOINT ["java", "-jar", "app.jar"]


