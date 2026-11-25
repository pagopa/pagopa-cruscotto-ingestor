# Immagini pinnate per digest, come i due backend del cruscotto: il tag dice a una persona
# cos'e', il digest dice a Docker quale esattamente. Il prezzo e' che non si aggiornano da sole,
# quindi vanno rinnovate quando esce una patch del runtime - e' cosi' che la CVE-2026-114938
# (Base64 di OpenJDK) ha trovato i backend fermi su un'immagine di aprile 2025.

# Step 1: Use Maven to build the project
# Corretto 21.0.13.1.2. Si compila comunque al livello di linguaggio 17 (maven.compiler.* nel pom):
# il JDK della build e' piu' recente del target, che e' una combinazione supportata e la stessa che
# i backend usano gia'.
#
# IL 21 NON E' UN RIPIEGO, E' UN VINCOLO. Il parent JHipster applica una regola enforcer
# (RequireJavaVersion) che accetta JDK 17-23: con il 24 la build si ferma prima di compilare.
# Prima di salire di linea bisogna quindi passare da li', non dal Dockerfile. Il tag e' fissato
# alla minor di Maven e non al major mobile "3", cosi' un aggiornamento dell'immagine non puo'
# cambiare JDK di nascosto.
FROM maven:3.10.0-amazoncorretto-21-al2023@sha256:c287cc32273f94ac4ab07ba68c92809374db696633307ae06887d902f4bfb93e AS build
WORKDIR /app
COPY . .
RUN mvn clean package -DskipTests -Pdocker

# Step 2: Amazon Corretto 21.0.13.1.2 - la build che corregge la CVE-2026-114938.
# Stesso runtime dei due backend: un solo vendor da seguire quando esce un aggiornamento, invece
# di due. E' una JDK e non una JRE perche' Corretto non pubblica una variante JRE per alpine:
# l'immagine e' piu' grande di quella Temurin che c'era prima.
#
# E' UNA LINEA LTS, E DEVE RESTARLO. Dependabot ha proposto il salto alla 26, che e' EOL e che
# contiene 26.0.2.11.1 - cioe' l'ultima versione ANCORA AFFETTA dalla stessa CVE che questa riga
# esiste per chiudere. Era un aggiornamento che sembrava un avanzamento e sarebbe stato una
# regressione silenziosa. Il guard-rail e' in .github/dependabot.yml: i salti di major su queste
# due immagini sono una decisione umana, non una PR automatica.
FROM amazoncorretto:21-alpine@sha256:600e99e39da14b6a909027a924136c423e9334cb7347a8a77d5908bdaa3011b7
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar

# Expose the port your Spring Boot application runs on
EXPOSE 8080

# Run the Spring Boot application
ENTRYPOINT ["java", "-jar", "app.jar"]


