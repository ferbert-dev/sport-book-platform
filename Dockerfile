# Runs one pre-built service JAR. Build them first: ./mvnw clean package
#
# Deliberately not a multi-stage build-from-source image: the reactor is already
# built by ./mvnw, and rebuilding it per service inside Docker would download the
# whole dependency tree six times.
FROM eclipse-temurin:21-jre

# curl is only needed so compose healthchecks can poll Actuator. Installed before
# the MODULE arg so this layer is shared across all six service images.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ARG MODULE
ARG VERSION=1.0.0-SNAPSHOT
COPY ${MODULE}/target/${MODULE}-${VERSION}.jar /app/app.jar

# Respect the container memory limit instead of the host's.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75 -XX:+UseContainerSupport"

# exec so the JVM is PID 1 and receives SIGTERM for graceful shutdown.
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
