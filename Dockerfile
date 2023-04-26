# Used to create an image containing all of the dependencies in build.gradle.
# This allows for much faster execution times when running a Gradle task via Docker.
FROM gradle:7.6.1-jdk8 as builder
WORKDIR /app
COPY ./ ./
RUN gradle clean compileTestJava --no-daemon

FROM gradle:7.6.1-jdk8
COPY --from=builder /root/.gradle /root/.gradle

